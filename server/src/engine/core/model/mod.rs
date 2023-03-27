/*
 * Created on Mon Feb 06 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

pub(super) mod alt;
pub mod cell;

#[cfg(test)]
use std::cell::RefCell;

use {
    crate::engine::{
        core::model::cell::Datacell,
        data::{
            tag::{DataTag, FullTag, TagClass, TagSelector},
            ItemID,
        },
        error::{DatabaseError, DatabaseResult},
        idx::{IndexSTSeqCns, STIndex, STIndexSeq},
        mem::VInline,
        ql::ddl::{
            crt::CreateModel,
            syn::{FieldSpec, LayerSpec},
        },
    },
    core::cell::UnsafeCell,
    parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

type Fields = IndexSTSeqCns<Box<str>, Field>;

// FIXME(@ohsayan): update this!

#[derive(Debug)]
pub struct ModelView {
    p_key: Box<str>,
    p_tag: FullTag,
    fields: UnsafeCell<Fields>,
    sync_matrix: ISyncMatrix,
}

#[cfg(test)]
impl PartialEq for ModelView {
    fn eq(&self, m: &Self) -> bool {
        let mdl1 = self.intent_read_model();
        let mdl2 = m.intent_read_model();
        self.p_key == m.p_key && self.p_tag == m.p_tag && mdl1.fields() == mdl2.fields()
    }
}

impl ModelView {
    pub fn p_key(&self) -> &str {
        &self.p_key
    }
    pub fn p_tag(&self) -> FullTag {
        self.p_tag
    }
    pub fn sync_matrix(&self) -> &ISyncMatrix {
        &self.sync_matrix
    }
    unsafe fn _read_fields<'a>(&'a self) -> &'a Fields {
        &*self.fields.get().cast_const()
    }
    unsafe fn _read_fields_mut<'a>(&'a self) -> &'a mut Fields {
        &mut *self.fields.get()
    }
    pub fn intent_read_model<'a>(&'a self) -> IRModel<'a> {
        IRModel::new(self)
    }
    pub fn intent_write_model<'a>(&'a self) -> IWModel<'a> {
        IWModel::new(self)
    }
    fn is_pk(&self, new: &str) -> bool {
        self.p_key.as_bytes() == new.as_bytes()
    }
    fn not_pk(&self, new: &str) -> bool {
        !self.is_pk(new)
    }
    fn guard_pk(&self, new: &str) -> DatabaseResult<()> {
        if self.is_pk(new) {
            Err(DatabaseError::DdlModelAlterProtectedField)
        } else {
            Ok(())
        }
    }
}

impl ModelView {
    pub fn process_create(
        CreateModel {
            model_name,
            fields,
            props,
        }: CreateModel,
    ) -> DatabaseResult<Self> {
        let mut okay = props.is_empty() & !fields.is_empty() & ItemID::check(&model_name);
        // validate fields
        let mut field_spec = fields.into_iter();
        let mut fields = IndexSTSeqCns::with_capacity(field_spec.len());
        let mut last_pk = None;
        let mut pk_cnt = 0;
        while (field_spec.len() != 0) & okay {
            let FieldSpec {
                field_name,
                layers,
                null,
                primary,
            } = field_spec.next().unwrap();
            if primary {
                pk_cnt += 1usize;
                last_pk = Some(field_name.as_str());
                okay &= !null;
            }
            let layer = Field::parse_layers(layers, null)?;
            okay &= fields.st_insert(field_name.as_str().to_string().into_boxed_str(), layer);
        }
        okay &= pk_cnt <= 1;
        if okay {
            let last_pk = last_pk.unwrap_or(fields.stseq_ord_key().next().unwrap());
            let tag = fields.st_get(last_pk).unwrap().layers()[0].tag;
            if tag.tag_unique().is_unique() {
                return Ok(Self {
                    p_key: last_pk.into(),
                    p_tag: tag,
                    fields: UnsafeCell::new(fields),
                    sync_matrix: ISyncMatrix::new(),
                });
            }
        }
        Err(DatabaseError::DdlModelBadDefinition)
    }
}

impl ModelView {
    pub fn exec_create(
        gns: &super::GlobalNS,
        space: &[u8],
        stmt: CreateModel,
    ) -> DatabaseResult<()> {
        let name = stmt.model_name;
        let model = Self::process_create(stmt)?;
        let space_rl = gns.spaces().read();
        let Some(space) = space_rl.get(space) else {
            return Err(DatabaseError::DdlSpaceNotFound)
        };
        space._create_model(ItemID::new(&name), model)
    }
}

/*
    Layer
*/

static G: [u8; 15] = [0, 13, 12, 5, 6, 4, 3, 6, 1, 10, 4, 5, 7, 5, 5];
static S1: [u8; 7] = [13, 9, 4, 14, 2, 4, 7];
static S2: [u8; 7] = [12, 8, 2, 6, 4, 9, 9];

static LUT: [(&str, FullTag); 14] = [
    ("bool", FullTag::BOOL),
    ("uint8", FullTag::new_uint(TagSelector::UInt8)),
    ("uint16", FullTag::new_uint(TagSelector::UInt16)),
    ("uint32", FullTag::new_uint(TagSelector::UInt32)),
    ("uint64", FullTag::new_uint(TagSelector::UInt64)),
    ("sint8", FullTag::new_sint(TagSelector::SInt8)),
    ("sint16", FullTag::new_sint(TagSelector::SInt16)),
    ("sint32", FullTag::new_sint(TagSelector::SInt32)),
    ("sint64", FullTag::new_sint(TagSelector::SInt64)),
    ("float32", FullTag::new_float(TagSelector::Float32)),
    ("float64", FullTag::new_float(TagSelector::Float64)),
    ("binary", FullTag::BIN),
    ("string", FullTag::STR),
    ("list", FullTag::LIST),
];

pub static TY_BOOL: &str = LUT[0].0;
pub static TY_UINT: [&str; 4] = [LUT[1].0, LUT[2].0, LUT[3].0, LUT[4].0];
pub static TY_SINT: [&str; 4] = [LUT[5].0, LUT[6].0, LUT[7].0, LUT[8].0];
pub static TY_FLOAT: [&str; 2] = [LUT[9].0, LUT[10].0];
pub static TY_BINARY: &str = LUT[11].0;
pub static TY_STRING: &str = LUT[12].0;
pub static TY_LIST: &str = LUT[13].0;

#[derive(Debug, PartialEq, Clone)]
pub struct Field {
    layers: VInline<1, Layer>,
    nullable: bool,
}

impl Field {
    pub fn new(layers: VInline<1, Layer>, nullable: bool) -> Self {
        Self { layers, nullable }
    }
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    pub fn layers(&self) -> &[Layer] {
        &self.layers
    }
    pub fn parse_layers(spec: Vec<LayerSpec>, nullable: bool) -> DatabaseResult<Self> {
        let mut layers = spec.into_iter().rev();
        let mut okay = true;
        let mut fin = false;
        let mut layerview = VInline::new();
        while (layers.len() != 0) & okay & !fin {
            let LayerSpec { ty, props } = layers.next().unwrap();
            okay &= props.is_empty(); // FIXME(@ohsayan): you know what to do here
            match Layer::get_layer(&ty) {
                Some(l) => {
                    fin = l.tag.tag_selector() != TagSelector::List;
                    layerview.push(l);
                }
                None => okay = false,
            }
        }
        okay &= fin & (layers.len() == 0);
        if okay {
            Ok(Self {
                layers: layerview,
                nullable,
            })
        } else {
            Err(DatabaseError::DdlModelInvalidTypeDefinition)
        }
    }
    #[inline(always)]
    fn single_pass_for(&self, dc: &Datacell) -> bool {
        ((self.layers().len() == 1) & (self.layers()[0].tag.tag_class() == dc.kind()))
            | (self.nullable & dc.is_null())
    }
    #[inline(always)]
    fn compute_index(&self, dc: &Datacell) -> usize {
        // escape check if it makes sense to
        !(self.nullable & dc.is_null()) as usize * self.layers()[0].tag.tag_class().word()
    }
    pub fn validate_data_fpath(&self, data: &Datacell) -> bool {
        // if someone sends a PR with an added check, I'll personally come to your house and throw a brick on your head
        if self.single_pass_for(data) {
            layertrace("fpath");
            unsafe { LVERIFY[self.compute_index(data)](self.layers()[0], data) }
        } else {
            Self::rverify_layers(self.layers(), data)
        }
    }
    // TODO(@ohsayan): improve algo with dfs
    fn rverify_layers(layers: &[Layer], data: &Datacell) -> bool {
        let layer = layers[0];
        let layers = &layers[1..];
        match (layer.tag.tag_class(), data.kind()) {
            (layer_tag, data_tag) if (layer_tag == data_tag) & (layer_tag < TagClass::List) => {
                // time to go home
                (unsafe { LVERIFY[layer.tag.tag_class().word()](layer, data) } & layers.is_empty())
            }
            (TagClass::List, TagClass::List) => unsafe {
                let mut okay = !layers.is_empty() & LVERIFY[TagClass::List.word()](layer, data);
                let list = data.read_list().read();
                let mut it = list.iter();
                while (it.len() != 0) & okay {
                    okay &= Self::rverify_layers(layers, it.next().unwrap());
                }
                okay
            },
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Layer {
    tag: FullTag,
    config: [usize; 2],
}

impl Layer {
    pub const fn bool() -> Self {
        Self::empty(FullTag::BOOL)
    }
    pub const fn uint8() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt8))
    }
    pub const fn uint16() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt16))
    }
    pub const fn uint32() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt32))
    }
    pub const fn uint64() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt64))
    }
    pub const fn sint8() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt8))
    }
    pub const fn sint16() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt16))
    }
    pub const fn sint32() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt32))
    }
    pub const fn sint64() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt64))
    }
    pub const fn float32() -> Self {
        Self::empty(FullTag::new_float(TagSelector::Float32))
    }
    pub const fn float64() -> Self {
        Self::empty(FullTag::new_float(TagSelector::Float64))
    }
    pub const fn bin() -> Self {
        Self::empty(FullTag::BIN)
    }
    pub const fn str() -> Self {
        Self::empty(FullTag::STR)
    }
    pub const fn list() -> Self {
        Self::empty(FullTag::LIST)
    }
}

impl Layer {
    #[cfg(test)]
    pub fn new_test(tag: FullTag, config: [usize; 2]) -> Self {
        Self::new(tag, config)
    }
    #[inline(always)]
    fn compute_index(&self, dc: &Datacell) -> usize {
        self.tag.tag_class().word() * (dc.is_null() as usize)
    }
    const fn new(tag: FullTag, config: [usize; 2]) -> Self {
        Self { tag, config }
    }
    const fn empty(tag: FullTag) -> Self {
        Self::new(tag, [0; 2])
    }
    fn hf(key: &[u8], v: [u8; 7]) -> u16 {
        let mut tot = 0;
        let mut i = 0;
        while i < key.len() {
            tot += v[i % v.len()] as u16 * key[i] as u16;
            i += 1;
        }
        tot % 15
    }
    fn pf(key: &[u8]) -> u16 {
        (G[Self::hf(key, S1) as usize] as u16 + G[Self::hf(key, S2) as usize] as u16) % 15
    }
    fn get_layer(ident: &str) -> Option<Self> {
        let idx = Self::pf(ident.as_bytes()) as usize;
        if idx < LUT.len() && LUT[idx].0 == ident {
            Some(Self::empty(LUT[idx].1))
        } else {
            None
        }
    }
}

static LVERIFY: [unsafe fn(Layer, &Datacell) -> bool; 7] = [
    lverify_bool,
    lverify_uint,
    lverify_sint,
    lverify_float,
    lverify_bin,
    lverify_str,
    lverify_list,
];

#[cfg(test)]
thread_local! {
    static LAYER_TRACE: RefCell<Vec<Box<str>>> = RefCell::new(Vec::new());
}

#[inline(always)]
fn layertrace(_layer: impl ToString) {
    #[cfg(test)]
    {
        LAYER_TRACE.with(|v| v.borrow_mut().push(_layer.to_string().into()));
    }
}

#[cfg(test)]
/// Obtain a layer trace and clear older traces
pub(super) fn layer_traces() -> Box<[Box<str>]> {
    LAYER_TRACE.with(|x| {
        let ret = x.borrow().iter().cloned().collect();
        x.borrow_mut().clear();
        ret
    })
}

unsafe fn lverify_bool(_: Layer, _: &Datacell) -> bool {
    layertrace("bool");
    true
}
unsafe fn lverify_uint(l: Layer, d: &Datacell) -> bool {
    layertrace("uint");
    const MX: [u64; 4] = [u8::MAX as _, u16::MAX as _, u32::MAX as _, u64::MAX];
    d.read_uint() <= MX[l.tag.tag_selector().word() - 1]
}
unsafe fn lverify_sint(l: Layer, d: &Datacell) -> bool {
    layertrace("sint");
    const MN_MX: [(i64, i64); 4] = [
        (i8::MIN as _, i8::MAX as _),
        (i16::MIN as _, i16::MAX as _),
        (i32::MIN as _, i32::MAX as _),
        (i64::MIN, i64::MAX),
    ];
    let (mn, mx) = MN_MX[l.tag.tag_selector().word() - 5];
    (d.read_sint() >= mn) & (d.read_sint() <= mx)
}
unsafe fn lverify_float(l: Layer, d: &Datacell) -> bool {
    layertrace("float");
    const MN_MX: [(f64, f64); 2] = [(f32::MIN as _, f32::MAX as _), (f64::MIN, f64::MAX)];
    let (mn, mx) = MN_MX[l.tag.tag_selector().word() - 9];
    (d.read_float() >= mn) & (d.read_float() <= mx)
}
unsafe fn lverify_bin(_: Layer, _: &Datacell) -> bool {
    layertrace("binary");
    true
}
unsafe fn lverify_str(_: Layer, _: &Datacell) -> bool {
    layertrace("string");
    true
}
unsafe fn lverify_list(_: Layer, _: &Datacell) -> bool {
    layertrace("list");
    true
}

// FIXME(@ohsayan): This an inefficient repr of the matrix; replace it with my other design
#[derive(Debug)]
pub struct ISyncMatrix {
    // virtual privileges
    v_priv_model_alter: RwLock<()>,
    v_priv_data_new_or_revise: RwLock<()>,
}

#[cfg(test)]
impl PartialEq for ISyncMatrix {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct IRModelSMData<'a> {
    rmodel: RwLockReadGuard<'a, ()>,
    mdata: RwLockReadGuard<'a, ()>,
    fields: &'a Fields,
}

impl<'a> IRModelSMData<'a> {
    pub fn new(m: &'a ModelView) -> Self {
        let rmodel = m.sync_matrix().v_priv_model_alter.read();
        let mdata = m.sync_matrix().v_priv_data_new_or_revise.read();
        Self {
            rmodel,
            mdata,
            fields: unsafe {
                // UNSAFE(@ohsayan): we already have acquired this resource
                m._read_fields()
            },
        }
    }
    pub fn fields(&'a self) -> &'a Fields {
        self.fields
    }
}

#[derive(Debug)]
pub struct IRModel<'a> {
    rmodel: RwLockReadGuard<'a, ()>,
    fields: &'a Fields,
}

impl<'a> IRModel<'a> {
    pub fn new(m: &'a ModelView) -> Self {
        Self {
            rmodel: m.sync_matrix().v_priv_model_alter.read(),
            fields: unsafe {
                // UNSAFE(@ohsayan): we already have acquired this resource
                m._read_fields()
            },
        }
    }
    pub fn fields(&'a self) -> &'a Fields {
        self.fields
    }
}

#[derive(Debug)]
pub struct IWModel<'a> {
    wmodel: RwLockWriteGuard<'a, ()>,
    fields: &'a mut Fields,
}

impl<'a> IWModel<'a> {
    pub fn new(m: &'a ModelView) -> Self {
        Self {
            wmodel: m.sync_matrix().v_priv_model_alter.write(),
            fields: unsafe {
                // UNSAFE(@ohsayan): we have exclusive access to this resource
                m._read_fields_mut()
            },
        }
    }
    pub fn fields(&'a self) -> &'a Fields {
        self.fields
    }
    // ALIASING
    pub fn fields_mut(&mut self) -> &mut Fields {
        self.fields
    }
}

impl ISyncMatrix {
    pub const fn new() -> Self {
        Self {
            v_priv_model_alter: RwLock::new(()),
            v_priv_data_new_or_revise: RwLock::new(()),
        }
    }
}
