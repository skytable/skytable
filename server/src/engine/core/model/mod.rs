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
mod delta;

#[cfg(test)]
use std::cell::RefCell;

use {
    self::delta::{IRModel, IRModelSMData, ISyncMatrix, IWModel},
    super::{index::PrimaryIndex, util::EntityLocator},
    crate::engine::{
        data::{
            cell::Datacell,
            tag::{DataTag, FullTag, TagClass, TagSelector},
        },
        error::{DatabaseError, DatabaseResult},
        idx::{IndexBaseSpec, IndexSTSeqCns, STIndex, STIndexSeq},
        mem::VInline,
        ql::ddl::{
            crt::CreateModel,
            drop::DropModel,
            syn::{FieldSpec, LayerSpec},
        },
    },
    std::cell::UnsafeCell,
};

pub(in crate::engine::core) use self::delta::{DeltaKind, DeltaState, DeltaVersion};
pub(in crate::engine::core) type Fields = IndexSTSeqCns<Box<str>, Field>;

#[derive(Debug)]
pub struct ModelData {
    p_key: Box<str>,
    p_tag: FullTag,
    fields: UnsafeCell<Fields>,
    sync_matrix: ISyncMatrix,
    data: PrimaryIndex,
    delta: DeltaState,
}

#[cfg(test)]
impl PartialEq for ModelData {
    fn eq(&self, m: &Self) -> bool {
        let mdl1 = self.intent_read_model();
        let mdl2 = m.intent_read_model();
        self.p_key == m.p_key && self.p_tag == m.p_tag && mdl1.fields() == mdl2.fields()
    }
}

impl ModelData {
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
    pub fn intent_write_new_data<'a>(&'a self) -> IRModelSMData<'a> {
        IRModelSMData::new(self)
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
    pub fn is_empty_atomic(&self) -> bool {
        // TODO(@ohsayan): change this!
        true
    }
    pub fn primary_index(&self) -> &PrimaryIndex {
        &self.data
    }
    pub fn delta_state(&self) -> &DeltaState {
        &self.delta
    }
}

impl ModelData {
    pub fn process_create(
        CreateModel {
            model_name: _,
            fields,
            props,
        }: CreateModel,
    ) -> DatabaseResult<Self> {
        let mut okay = props.is_empty() & !fields.is_empty();
        // validate fields
        let mut field_spec = fields.into_iter();
        let mut fields = Fields::idx_init_cap(field_spec.len());
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
                    data: PrimaryIndex::new_empty(),
                    delta: DeltaState::new_resolved(),
                });
            }
        }
        Err(DatabaseError::DdlModelBadDefinition)
    }
}

impl ModelData {
    pub fn exec_create(gns: &super::GlobalNS, stmt: CreateModel) -> DatabaseResult<()> {
        let (space_name, model_name) = stmt.model_name.parse_entity()?;
        let model = Self::process_create(stmt)?;
        gns.with_space(space_name, |space| space._create_model(model_name, model))
    }
    pub fn exec_drop(gns: &super::GlobalNS, stmt: DropModel) -> DatabaseResult<()> {
        let (space, model) = stmt.entity.parse_entity()?;
        gns.with_space(space, |space| {
            let mut w_space = space.models().write();
            match w_space.st_delete_if(model, |mdl| !mdl.is_empty_atomic()) {
                Some(true) => Ok(()),
                Some(false) => Err(DatabaseError::DdlModelViewNotEmpty),
                None => Err(DatabaseError::DdlModelNotFound),
            }
        })
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
    fn compute_index(&self, dc: &Datacell) -> usize {
        if {
            ((!self.is_nullable()) & dc.is_null())
                | ((self.layers[0].tag.tag_class() != dc.kind()) & !dc.is_null())
        } {
            // illegal states: (1) bad null (2) tags don't match
            7
        } else {
            dc.kind().word()
        }
    }
    pub fn validate_data_fpath(&self, data: &Datacell) -> bool {
        // if someone sends a PR with an added check, I'll personally come to your house and throw a brick on your head
        if (self.layers.len() == 1) | data.is_null() {
            layertrace("fpath");
            unsafe {
                // UNSAFE(@ohsayan): checked for non-null, and used correct class
                LVERIFY[self.compute_index(data)](self.layers()[0], data)
            }
        } else {
            Self::rverify_layers(self.layers(), data)
        }
    }
    // TODO(@ohsayan): improve algo with dfs
    fn rverify_layers(layers: &[Layer], data: &Datacell) -> bool {
        let layer = layers[0];
        let layers = &layers[1..];
        match (layer.tag.tag_class(), data.kind()) {
            (TagClass::List, TagClass::List) if !layers.is_empty() => {
                let mut okay = unsafe {
                    // UNSAFE(@ohsayan): we've verified this
                    LVERIFY[TagClass::List.word()](layer, data)
                };
                let list = unsafe {
                    // UNSAFE(@ohsayan): we verified tags
                    data.read_list()
                };
                let lread = list.read();
                let mut i = 0;
                while (i < lread.len()) & okay {
                    okay &= Self::rverify_layers(layers, &lread[i]);
                    i += 1;
                }
                okay
            }
            (tag_a, tag_b) if tag_a == tag_b => {
                unsafe {
                    // UNSAFE(@ohsayan): same tags; not-null for now so no extra handling required here
                    LVERIFY[tag_a.word()](layer, data)
                }
            }
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Layer {
    tag: FullTag,
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
    pub fn tag(&self) -> FullTag {
        self.tag
    }
    pub fn new_empty_props(tag: FullTag) -> Self {
        Self::new(tag)
    }
    #[inline(always)]
    fn compute_index(&self, dc: &Datacell) -> usize {
        self.tag.tag_class().word() * (dc.is_null() as usize)
    }
    const fn new(tag: FullTag) -> Self {
        Self { tag }
    }
    const fn empty(tag: FullTag) -> Self {
        Self::new(tag)
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

static LVERIFY: [unsafe fn(Layer, &Datacell) -> bool; 8] = [
    lverify_bool,
    lverify_uint,
    lverify_sint,
    lverify_float,
    lverify_bin,
    lverify_str,
    lverify_list,
    |_, _| false,
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
