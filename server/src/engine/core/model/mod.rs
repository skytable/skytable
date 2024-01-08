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
pub(in crate::engine) mod delta;

use {
    super::index::PrimaryIndex,
    crate::engine::{
        data::{
            cell::Datacell,
            tag::{DataTag, FloatSpec, FullTag, SIntSpec, TagClass, TagSelector, UIntSpec},
            uuid::Uuid,
        },
        error::{QueryError, QueryResult},
        fractal::{GenericTask, GlobalInstanceLike, Task},
        idx::{self, IndexBaseSpec, IndexSTSeqCns, STIndex, STIndexSeq},
        mem::{RawStr, VInline},
        ql::ddl::{
            crt::CreateModel,
            drop::DropModel,
            syn::{FieldSpec, LayerSpec},
        },
        txn::gns::{self as gnstxn, SpaceIDRef},
    },
    std::collections::hash_map::{Entry, HashMap},
};

pub(in crate::engine::core) use self::delta::{DeltaState, DeltaVersion, SchemaDeltaKind};

use super::util::{EntityID, EntityIDRef};
type Fields = IndexSTSeqCns<RawStr, Field>;

#[derive(Debug)]
pub struct Model {
    uuid: Uuid,
    p_key: RawStr,
    p_tag: FullTag,
    fields: Fields,
    data: PrimaryIndex,
    delta: DeltaState,
    private: ModelPrivate,
    decl: String,
}

#[cfg(test)]
impl PartialEq for Model {
    fn eq(&self, m: &Self) -> bool {
        self.uuid == m.uuid
            && self.p_key == m.p_key
            && self.p_tag == m.p_tag
            && self.fields == m.fields
    }
}

impl Model {
    pub fn get_uuid(&self) -> Uuid {
        self.uuid
    }
    pub fn p_key(&self) -> &str {
        &self.p_key
    }
    pub fn p_tag(&self) -> FullTag {
        self.p_tag
    }
    fn is_pk(&self, new: &str) -> bool {
        self.p_key.as_bytes() == new.as_bytes()
    }
    fn not_pk(&self, new: &str) -> bool {
        !self.is_pk(new)
    }
    fn guard_pk(&self, new: &str) -> QueryResult<()> {
        if self.is_pk(new) {
            Err(QueryError::QExecDdlModelAlterIllegal)
        } else {
            Ok(())
        }
    }
    pub fn primary_index(&self) -> &PrimaryIndex {
        &self.data
    }
    pub fn delta_state(&self) -> &DeltaState {
        &self.delta
    }
    pub fn fields(&self) -> &Fields {
        &self.fields
    }
    pub fn model_mutator<'a>(&'a mut self) -> ModelMutator<'a> {
        ModelMutator { model: self }
    }
    fn sync_decl(&mut self) {
        self.decl = self.redescribe();
    }
    pub fn describe(&self) -> &str {
        &self.decl
    }
    fn redescribe(&self) -> String {
        let mut ret = format!("{{");
        let mut it = self.fields().stseq_ord_kv().peekable();
        while let Some((field_name, field_decl)) = it.next() {
            // legend: * -> primary, ! -> not null, ? -> null
            if self.is_pk(&field_name) {
                ret.push('*');
            } else if field_decl.is_nullable() {
                ret.push('?');
            } else {
                ret.push('!');
            }
            ret.push_str(&field_name);
            ret.push(':');
            // TODO(@ohsayan): it's all lists right now, so this is okay but fix it later
            if field_decl.layers().len() == 1 {
                ret.push_str(field_decl.layers()[0].tag().tag_selector().name_str());
            } else {
                ret.push_str(&"[".repeat(field_decl.layers().len() - 1));
                ret.push_str(
                    field_decl.layers()[field_decl.layers().len() - 1]
                        .tag()
                        .tag_selector()
                        .name_str(),
                );
                ret.push_str(&"]".repeat(field_decl.layers().len() - 1))
            }
            if it.peek().is_some() {
                ret.push(',');
            }
        }
        ret.push('}');
        ret
    }
}

impl Model {
    fn new_with_private(
        uuid: Uuid,
        p_key: RawStr,
        p_tag: FullTag,
        fields: Fields,
        private: ModelPrivate,
    ) -> Self {
        let mut slf = Self {
            uuid,
            p_key,
            p_tag,
            fields,
            data: PrimaryIndex::new_empty(),
            delta: DeltaState::new_resolved(),
            private,
            decl: String::new(),
        };
        slf.sync_decl();
        slf
    }
    pub fn new_restore(
        uuid: Uuid,
        p_key: Box<str>,
        p_tag: FullTag,
        decl_fields: IndexSTSeqCns<Box<str>, Field>,
    ) -> Self {
        let mut private = ModelPrivate::empty();
        let p_key = unsafe {
            // UNSAFE(@ohsayan): once again, all cool since we maintain the allocation
            private.push_allocated(p_key)
        };
        let mut fields = IndexSTSeqCns::idx_init();
        decl_fields
            .stseq_owned_kv()
            .map(|(field_key, field)| {
                (
                    unsafe {
                        // UNSAFE(@ohsayan): we ensure that priv is dropped iff model is dropped
                        private.push_allocated(field_key)
                    },
                    field,
                )
            })
            .for_each(|(field_key, field)| {
                fields.st_insert(field_key, field);
            });
        Self::new_with_private(uuid, p_key, p_tag, fields, private)
    }
    pub fn process_create(
        CreateModel {
            model_name: _,
            fields,
            props,
            ..
        }: CreateModel,
    ) -> QueryResult<Self> {
        let mut private = ModelPrivate::empty();
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
            let this_field_ptr = unsafe {
                // UNSAFE(@ohsayan): this is going to go with our alloc, so we're good! if we fail too, the dtor for private will run
                private.allocate_or_recycle(field_name.as_str())
            };
            if primary {
                pk_cnt += 1usize;
                last_pk = Some(unsafe {
                    // UNSAFE(@ohsayan): totally cool, it's all allocated
                    this_field_ptr.clone()
                });
                okay &= !null;
            }
            let layer = Field::parse_layers(layers, null)?;
            okay &= fields.st_insert(this_field_ptr, layer);
        }
        okay &= pk_cnt <= 1;
        if okay {
            let last_pk = last_pk.unwrap_or(unsafe {
                // UNSAFE(@ohsayan): once again, all of this is allocated
                fields.stseq_ord_key().next().unwrap().clone()
            });
            let tag = fields.st_get(&last_pk).unwrap().layers()[0].tag;
            if tag.tag_unique().is_unique() {
                return Ok(Self::new_with_private(
                    Uuid::new(),
                    last_pk,
                    tag,
                    fields,
                    private,
                ));
            }
        }
        Err(QueryError::QExecDdlModelBadDefinition)
    }
}

impl Model {
    pub fn transactional_exec_create<G: GlobalInstanceLike>(
        global: &G,
        stmt: CreateModel,
    ) -> QueryResult<Option<bool>> {
        let (space_name, model_name) = (stmt.model_name.space(), stmt.model_name.entity());
        let if_nx = stmt.if_not_exists;
        let model = Self::process_create(stmt)?;
        global.namespace().ddl_with_space_mut(&space_name, |space| {
            // TODO(@ohsayan): be extra cautious with post-transactional tasks (memck)
            if space.models().contains(model_name) {
                if if_nx {
                    return Ok(Some(false));
                } else {
                    return Err(QueryError::QExecDdlObjectAlreadyExists);
                }
            }
            // since we've locked this down, no one else can parallely create another model in the same space (or remove)
            if G::FS_IS_NON_NULL {
                let mut txn_driver = global.namespace_txn_driver().lock();
                // prepare txn
                let txn = gnstxn::CreateModelTxn::new(
                    SpaceIDRef::new(&space_name, &space),
                    &model_name,
                    &model,
                );
                // attempt to initialize driver
                global.initialize_model_driver(
                    &space_name,
                    space.get_uuid(),
                    &model_name,
                    model.get_uuid(),
                )?;
                // commit txn
                match txn_driver.try_commit(txn) {
                    Ok(()) => {}
                    Err(e) => {
                        // failed to commit, request cleanup
                        global.taskmgr_post_standard_priority(Task::new(
                            GenericTask::delete_model_dir(
                                &space_name,
                                space.get_uuid(),
                                &model_name,
                                model.get_uuid(),
                            ),
                        ));
                        return Err(e.into());
                    }
                }
            }
            // update global state
            let _ = space.models_mut().insert(model_name.into());
            let _ = global
                .namespace()
                .idx_models()
                .write()
                .insert(EntityID::new(&space_name, &model_name), model);
            if if_nx {
                Ok(Some(true))
            } else {
                Ok(None)
            }
        })
    }
    pub fn transactional_exec_drop<G: GlobalInstanceLike>(
        global: &G,
        stmt: DropModel,
    ) -> QueryResult<Option<bool>> {
        let (space_name, model_name) = (stmt.entity.space(), stmt.entity.entity());
        global.namespace().ddl_with_space_mut(&space_name, |space| {
            if !space.models().contains(model_name) {
                if stmt.if_exists {
                    return Ok(Some(false));
                } else {
                    // the model isn't even present
                    return Err(QueryError::QExecObjectNotFound);
                }
            }
            // get exclusive lock on models
            let mut models_idx = global.namespace().idx_models().write();
            let model = models_idx
                .get(&EntityIDRef::new(&space_name, &model_name))
                .unwrap();
            // the model must be empty for us to clean it up! (NB: consistent view + EX)
            if (model.primary_index().count() != 0) & !(stmt.force) {
                // nope, we can't drop this
                return Err(QueryError::QExecDdlNotEmpty);
            }
            // okay this is looking good for us
            if G::FS_IS_NON_NULL {
                // prepare txn
                let txn = gnstxn::DropModelTxn::new(gnstxn::ModelIDRef::new(
                    SpaceIDRef::new(&space_name, &space),
                    &model_name,
                    model.get_uuid(),
                    model.delta_state().schema_current_version().value_u64(),
                ));
                // commit txn
                global.namespace_txn_driver().lock().try_commit(txn)?;
                // request cleanup
                global.purge_model_driver(
                    space_name,
                    space.get_uuid(),
                    model_name,
                    model.get_uuid(),
                    false,
                );
            }
            // update global state
            let _ = models_idx.remove(&EntityIDRef::new(&space_name, &model_name));
            let _ = space.models_mut().remove(model_name);
            if stmt.if_exists {
                Ok(Some(true))
            } else {
                Ok(None)
            }
        })
    }
}

#[derive(Debug, PartialEq)]
struct ModelPrivate {
    alloc: HashMap<Box<str>, bool, idx::meta::hash::HasherNativeFx>,
}

impl ModelPrivate {
    fn empty() -> Self {
        Self {
            alloc: HashMap::with_hasher(Default::default()),
        }
    }
    pub(self) unsafe fn allocate_or_recycle(&mut self, new: &str) -> RawStr {
        match self.alloc.get_key_value(new) {
            Some((prev_alloc, _)) => {
                // already allocated this
                let ret = RawStr::new(prev_alloc.as_ptr(), prev_alloc.len());
                // set live!
                *self.alloc.get_mut(ret.as_str()).unwrap() = false;
                return ret;
            }
            None => {
                // need to allocate
                let alloc = new.to_owned().into_boxed_str();
                let ret = RawStr::new(alloc.as_ptr(), alloc.len());
                let _ = self.alloc.insert(alloc, false);
                return ret;
            }
        }
    }
    pub(self) unsafe fn mark_pending_remove(&mut self, v: &str) -> RawStr {
        let ret = self.alloc.get_key_value(v).unwrap().0;
        let ret = RawStr::new(ret.as_ptr(), ret.len());
        *self.alloc.get_mut(v).unwrap() = true;
        ret
    }
    pub(self) unsafe fn vacuum_marked(&mut self) {
        self.alloc.retain(|_, dead| !*dead)
    }
    pub(self) unsafe fn push_allocated(&mut self, alloc: Box<str>) -> RawStr {
        match self.alloc.entry(alloc) {
            Entry::Occupied(mut oe) => {
                oe.insert(false);
                RawStr::new(oe.key().as_ptr(), oe.key().len())
            }
            Entry::Vacant(ve) => {
                let ret = RawStr::new(ve.key().as_ptr(), ve.key().len());
                ve.insert(false);
                return ret;
            }
        }
    }
}

pub struct ModelMutator<'a> {
    model: &'a mut Model,
}

impl<'a> ModelMutator<'a> {
    pub unsafe fn vacuum_stashed(&mut self) {
        self.model.private.vacuum_marked()
    }
    pub fn remove_field(&mut self, name: &str) -> bool {
        // remove
        let r = self.model.fields.st_delete(name);
        // recycle
        let ptr = unsafe { self.model.private.mark_pending_remove(name) };
        // publish delta
        self.model.delta.unresolved_append_field_rem(ptr);
        r
    }
    pub fn add_field(&mut self, name: Box<str>, field: Field) -> bool {
        unsafe {
            // allocate
            let fkeyptr = self.model.private.push_allocated(name);
            // add
            let r = self.model.fields.st_insert(fkeyptr.clone(), field);
            // delta
            self.model.delta.unresolved_append_field_add(fkeyptr);
            r
        }
    }
    pub fn update_field(&mut self, name: &str, field: Field) -> bool {
        self.model.fields.st_update(name, field)
    }
}

impl<'a> Drop for ModelMutator<'a> {
    fn drop(&mut self) {
        self.model.sync_decl();
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

#[cfg(test)]
pub static TY_BOOL: &str = LUT[0].0;
#[cfg(test)]
pub static TY_UINT: [&str; 4] = [LUT[1].0, LUT[2].0, LUT[3].0, LUT[4].0];
#[cfg(test)]
pub static TY_SINT: [&str; 4] = [LUT[5].0, LUT[6].0, LUT[7].0, LUT[8].0];
#[cfg(test)]
pub static TY_FLOAT: [&str; 2] = [LUT[9].0, LUT[10].0];
#[cfg(test)]
pub static TY_BINARY: &str = LUT[11].0;
#[cfg(test)]
pub static TY_STRING: &str = LUT[12].0;
#[cfg(test)]
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
    pub fn parse_layers(spec: Vec<LayerSpec>, nullable: bool) -> QueryResult<Self> {
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
            Err(QueryError::QExecDdlInvalidTypeDefinition)
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
            dc.kind().value_word()
        }
    }
    pub fn vt_data_fpath(&self, data: &mut Datacell) -> bool {
        if (self.layers.len() == 1) | (data.is_null()) {
            layertrace("fpath");
            unsafe { VTFN[self.compute_index(data)](self.layers()[0], data) }
        } else {
            Self::rvt_data(self.layers(), data)
        }
    }
    fn rvt_data(layers: &[Layer], data: &mut Datacell) -> bool {
        let layer = layers[0];
        let layers = &layers[1..];
        match (layer.tag().tag_class(), data.kind()) {
            (TagClass::List, TagClass::List) => {
                let mut okay = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    VTFN[TagClass::List.value_word()](layer, data)
                };
                let list = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    data.read_list()
                };
                let mut lread = list.write();
                let mut i = 0;
                while (i < lread.len()) & okay {
                    okay &= Self::rvt_data(layers, &mut lread[i]);
                    i += 1;
                }
                okay
            }
            (tag_a, tag_b) if tag_a == tag_b => {
                unsafe {
                    // UNSAFE(@ohsayan): same tags and lists have non-null elements
                    VTFN[tag_a.value_word()](layer, data)
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

#[allow(unused)]
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
    pub const fn new(tag: FullTag) -> Self {
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

#[cfg(test)]
local! {
    static LAYER_TRACE: Vec<Box<str>> = Vec::new();
}

#[inline(always)]
fn layertrace(_layer: impl ToString) {
    #[cfg(test)]
    {
        local_mut!(LAYER_TRACE, |ltrace| ltrace.push(_layer.to_string().into()))
    }
}

#[cfg(test)]
/// Obtain a layer trace and clear older traces
pub(super) fn layer_traces() -> Box<[Box<str>]> {
    local_mut!(LAYER_TRACE, |ltrace| ltrace.drain(..).collect())
}

static VTFN: [unsafe fn(Layer, &mut Datacell) -> bool; 8] = [
    vt_bool,
    vt_uint,
    vt_sint,
    vt_float,
    vt_bin,
    vt_str,
    vt_list,
    |_, _| false,
];
unsafe fn vt_bool(_: Layer, _: &mut Datacell) -> bool {
    layertrace("bool");
    true
}
unsafe fn vt_uint(l: Layer, dc: &mut Datacell) -> bool {
    layertrace("uint");
    dc.set_tag(l.tag());
    UIntSpec::from_full(l.tag()).check(dc.read_uint())
}
unsafe fn vt_sint(l: Layer, dc: &mut Datacell) -> bool {
    layertrace("sint");
    dc.set_tag(l.tag());
    SIntSpec::from_full(l.tag()).check(dc.read_sint())
}
unsafe fn vt_float(l: Layer, dc: &mut Datacell) -> bool {
    layertrace("float");
    dc.set_tag(l.tag());
    FloatSpec::from_full(l.tag()).check(dc.read_float())
}
unsafe fn vt_bin(_: Layer, _: &mut Datacell) -> bool {
    layertrace("binary");
    true
}
unsafe fn vt_str(_: Layer, _: &mut Datacell) -> bool {
    layertrace("string");
    true
}
unsafe fn vt_list(_: Layer, _: &mut Datacell) -> bool {
    layertrace("list");
    true
}
