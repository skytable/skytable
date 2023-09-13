/*
 * Created on Sat May 06 2023
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

use {
    super::{Fields, Model},
    crate::{
        engine::{
            core::index::Row,
            fractal::{FractalToken, GlobalInstanceLike},
            sync::atm::Guard,
            sync::queue::Queue,
        },
        util::compiler,
    },
    parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    std::{
        collections::btree_map::{BTreeMap, Range},
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

/*
    sync matrix
*/

// FIXME(@ohsayan): This an inefficient repr of the matrix; replace it with my other design
#[derive(Debug)]
/// A sync matrix enables different queries to have different access permissions on the data model, and the data in the
/// index
pub struct ISyncMatrix {
    // virtual privileges
    /// read/write model
    v_priv_model_alter: RwLock<()>,
    /// RW data/block all
    v_priv_data_new_or_revise: RwLock<()>,
}

#[cfg(test)]
impl PartialEq for ISyncMatrix {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Debug)]
/// Read model, write new data
pub struct IRModelSMData<'a> {
    rmodel: RwLockReadGuard<'a, ()>,
    mdata: RwLockReadGuard<'a, ()>,
    fields: &'a Fields,
}

impl<'a> IRModelSMData<'a> {
    pub fn new(m: &'a Model) -> Self {
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
/// Read model
pub struct IRModel<'a> {
    rmodel: RwLockReadGuard<'a, ()>,
    fields: &'a Fields,
}

impl<'a> IRModel<'a> {
    pub fn new(m: &'a Model) -> Self {
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
/// Write model
pub struct IWModel<'a> {
    wmodel: RwLockWriteGuard<'a, ()>,
    fields: &'a mut Fields,
}

impl<'a> IWModel<'a> {
    pub fn new(m: &'a Model) -> Self {
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

/*
    delta
*/

#[derive(Debug)]
/// A delta state for the model
pub struct DeltaState {
    // schema
    schema_current_version: AtomicU64,
    schema_deltas: RwLock<BTreeMap<DeltaVersion, SchemaDeltaPart>>,
    // data
    data_current_version: AtomicU64,
    data_deltas: Queue<DataDelta>,
    data_deltas_size: AtomicUsize,
}

impl DeltaState {
    /// A new, fully resolved delta state with version counters set to 0
    pub fn new_resolved() -> Self {
        Self {
            schema_current_version: AtomicU64::new(0),
            schema_deltas: RwLock::new(BTreeMap::new()),
            data_current_version: AtomicU64::new(0),
            data_deltas: Queue::new(),
            data_deltas_size: AtomicUsize::new(0),
        }
    }
}

// data direct
impl DeltaState {
    pub(in crate::engine::core) fn guard_delta_overflow(
        global: &impl GlobalInstanceLike,
        space_name: &str,
        model_name: &str,
        model: &Model,
    ) {
        let current_deltas_size = model.delta_state().data_deltas_size.load(Ordering::Acquire);
        let max_len = global
            .get_max_delta_size()
            .min((model.primary_index().count() as f64 * 0.05) as usize);
        if compiler::unlikely(current_deltas_size >= max_len) {
            global.request_batch_resolve(
                space_name,
                model_name,
                model.get_uuid(),
                current_deltas_size,
            );
        }
    }
}

// data
impl DeltaState {
    pub fn append_new_data_delta_with(
        &self,
        kind: DataDeltaKind,
        row: Row,
        schema_version: DeltaVersion,
        data_version: DeltaVersion,
        g: &Guard,
    ) {
        self.append_new_data_delta(DataDelta::new(schema_version, data_version, row, kind), g);
    }
    pub fn append_new_data_delta(&self, delta: DataDelta, g: &Guard) {
        self.data_deltas.blocking_enqueue(delta, g);
        self.data_deltas_size.fetch_add(1, Ordering::Release);
    }
    pub fn create_new_data_delta_version(&self) -> DeltaVersion {
        DeltaVersion(self.__data_delta_step())
    }
    pub fn get_data_delta_size(&self) -> usize {
        self.data_deltas_size.load(Ordering::Acquire)
    }
}

impl DeltaState {
    fn __data_delta_step(&self) -> u64 {
        self.data_current_version.fetch_add(1, Ordering::AcqRel)
    }
    pub fn __data_delta_dequeue(&self, g: &Guard) -> Option<DataDelta> {
        match self.data_deltas.blocking_try_dequeue(g) {
            Some(d) => {
                self.data_deltas_size.fetch_sub(1, Ordering::Release);
                Some(d)
            }
            None => None,
        }
    }
}

// schema
impl DeltaState {
    pub fn schema_delta_write<'a>(&'a self) -> SchemaDeltaIndexWGuard<'a> {
        SchemaDeltaIndexWGuard(self.schema_deltas.write())
    }
    pub fn schema_delta_read<'a>(&'a self) -> SchemaDeltaIndexRGuard<'a> {
        SchemaDeltaIndexRGuard(self.schema_deltas.read())
    }
    pub fn schema_current_version(&self) -> DeltaVersion {
        self.__schema_delta_current()
    }
    pub fn schema_append_unresolved_wl_field_add(
        &self,
        guard: &mut SchemaDeltaIndexWGuard,
        field_name: &str,
    ) {
        self.__schema_append_unresolved_delta(&mut guard.0, SchemaDeltaPart::field_add(field_name));
    }
    pub fn schema_append_unresolved_wl_field_rem(
        &self,
        guard: &mut SchemaDeltaIndexWGuard,
        field_name: &str,
    ) {
        self.__schema_append_unresolved_delta(&mut guard.0, SchemaDeltaPart::field_rem(field_name));
    }
    pub fn schema_append_unresolved_field_add(&self, field_name: &str) {
        self.schema_append_unresolved_wl_field_add(&mut self.schema_delta_write(), field_name);
    }
    pub fn schema_append_unresolved_field_rem(&self, field_name: &str) {
        self.schema_append_unresolved_wl_field_rem(&mut self.schema_delta_write(), field_name);
    }
}

impl DeltaState {
    fn __schema_delta_step(&self) -> DeltaVersion {
        DeltaVersion(self.schema_current_version.fetch_add(1, Ordering::AcqRel))
    }
    fn __schema_delta_current(&self) -> DeltaVersion {
        DeltaVersion(self.schema_current_version.load(Ordering::Acquire))
    }
    fn __schema_append_unresolved_delta(
        &self,
        w: &mut BTreeMap<DeltaVersion, SchemaDeltaPart>,
        part: SchemaDeltaPart,
    ) -> DeltaVersion {
        let v = self.__schema_delta_step();
        w.insert(v, part);
        v
    }
}

// fractal
impl DeltaState {
    pub fn __fractal_take_from_data_delta(&self, cnt: usize, _token: FractalToken) {
        let _ = self.data_deltas_size.fetch_sub(cnt, Ordering::Release);
    }
    pub fn __fractal_take_full_from_data_delta(&self, _token: FractalToken) -> usize {
        self.data_deltas_size.swap(0, Ordering::AcqRel)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct DeltaVersion(u64);
impl DeltaVersion {
    pub const fn genesis() -> Self {
        Self(0)
    }
    pub const fn __new(v: u64) -> Self {
        Self(v)
    }
    fn step(&self) -> Self {
        Self(self.0 + 1)
    }
    pub const fn value_u64(&self) -> u64 {
        self.0
    }
}

/*
    schema delta
*/

#[derive(Debug)]
pub struct SchemaDeltaPart {
    kind: SchemaDeltaKind,
}

impl SchemaDeltaPart {
    pub fn kind(&self) -> &SchemaDeltaKind {
        &self.kind
    }
}

#[derive(Debug)]
pub enum SchemaDeltaKind {
    FieldAdd(Box<str>),
    FieldRem(Box<str>),
}

impl SchemaDeltaPart {
    fn new(kind: SchemaDeltaKind) -> Self {
        Self { kind }
    }
    fn field_add(field_name: &str) -> Self {
        Self::new(SchemaDeltaKind::FieldAdd(
            field_name.to_owned().into_boxed_str(),
        ))
    }
    fn field_rem(field_name: &str) -> Self {
        Self::new(SchemaDeltaKind::FieldRem(
            field_name.to_owned().into_boxed_str(),
        ))
    }
}

pub struct SchemaDeltaIndexWGuard<'a>(
    RwLockWriteGuard<'a, BTreeMap<DeltaVersion, SchemaDeltaPart>>,
);
pub struct SchemaDeltaIndexRGuard<'a>(RwLockReadGuard<'a, BTreeMap<DeltaVersion, SchemaDeltaPart>>);
impl<'a> SchemaDeltaIndexRGuard<'a> {
    pub fn resolve_iter_since(
        &self,
        current_version: DeltaVersion,
    ) -> Range<DeltaVersion, SchemaDeltaPart> {
        self.0.range(current_version.step()..)
    }
}

/*
    data delta
*/

#[derive(Debug, Clone)]
pub struct DataDelta {
    schema_version: DeltaVersion,
    data_version: DeltaVersion,
    row: Row,
    change: DataDeltaKind,
}

impl DataDelta {
    pub const fn new(
        schema_version: DeltaVersion,
        data_version: DeltaVersion,
        row: Row,
        change: DataDeltaKind,
    ) -> Self {
        Self {
            schema_version,
            data_version,
            row,
            change,
        }
    }
    pub fn schema_version(&self) -> DeltaVersion {
        self.schema_version
    }
    pub fn data_version(&self) -> DeltaVersion {
        self.data_version
    }
    pub fn row(&self) -> &Row {
        &self.row
    }
    pub fn change(&self) -> DataDeltaKind {
        self.change
    }
}

#[derive(Debug, Clone, Copy, sky_macros::EnumMethods, PartialEq)]
#[repr(u8)]
pub enum DataDeltaKind {
    Delete = 0,
    Insert = 1,
    Update = 2,
}
