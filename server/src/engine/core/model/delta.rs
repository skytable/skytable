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
    super::ModelData,
    crate::engine::{
        core::{dml::QueryExecMeta, index::Row},
        fractal::{FractalToken, GlobalInstanceLike},
        mem::RawStr,
        sync::atm::Guard,
        sync::queue::Queue,
    },
    std::{
        collections::btree_map::{BTreeMap, Range},
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
/// A delta state for the model
pub struct DeltaState {
    // schema
    schema_current_version: u64,
    schema_deltas: BTreeMap<DeltaVersion, SchemaDeltaPart>,
    // data
    data_current_version: AtomicU64,
    data_deltas: Queue<DataDelta>,
    data_deltas_size: AtomicUsize,
}

impl DeltaState {
    /// A new, fully resolved delta state with version counters set to 0
    pub fn new_resolved() -> Self {
        Self {
            schema_current_version: 0,
            schema_deltas: BTreeMap::new(),
            data_current_version: AtomicU64::new(0),
            data_deltas: Queue::new(),
            data_deltas_size: AtomicUsize::new(0),
        }
    }
    pub fn __set_delta_version(&self, version: DeltaVersion) {
        self.data_current_version
            .store(version.value_u64(), Ordering::Relaxed)
    }
}

// data direct
impl DeltaState {
    pub(in crate::engine::core) fn guard_delta_overflow(
        global: &impl GlobalInstanceLike,
        space_name: &str,
        model_name: &str,
        model: &ModelData,
        hint: QueryExecMeta,
    ) {
        global.request_batch_resolve_if_cache_full(space_name, model_name, model, hint)
    }
}

// data
impl DeltaState {
    pub fn append_new_data_delta_with(
        &self,
        kind: DataDeltaKind,
        row: Row,
        data_version: DeltaVersion,
        g: &Guard,
    ) -> usize {
        self.append_new_data_delta(DataDelta::new(data_version, row, kind), g)
    }
    pub fn append_new_data_delta(&self, delta: DataDelta, g: &Guard) -> usize {
        self.data_deltas.blocking_enqueue(delta, g);
        self.data_deltas_size.fetch_add(1, Ordering::Release) + 1
    }
    pub fn create_new_data_delta_version(&self) -> DeltaVersion {
        DeltaVersion(self.__data_delta_step())
    }
}

impl DeltaState {
    fn __data_delta_step(&self) -> u64 {
        self.data_current_version.fetch_add(1, Ordering::AcqRel)
    }
    pub fn __data_delta_dequeue(&self, g: &Guard) -> Option<DataDelta> {
        self.data_deltas.blocking_try_dequeue(g)
    }
}

// schema
impl DeltaState {
    pub fn resolve_iter_since(
        &self,
        current_version: DeltaVersion,
    ) -> Range<DeltaVersion, SchemaDeltaPart> {
        self.schema_deltas.range(current_version.step()..)
    }
    pub fn schema_current_version(&self) -> DeltaVersion {
        DeltaVersion(self.schema_current_version)
    }
    pub fn unresolved_append_field_add(&mut self, field_name: RawStr) {
        self.__schema_append_unresolved_delta(SchemaDeltaPart::field_add(field_name));
    }
    pub fn unresolved_append_field_rem(&mut self, field_name: RawStr) {
        self.__schema_append_unresolved_delta(SchemaDeltaPart::field_rem(field_name));
    }
}

impl DeltaState {
    fn __schema_delta_step(&mut self) -> DeltaVersion {
        let current = self.schema_current_version;
        self.schema_current_version += 1;
        DeltaVersion(current)
    }
    fn __schema_append_unresolved_delta(&mut self, part: SchemaDeltaPart) -> DeltaVersion {
        let v = self.__schema_delta_step();
        self.schema_deltas.insert(v, part);
        v
    }
}

// fractal
impl DeltaState {
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
    FieldAdd(RawStr),
    FieldRem(RawStr),
}

impl SchemaDeltaPart {
    fn new(kind: SchemaDeltaKind) -> Self {
        Self { kind }
    }
    fn field_add(field_name: RawStr) -> Self {
        Self::new(SchemaDeltaKind::FieldAdd(field_name))
    }
    fn field_rem(field_name: RawStr) -> Self {
        Self::new(SchemaDeltaKind::FieldRem(field_name))
    }
}

/*
    data delta
*/

#[derive(Debug, Clone)]
pub struct DataDelta {
    data_version: DeltaVersion,
    row: Row,
    change: DataDeltaKind,
}

impl DataDelta {
    pub const fn new(data_version: DeltaVersion, row: Row, change: DataDeltaKind) -> Self {
        Self {
            data_version,
            row,
            change,
        }
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
