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
    parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    std::{
        collections::btree_map::{BTreeMap, Range},
        sync::atomic::{AtomicU64, Ordering},
    },
};

/*
    sync matrix
*/

// FIXME(@ohsayan): This an inefficient repr of the matrix; replace it with my other design
#[derive(Debug)]
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
pub struct DeltaState {
    current_version: AtomicU64,
    deltas: RwLock<BTreeMap<DeltaVersion, DeltaPart>>,
}

#[derive(Debug)]
pub struct DeltaPart {
    kind: DeltaKind,
}

impl DeltaPart {
    pub fn kind(&self) -> &DeltaKind {
        &self.kind
    }
}

#[derive(Debug)]
pub enum DeltaKind {
    FieldAdd(Box<str>),
    FieldRem(Box<str>),
}

impl DeltaPart {
    fn new(kind: DeltaKind) -> Self {
        Self { kind }
    }
    fn field_add(field_name: &str) -> Self {
        Self::new(DeltaKind::FieldAdd(field_name.to_owned().into_boxed_str()))
    }
    fn field_rem(field_name: &str) -> Self {
        Self::new(DeltaKind::FieldRem(field_name.to_owned().into_boxed_str()))
    }
}

pub struct DeltaIndexWGuard<'a>(RwLockWriteGuard<'a, BTreeMap<DeltaVersion, DeltaPart>>);
pub struct DeltaIndexRGuard<'a>(RwLockReadGuard<'a, BTreeMap<DeltaVersion, DeltaPart>>);
impl<'a> DeltaIndexRGuard<'a> {
    pub fn resolve_iter_since(
        &self,
        current_version: DeltaVersion,
    ) -> Range<DeltaVersion, DeltaPart> {
        self.0.range(current_version.step()..)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct DeltaVersion(u64);
impl DeltaVersion {
    pub const fn genesis() -> Self {
        Self(0)
    }
    #[cfg(test)]
    pub fn test_new(v: u64) -> Self {
        Self(v)
    }
    fn step(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl DeltaState {
    pub fn new_resolved() -> Self {
        Self {
            current_version: AtomicU64::new(0),
            deltas: RwLock::new(BTreeMap::new()),
        }
    }
    pub fn wguard<'a>(&'a self) -> DeltaIndexWGuard<'a> {
        DeltaIndexWGuard(self.deltas.write())
    }
    pub fn rguard<'a>(&'a self) -> DeltaIndexRGuard<'a> {
        DeltaIndexRGuard(self.deltas.read())
    }
    pub fn current_version(&self) -> DeltaVersion {
        self.__delta_current()
    }
    pub fn append_unresolved_wl_field_add(&self, guard: &mut DeltaIndexWGuard, field_name: &str) {
        self.__append_unresolved_delta(&mut guard.0, DeltaPart::field_add(field_name));
    }
    pub fn append_unresolved_wl_field_rem(&self, guard: &mut DeltaIndexWGuard, field_name: &str) {
        self.__append_unresolved_delta(&mut guard.0, DeltaPart::field_rem(field_name));
    }
    pub fn append_unresolved_field_add(&self, field_name: &str) {
        self.append_unresolved_wl_field_add(&mut self.wguard(), field_name);
    }
    pub fn append_unresolved_field_rem(&self, field_name: &str) {
        self.append_unresolved_wl_field_rem(&mut self.wguard(), field_name);
    }
}

impl DeltaState {
    fn __delta_step(&self) -> DeltaVersion {
        DeltaVersion(self.current_version.fetch_add(1, Ordering::AcqRel))
    }
    fn __delta_current(&self) -> DeltaVersion {
        DeltaVersion(self.current_version.load(Ordering::Acquire))
    }
    fn __append_unresolved_delta(
        &self,
        w: &mut BTreeMap<DeltaVersion, DeltaPart>,
        part: DeltaPart,
    ) -> DeltaVersion {
        let v = self.__delta_step();
        w.insert(v, part);
        v
    }
}
