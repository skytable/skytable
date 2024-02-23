/*
 * Created on Thu Apr 27 2023
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
    super::key::PrimaryIndexKey,
    crate::{
        engine::{
            core::model::{DeltaState, DeltaVersion, SchemaDeltaKind},
            data::cell::Datacell,
            idx::{meta::hash::HasherNativeFx, mtchm::meta::TreeElement, IndexST, STIndex},
            mem::RawStr,
            sync::smart::RawRC,
        },
        util::compiler,
    },
    parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard},
    std::mem::ManuallyDrop,
};

pub type DcFieldIndex = IndexST<RawStr, Datacell, HasherNativeFx>;

#[derive(Debug)]
pub struct Row {
    __pk: ManuallyDrop<PrimaryIndexKey>,
    __rc: RawRC<RwLock<RowData>>,
}

#[derive(Debug, PartialEq)]
pub struct RowData {
    fields: DcFieldIndex,
    txn_revised_data: DeltaVersion,
    txn_revised_schema_version: DeltaVersion,
}

impl RowData {
    pub fn fields(&self) -> &DcFieldIndex {
        &self.fields
    }
    pub fn fields_mut(&mut self) -> &mut DcFieldIndex {
        &mut self.fields
    }
    pub fn set_txn_revised(&mut self, new: DeltaVersion) {
        self.txn_revised_data = new;
    }
    pub fn get_txn_revised(&self) -> DeltaVersion {
        self.txn_revised_data
    }
}

impl TreeElement for Row {
    type IKey = PrimaryIndexKey;
    type Key = PrimaryIndexKey;
    type IValue = DcFieldIndex;
    type Value = RwLock<RowData>;
    type VEx1 = DeltaVersion;
    type VEx2 = DeltaVersion;
    fn key(&self) -> &Self::Key {
        self.d_key()
    }
    fn val(&self) -> &Self::Value {
        self.d_data()
    }
    fn new(
        k: Self::Key,
        v: Self::IValue,
        txn_genesis: DeltaVersion,
        txn_revised: DeltaVersion,
    ) -> Self {
        Self::new(k, v, txn_genesis, txn_revised)
    }
}

impl Row {
    pub fn new(
        pk: PrimaryIndexKey,
        data: DcFieldIndex,
        schema_version: DeltaVersion,
        txn_revised_data: DeltaVersion,
    ) -> Self {
        Self::new_restored(pk, data, schema_version, txn_revised_data)
    }
    pub fn new_restored(
        pk: PrimaryIndexKey,
        data: DcFieldIndex,
        schema_version: DeltaVersion,
        txn_revised_data: DeltaVersion,
    ) -> Self {
        Self {
            __pk: ManuallyDrop::new(pk),
            __rc: unsafe {
                // UNSAFE(@ohsayan): we free this up later
                RawRC::new(RwLock::new(RowData {
                    fields: data,
                    txn_revised_schema_version: schema_version,
                    txn_revised_data,
                }))
            },
        }
    }
    pub fn d_key(&self) -> &PrimaryIndexKey {
        &self.__pk
    }
    pub fn d_data(&self) -> &RwLock<RowData> {
        self.__rc.data()
    }
    #[cfg(test)]
    pub fn cloned_data(&self) -> Vec<(Box<str>, Datacell)> {
        self.d_data()
            .read()
            .fields()
            .st_iter_kv()
            .map(|(id, data)| (id.as_str().to_owned().into_boxed_str(), data.clone()))
            .collect()
    }
}

impl Row {
    /// Only apply deltas if a certain condition is met
    pub fn resolve_schema_deltas_and_freeze_if<'g>(
        &'g self,
        delta_state: &DeltaState,
        iff: impl Fn(&RowData) -> bool,
    ) -> RwLockReadGuard<'g, RowData> {
        let rwl_ug = self.d_data().upgradable_read();
        if !iff(&rwl_ug) {
            return RwLockUpgradableReadGuard::downgrade(rwl_ug);
        }
        let current_version = delta_state.schema_current_version();
        if compiler::likely(current_version <= rwl_ug.txn_revised_schema_version) {
            return RwLockUpgradableReadGuard::downgrade(rwl_ug);
        }
        // we have deltas to apply
        let mut wl = RwLockUpgradableReadGuard::upgrade(rwl_ug);
        let mut max_delta = wl.txn_revised_schema_version;
        for (delta_id, delta) in delta_state.resolve_iter_since(wl.txn_revised_schema_version) {
            match delta.kind() {
                SchemaDeltaKind::FieldAdd(f) => {
                    wl.fields.st_insert(
                        unsafe {
                            // UNSAFE(@ohsayan): a row is inside a model and is valid as long as it is in there!
                            // even if the model was chucked and the row was lying around it won't cause any harm because it
                            // neither frees anything nor allocates
                            f.clone()
                        },
                        Datacell::null(),
                    );
                }
                SchemaDeltaKind::FieldRem(f) => {
                    wl.fields.st_delete(f);
                }
            }
            max_delta = *delta_id;
        }
        // we've revised upto the most most recent delta version (that we saw at this point)
        wl.txn_revised_schema_version = max_delta;
        return RwLockWriteGuard::downgrade(wl);
    }
    pub fn resolve_schema_deltas_and_freeze<'g>(
        &'g self,
        delta_state: &DeltaState,
    ) -> RwLockReadGuard<'g, RowData> {
        self.resolve_schema_deltas_and_freeze_if(delta_state, |_| true)
    }
}

impl Clone for Row {
    fn clone(&self) -> Self {
        let rc = unsafe {
            // UNSAFE(@ohsayan): we're calling this in the clone implementation
            self.__rc.rc_clone()
        };
        Self {
            __pk: unsafe {
                // UNSAFE(@ohsayan): this is safe because of the refcount
                ManuallyDrop::new(self.__pk.raw_clone())
            },
            __rc: rc,
            ..*self
        }
    }
}

impl Drop for Row {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): we call in this the dtor itself
            self.__rc.rc_drop(|| {
                // UNSAFE(@ohsayan): we rely on the correctness of the rc
                ManuallyDrop::drop(&mut self.__pk);
            });
        }
    }
}
