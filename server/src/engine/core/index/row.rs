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
            core::model::{DeltaKind, DeltaState, DeltaVersion},
            data::cell::Datacell,
            idx::{meta::hash::HasherNativeFx, mtchm::meta::TreeElement, IndexST, STIndex},
            sync::smart::RawRC,
        },
        util::compiler,
    },
    parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard},
    std::mem::ManuallyDrop,
};

pub type DcFieldIndex = IndexST<Box<str>, Datacell, HasherNativeFx>;

#[derive(Debug)]
pub struct Row {
    __txn_genesis: DeltaVersion,
    __pk: ManuallyDrop<PrimaryIndexKey>,
    __rc: RawRC<RwLock<RowData>>,
}

#[derive(Debug, PartialEq)]
pub struct RowData {
    fields: DcFieldIndex,
    txn_revised: DeltaVersion,
}

impl RowData {
    pub fn fields(&self) -> &DcFieldIndex {
        &self.fields
    }
    pub fn fields_mut(&mut self) -> &mut DcFieldIndex {
        &mut self.fields
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
        txn_genesis: DeltaVersion,
        txn_revised: DeltaVersion,
    ) -> Self {
        Self {
            __txn_genesis: txn_genesis,
            __pk: ManuallyDrop::new(pk),
            __rc: unsafe {
                // UNSAFE(@ohsayan): we free this up later
                RawRC::new(RwLock::new(RowData {
                    fields: data,
                    txn_revised,
                }))
            },
        }
    }
    pub fn with_data_read<T>(&self, f: impl Fn(&DcFieldIndex) -> T) -> T {
        let data = self.__rc.data().read();
        f(&data.fields)
    }
    pub fn with_data_write<T>(&self, f: impl Fn(&mut DcFieldIndex) -> T) -> T {
        let mut data = self.__rc.data().write();
        f(&mut data.fields)
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
            .map(|(id, data)| (id.clone(), data.clone()))
            .collect()
    }
}

impl Row {
    pub fn resolve_deltas_and_freeze<'g>(
        &'g self,
        delta_state: &DeltaState,
    ) -> RwLockReadGuard<'g, RowData> {
        let rwl_ug = self.d_data().upgradable_read();
        let current_version = delta_state.current_version();
        if compiler::likely(current_version <= rwl_ug.txn_revised) {
            return RwLockUpgradableReadGuard::downgrade(rwl_ug);
        }
        // we have deltas to apply
        let mut wl = RwLockUpgradableReadGuard::upgrade(rwl_ug);
        let delta_read = delta_state.rguard();
        let mut max_delta = wl.txn_revised;
        for (delta_id, delta) in delta_read.resolve_iter_since(wl.txn_revised) {
            match delta.kind() {
                DeltaKind::FieldAdd(f) => {
                    wl.fields.st_insert(f.clone(), Datacell::null());
                }
                DeltaKind::FieldRem(f) => {
                    wl.fields.st_delete(f);
                }
            }
            max_delta = *delta_id;
        }
        wl.txn_revised = max_delta;
        return RwLockWriteGuard::downgrade(wl);
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
