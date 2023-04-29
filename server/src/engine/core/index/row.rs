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
    crate::engine::{
        data::cell::Datacell,
        idx::{meta::hash::HasherNativeFx, mtchm::meta::TreeElement, IndexST},
        sync::smart::RawRC,
    },
    parking_lot::RwLock,
    std::mem::ManuallyDrop,
};

type DcFieldIndex = IndexST<Box<str>, Datacell, HasherNativeFx>;

pub struct Row {
    pk: ManuallyDrop<PrimaryIndexKey>,
    rc: RawRC<RwLock<DcFieldIndex>>,
}

impl TreeElement for Row {
    type Key = PrimaryIndexKey;
    type Value = RwLock<DcFieldIndex>;
    fn key(&self) -> &Self::Key {
        &self.pk
    }
    fn val(&self) -> &Self::Value {
        self.rc.data()
    }
    fn new(k: Self::Key, v: Self::Value) -> Self {
        Self::new(k, v)
    }
}

impl Row {
    pub fn new(pk: PrimaryIndexKey, data: RwLock<DcFieldIndex>) -> Self {
        Self {
            pk: ManuallyDrop::new(pk),
            rc: unsafe {
                // UNSAFE(@ohsayan): we free this up later
                RawRC::new(data)
            },
        }
    }
    pub fn with_data_read<T>(&self, f: impl Fn(&DcFieldIndex) -> T) -> T {
        let data = self.rc.data().read();
        f(&data)
    }
    pub fn with_data_write<T>(&self, f: impl Fn(&mut DcFieldIndex) -> T) -> T {
        let mut data = self.rc.data().write();
        f(&mut data)
    }
}

impl Clone for Row {
    fn clone(&self) -> Self {
        let rc = unsafe {
            // UNSAFE(@ohsayan): we're calling this in the clone implementation
            self.rc.rc_clone()
        };
        Self {
            pk: unsafe {
                // UNSAFE(@ohsayan): this is safe because of the refcount
                ManuallyDrop::new(self.pk.raw_clone())
            },
            rc,
        }
    }
}

impl Drop for Row {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): we call in this the dtor itself
            self.rc.rc_drop(|| {
                // UNSAFE(@ohsayan): we rely on the correctness of the rc
                ManuallyDrop::drop(&mut self.pk);
            });
        }
    }
}
