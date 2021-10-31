/*
 * Created on Tue Aug 31 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use super::KVTable;
use super::SingleEncoder;
use crate::corestore::htable::Coremap;
use crate::corestore::map::bref::Ref;
use crate::corestore::Data;
use crate::resp::{TSYMBOL_BINARY, TSYMBOL_UNICODE};
use core::borrow::Borrow;
use core::hash::Hash;
use parking_lot::RwLock;

type Vecref<'a> = Ref<'a, Data, LockedVec>;
pub type LockedVec = RwLock<Vec<Data>>;

#[derive(Debug)]
pub struct KVEListMap {
    encoded_id: bool,
    encoded_payload_element: bool,
    base: Coremap<Data, LockedVec>,
}

impl KVEListMap {
    /// Create a new KVEListMap. `Encoded ID == encoded key` and `encoded payload == encoded elements`
    pub fn new(encoded_id: bool, encoded_payload_element: bool) -> Self {
        Self::init_with_data(encoded_id, encoded_payload_element, Coremap::new())
    }
    pub const fn payload_needs_encoding(&self) -> bool {
        self.encoded_payload_element
    }
    pub const fn id_needs_encoding(&self) -> bool {
        self.encoded_id
    }
    pub fn init_with_data(
        encoded_id: bool,
        encoded_payload_element: bool,
        base: Coremap<Data, LockedVec>,
    ) -> Self {
        Self {
            encoded_id,
            encoded_payload_element,
            base,
        }
    }
    /// Get an encoder instance for the payload elements
    pub fn get_payload_encoder(&self) -> SingleEncoder {
        s_encoder_booled!(self.encoded_payload_element)
    }
    /// Get an encoder instance for the ID
    pub fn get_id_encoder(&self) -> SingleEncoder {
        s_encoder_booled!(self.encoded_id)
    }
    /// Check if the key is encoded correctly
    pub fn encode_key<T: AsRef<[u8]>>(&self, val: T) -> bool {
        s_encoder!(self.encoded_id)(val.as_ref())
    }
    /// Check if the element in a list is encoded correctly
    pub fn encode_value<T: AsRef<[u8]>>(&self, val: T) -> bool {
        s_encoder!(self.encoded_id)(val.as_ref())
    }
    pub fn get_payload_tsymbol(&self) -> u8 {
        if self.encoded_payload_element {
            TSYMBOL_UNICODE
        } else {
            TSYMBOL_BINARY
        }
    }
    borrow_hash_fn! {
        /// Check the length of a list if it exists
        pub fn {borrow: Data} len_of(self: &Self, key: &Q) -> Option<usize> {
            self.base.get(key).map(|v| v.read().len())
        }
        pub fn {borrow: Data} get(self: &Self, key: &Q) -> Option<Vecref<'_>> {
            self.base.get(key)
        }
        pub fn {borrow: Data} get_cloned(self: &Self, key: &Q, count: usize) -> Option<Vec<Data>> {
            self.base.get(key).map(|v| v.read().iter().take(count).cloned().collect())
        }
    }
    /// Create and add a new list to the map
    pub fn add_list(&self, listname: Data) -> Option<bool> {
        if_cold! {
            if (self.encode_key(&listname)) {
                Some(self.base.true_if_insert(listname, LockedVec::default()))
            } else {
                None
            }
        }
    }
    /// Remove a key from the map
    pub fn remove(&self, listname: &[u8]) -> bool {
        self.base.true_if_removed(listname)
    }
}

impl<'a> KVTable<'a, Coremap<Data, RwLock<Vec<Data>>>> for KVEListMap {
    fn kve_len(&self) -> usize {
        self.base.len()
    }
    fn kve_clear(&self) {
        self.base.clear()
    }
    fn kve_key_encoded(&self) -> bool {
        self.encoded_id
    }
    fn kve_payload_encoded(&self) -> bool {
        self.encoded_payload_element
    }
    fn kve_inner_ref(&'a self) -> &'a Coremap<Data, RwLock<Vec<Data>>> {
        &self.base
    }
    fn kve_remove<Q: ?Sized + Eq + Hash>(&self, input: &Q) -> bool
    where
        Data: Borrow<Q>,
    {
        self.base.true_if_removed(input)
    }
    fn kve_exists<Q: ?Sized + Eq + Hash>(&self, input: &Q) -> bool
    where
        Data: Borrow<Q>,
    {
        self.base.contains_key(input)
    }
    fn kve_keylen<Q: ?Sized + Eq + Hash>(&self, input: &Q) -> Option<usize>
    where
        Data: Borrow<Q>,
    {
        self.base.get(input).map(|v| v.key().len())
    }
}
