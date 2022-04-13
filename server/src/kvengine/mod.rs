/*
 * Created on Sun Mar 13 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

#![allow(dead_code)] // TODO(@ohsayan): Clean this up later

pub mod encoding;
#[cfg(test)]
mod tests;

use self::encoding::{ENCODING_LUT, ENCODING_LUT_PAIR};
use crate::corestore::{booltable::BoolTable, htable::Coremap, map::bref::Ref, Data};
use crate::util::compiler;
use parking_lot::RwLock;

pub type KVEStandard = KVEngine<Data>;
pub type KVEListmap = KVEngine<LockedVec>;
pub type LockedVec = RwLock<Vec<Data>>;
pub type SingleEncoder = fn(&[u8]) -> bool;
pub type DoubleEncoder = fn(&[u8], &[u8]) -> bool;
type EntryRef<'a, T> = Ref<'a, Data, T>;
type EncodingResult<T> = Result<T, ()>;
type OptionRef<'a, T> = Option<Ref<'a, Data, T>>;
type EncodingResultRef<'a, T> = EncodingResult<OptionRef<'a, T>>;

const TSYMBOL_LUT: BoolTable<u8> = BoolTable::new(b'+', b'?');

pub trait KVEValue {
    fn verify_encoding(&self, e_v: bool) -> EncodingResult<()>;
}

impl KVEValue for Data {
    fn verify_encoding(&self, e_v: bool) -> EncodingResult<()> {
        if ENCODING_LUT[e_v](self) {
            Ok(())
        } else {
            Err(())
        }
    }
}

impl KVEValue for LockedVec {
    fn verify_encoding(&self, e_v: bool) -> EncodingResult<()> {
        let func = ENCODING_LUT[e_v];
        if self.read().iter().all(|v| func(v)) {
            Ok(())
        } else {
            Err(())
        }
    }
}

#[derive(Debug)]
pub struct KVEngine<T> {
    data: Coremap<Data, T>,
    e_k: bool,
    e_v: bool,
}

// basic method impls
impl<T> KVEngine<T> {
    /// Create a new KVEBlob
    pub fn new(e_k: bool, e_v: bool, data: Coremap<Data, T>) -> Self {
        Self { data, e_k, e_v }
    }
    /// Create a new empty KVEBlob
    pub fn init(e_k: bool, e_v: bool) -> Self {
        Self::new(e_k, e_v, Default::default())
    }
    /// Number of KV pairs
    pub fn len(&self) -> usize {
        self.data.len()
    }
    /// Delete all the key/value pairs
    pub fn truncate_table(&self) {
        self.data.clear()
    }
    /// Returns a reference to the inner structure
    pub fn get_inner_ref(&self) -> &Coremap<Data, T> {
        &self.data
    }
    /// Check the encoding of the key
    pub fn is_key_ok(&self, key: &[u8]) -> bool {
        self._check_encoding(key, self.e_k)
    }
    /// Check the encoding of the value
    pub fn is_val_ok(&self, val: &[u8]) -> bool {
        self._check_encoding(val, self.e_v)
    }
    #[inline(always)]
    fn check_key_encoding(&self, item: &[u8]) -> Result<(), ()> {
        self.check_encoding(item, self.e_k)
    }
    #[inline(always)]
    fn check_value_encoding(&self, item: &[u8]) -> Result<(), ()> {
        self.check_encoding(item, self.e_v)
    }
    #[inline(always)]
    fn _check_encoding(&self, item: &[u8], encoded: bool) -> bool {
        ENCODING_LUT[encoded](item)
    }
    #[inline(always)]
    fn check_encoding(&self, item: &[u8], encoded: bool) -> Result<(), ()> {
        if compiler::likely(self._check_encoding(item, encoded)) {
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn is_key_encoded(&self) -> bool {
        self.e_k
    }
    pub fn is_val_encoded(&self) -> bool {
        self.e_v
    }
    /// Get the key tsymbol
    pub fn get_key_tsymbol(&self) -> u8 {
        TSYMBOL_LUT[self.e_k]
    }
    /// Get the value tsymbol
    pub fn get_value_tsymbol(&self) -> u8 {
        TSYMBOL_LUT[self.e_v]
    }
    /// Returns (k_enc, v_enc)
    pub fn get_encoding_tuple(&self) -> (bool, bool) {
        (self.e_k, self.e_v)
    }
    /// Returns an encoder fnptr for the key
    pub fn get_key_encoder(&self) -> SingleEncoder {
        ENCODING_LUT[self.e_k]
    }
    /// Returns an encoder fnptr for the value
    pub fn get_val_encoder(&self) -> SingleEncoder {
        ENCODING_LUT[self.e_v]
    }
}

// dict impls
impl<T: KVEValue> KVEngine<T> {
    /// Get the value of the given key
    pub fn get<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResultRef<T> {
        self.check_key_encoding(key.as_ref())
            .map(|_| self.get_unchecked(key))
    }
    /// Get the value of the given key without any encoding checks
    pub fn get_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> OptionRef<T> {
        self.data.get(key.as_ref())
    }
    /// Set the value of the given key
    pub fn set(&self, key: Data, val: T) -> EncodingResult<bool> {
        self.check_key_encoding(&key)
            .and_then(|_| val.verify_encoding(self.e_v))
            .map(|_| self.set_unchecked(key, val))
    }
    /// Same as set, but doesn't check encoding. Caller must check encoding
    pub fn set_unchecked(&self, key: Data, val: T) -> bool {
        self.data.true_if_insert(key, val)
    }
    /// Check if the provided key exists
    pub fn exists<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResult<bool> {
        self.check_key_encoding(key.as_ref())?;
        Ok(self.exists_unchecked(key.as_ref()))
    }
    pub fn exists_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> bool {
        self.data.contains_key(key.as_ref())
    }
    /// Update the value of an existing key. Returns `true` if updated
    pub fn update(&self, key: Data, val: T) -> EncodingResult<bool> {
        self.check_key_encoding(&key)?;
        val.verify_encoding(self.e_v)?;
        Ok(self.update_unchecked(key, val))
    }
    /// Update the value of an existing key without encoding checks
    pub fn update_unchecked(&self, key: Data, val: T) -> bool {
        self.data.true_if_update(key, val)
    }
    /// Update or insert an entry
    pub fn upsert(&self, key: Data, val: T) -> EncodingResult<()> {
        self.check_key_encoding(&key)?;
        val.verify_encoding(self.e_v)?;
        self.upsert_unchecked(key, val);
        Ok(())
    }
    /// Update or insert an entry without encoding checks
    pub fn upsert_unchecked(&self, key: Data, val: T) {
        self.data.upsert(key, val)
    }
    /// Remove an entry
    pub fn remove<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResult<bool> {
        self.check_key_encoding(key.as_ref())?;
        Ok(self.remove_unchecked(key))
    }
    /// Remove an entry without encoding checks
    pub fn remove_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> bool {
        self.data.true_if_removed(key.as_ref())
    }
    /// Pop an entry
    pub fn pop<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResult<Option<T>> {
        self.check_key_encoding(key.as_ref())?;
        Ok(self.pop_unchecked(key))
    }
    /// Pop an entry without encoding checks
    pub fn pop_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> Option<T> {
        self.data.remove(key.as_ref()).map(|(_, v)| v)
    }
}

impl<T: Clone> KVEngine<T> {
    pub fn get_cloned<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResult<Option<T>> {
        self.check_key_encoding(key.as_ref())?;
        Ok(self.get_cloned_unchecked(key.as_ref()))
    }
    pub fn get_cloned_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> Option<T> {
        self.data.get_cloned(key.as_ref())
    }
}

impl KVEStandard {
    pub fn take_snapshot_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> Option<Data> {
        self.data.get_cloned(key.as_ref())
    }
    /// Returns an encoder that checks each key and each value in turn
    /// Usual usage:
    /// ```notest
    /// for (k, v) in samples {
    ///     assert!(kve.get_double_encoder(k, v))
    /// }
    /// ```
    pub fn get_double_encoder(&self) -> DoubleEncoder {
        ENCODING_LUT_PAIR[(self.e_k, self.e_v)]
    }
}

// list impls
impl KVEListmap {
    #[cfg(test)]
    pub fn add_list(&self, listname: Data) -> EncodingResult<bool> {
        self.check_key_encoding(&listname)?;
        Ok(self.data.true_if_insert(listname, LockedVec::new(vec![])))
    }
    pub fn list_len(&self, listname: &[u8]) -> EncodingResult<Option<usize>> {
        self.check_key_encoding(listname)?;
        Ok(self.data.get(listname).map(|list| list.read().len()))
    }
    pub fn list_cloned(&self, listname: &[u8], count: usize) -> EncodingResult<Option<Vec<Data>>> {
        self.check_key_encoding(listname)?;
        Ok(self
            .data
            .get(listname)
            .map(|list| list.read().iter().take(count).cloned().collect()))
    }
    pub fn list_cloned_full(&self, listname: &[u8]) -> EncodingResult<Option<Vec<Data>>> {
        self.check_key_encoding(listname)?;
        Ok(self
            .data
            .get(listname)
            .map(|list| list.read().iter().cloned().collect()))
    }
}

impl<T> Default for KVEngine<T> {
    fn default() -> Self {
        Self::init(false, false)
    }
}
