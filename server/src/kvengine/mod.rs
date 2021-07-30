/*
 * Created on Wed Jun 30 2021
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

use crate::corestore::htable::Coremap;
use crate::corestore::htable::Data;
use crate::corestore::htable::MapRWLGuard;
use crate::corestore::htable::MapSingleReference;
use crate::corestore::htable::SharedValue;
use core::borrow::Borrow;
use core::hash::Hash;
use core::sync::atomic::AtomicBool;
use core::sync::atomic::Ordering;
pub mod encoding;

const ORD_RELAXED: Ordering = Ordering::Relaxed;

/// A shard lock
///
/// Our jagged or sharded or striped in-memory table is made of multiple in-memory shards
/// and we need a convenient interface to lock down the records. This is exactly why this
/// structure exists: it locks down the table making it resistant to any possible write
/// operation which might give us trouble in some cases
///
pub struct ShardLock<'a> {
    /// A reference to the table (just for lifetime convenience)
    _tableref: &'a Coremap<Data, Data>,
    /// the shard locks
    shard_locks: Vec<MapRWLGuard<'a, std::collections::HashMap<Data, SharedValue<Data>>>>,
}

impl<'a> ShardLock<'a> {
    /// Initialize a shard lock from a provided table: DARN, **this is blocking** because
    /// it will wait for every writer in every stripe to exit before returning. So, know
    /// what you're doing beforehand!
    pub fn init(_tableref: &'a Coremap<Data, Data>) -> Self {
        let shard_locks = _tableref
            .inner
            .shards()
            .iter()
            .map(|lck| lck.read())
            .collect();
        // no lifetime issues here :)
        Self {
            _tableref,
            shard_locks,
        }
    }
}

/// An arbitrary unicode/binary _double encoder_ for two byte slice inputs
pub struct DoubleEncoder {
    fn_ptr: fn(&[u8], &[u8]) -> bool,
}

impl DoubleEncoder {
    /// Check if the underlying encoding validator verifies the encoding
    pub fn is_ok(&self, a: &[u8], b: &[u8]) -> bool {
        (self.fn_ptr)(a, b)
    }
}

/// A _single encoder_ for a single byte slice input
pub struct SingleEncoder {
    fn_ptr: fn(&[u8]) -> bool,
}

impl SingleEncoder {
    /// Check if the underlying encoding validator verifies the encoding
    pub fn is_ok(&self, a: &[u8]) -> bool {
        (self.fn_ptr)(a)
    }
}

// DROP impl isn't required as ShardLock's field types need-drop (std::mem)

/// The key/value engine that acts as the in-memory backing store for the database
///
#[derive(Debug)]
pub struct KVEngine {
    /// the atomic table
    table: Coremap<Data, Data>,
    /// the encoding switch for the key
    encoded_k: AtomicBool,
    /// the encoding switch for the value
    encoded_v: AtomicBool,
}

/// Errors arising from trying to modify the definition of tables
pub enum DdlError {
    /// The table is not empty
    TableNotEmpty,
}

impl Default for KVEngine {
    fn default() -> Self {
        // by default, we don't care about the encoding scheme unless explicitly
        // specified
        KVEngine::init(false, false)
    }
}

impl KVEngine {
    /// Create a new in-memory KVEngine with the specified encoding schemes
    pub fn init(encoded_k: bool, encoded_v: bool) -> Self {
        Self::init_with_data(encoded_k, encoded_v, Coremap::new())
    }
    pub fn init_with_data(encoded_k: bool, encoded_v: bool, table: Coremap<Data, Data>) -> Self {
        Self {
            table,
            encoded_k: AtomicBool::new(encoded_k),
            encoded_v: AtomicBool::new(encoded_v),
        }
    }
    pub fn get_encoding(&self) -> (bool, bool) {
        (
            self.encoded_k.load(ORD_RELAXED),
            self.encoded_v.load(ORD_RELAXED),
        )
    }
    /// Returns an encoder for the key and the value
    pub fn get_encoder(&self) -> DoubleEncoder {
        let (encoded_k, encoded_v) = (
            self.encoded_k.load(ORD_RELAXED),
            self.encoded_v.load(ORD_RELAXED),
        );
        let ret = match (encoded_k, encoded_v) {
            (true, true) => {
                // both k & v
                fn is_okay(key: &[u8], value: &[u8]) -> bool {
                    encoding::is_utf8(key) && encoding::is_utf8(value)
                }
                is_okay
            }
            (true, false) => {
                // only k
                fn is_okay(key: &[u8], _value: &[u8]) -> bool {
                    encoding::is_utf8(key)
                }
                is_okay
            }
            (false, false) => {
                // none
                fn is_okay(_k: &[u8], _v: &[u8]) -> bool {
                    true
                }
                is_okay
            }
            (false, true) => {
                // only v
                fn is_okay(_k: &[u8], v: &[u8]) -> bool {
                    encoding::is_utf8(v)
                }
                is_okay
            }
        };
        DoubleEncoder { fn_ptr: ret }
    }
    /// Returns an encoder for the key
    pub fn get_key_encoder(&self) -> SingleEncoder {
        let ret = if self.encoded_k.load(ORD_RELAXED) {
            fn e(inp: &[u8]) -> bool {
                encoding::is_utf8(inp)
            }
            e
        } else {
            fn e(_inp: &[u8]) -> bool {
                true
            }
            e
        };
        SingleEncoder { fn_ptr: ret }
    }
    /// Returns an encoder for the value
    pub fn get_value_encoder(&self) -> SingleEncoder {
        let ret = if self.encoded_v.load(ORD_RELAXED) {
            fn e(inp: &[u8]) -> bool {
                encoding::is_utf8(inp)
            }
            e
        } else {
            fn e(_inp: &[u8]) -> bool {
                true
            }
            e
        };
        SingleEncoder { fn_ptr: ret }
    }
    pub fn len(&self) -> usize {
        self.table.len()
    }
    pub fn __get_inner_ref(&self) -> &Coremap<Data, Data> {
        &self.table
    }
    /// Alter the table and set the key encoding switch
    ///
    /// Note: this will need an empty table
    pub fn alter_table_key(&self, encoded_k: bool) -> Result<(), DdlError> {
        let _shardlock = ShardLock::init(&self.table);
        // we can now be sure random records are not being tossed around
        if self.table.len() != 0 {
            Err(DdlError::TableNotEmpty)
        } else {
            // the table is empty, roger the alter
            // relaxed memory ordering is fine because we have locked the table
            // for this specific alteration
            self.encoded_k.store(encoded_k, ORD_RELAXED);
            Ok(())
        }
    }
    /// Alter the table and set the value encoding switch
    ///
    /// Note: this will need an empty table
    // TODO(@ohsayan): Figure out how exactly we will handle this at the keyspace level
    pub fn alter_table_value(&self, encoded_v: bool) -> Result<(), DdlError> {
        let _shardlock = ShardLock::init(&self.table);
        // we can now be sure random records are not being tossed around
        if self.table.len() != 0 {
            Err(DdlError::TableNotEmpty)
        } else {
            // the table is empty, roger the alter
            // relaxed memory ordering is fine because we have locked the table
            // for this specific alteration
            self.encoded_v.store(encoded_v, ORD_RELAXED);
            Ok(())
        }
    }
    /// Return an owned value of the key. In most cases, the reference count is just incremented
    /// unless the data itself is mutated in place
    pub fn take_snapshot<Q>(&self, key: &Q) -> Option<Data>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]> + Hash + Eq,
    {
        self.table.get(key).map(|v| v.clone())
    }
    /// Truncate the table
    pub fn truncate_table(&self) {
        self.table.clear()
    }
    /// Get the value for a given key if it exists
    pub fn get(&self, key: impl Into<Data>) -> Result<Option<MapSingleReference<Data, Data>>, ()> {
        Ok(self.table.get(&self._encode_key(key.into())?))
    }
    pub fn exists<Q>(&self, key: Q) -> Result<bool, ()>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]> + Hash + Eq,
    {
        Ok(self.table.contains_key(&self._encode_key(key)?))
    }
    /// Check the unicode encoding of a given byte array
    fn _encode<Q>(data: Q) -> Result<Q, ()>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        if encoding::is_utf8(data.borrow()) {
            Ok(data)
        } else {
            Err(())
        }
    }
    /// Check the unicode encoding of the given key, if the encoded_k flag is set
    fn _encode_key<Q>(&self, key: Q) -> Result<Q, ()>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        if self.encoded_k.load(ORD_RELAXED) {
            Self::_encode(key)
        } else {
            Ok(key)
        }
    }
    /// Check the unicode encoding of the given value, if the encoded_v flag is set
    fn _encode_value<Q>(&self, value: Q) -> Result<Q, ()>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        if self.encoded_v.load(ORD_RELAXED) {
            Self::_encode(value)
        } else {
            Ok(value)
        }
    }
    /// Set the value of a non-existent key
    pub fn set(&self, key: Data, value: Data) -> Result<bool, ()> {
        Ok(self
            .table
            .true_if_insert(self._encode_key(key)?, self._encode_value(value)?))
    }
    /// Update the value of an existing key
    pub fn update(&self, key: Data, value: Data) -> Result<bool, ()> {
        Ok(self
            .table
            .true_if_update(self._encode_key(key)?, self._encode_value(value)?))
    }
    /// Update or insert the value of a key
    pub fn upsert(&self, key: Data, value: Data) -> Result<(), ()> {
        self.table
            .upsert(self._encode_key(key)?, self._encode_value(value)?);
        Ok(())
    }
    /// Remove an existing key
    pub fn remove<Q>(&self, key: Q) -> Result<bool, ()>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]> + Hash + Eq,
    {
        Ok(self.table.true_if_removed(&self._encode_key(key)?))
    }
    pub fn pop<Q>(&self, key: Q) -> Result<Option<(Data, Data)>, ()>
    where
        Data: Borrow<Q>,
        Q: AsRef<[u8]> + Hash + Eq,
    {
        Ok(self.table.remove(&self._encode_key(key)?))
    }
}

#[test]
fn test_ignore_encoding() {
    let non_unicode_value = b"Hello \xF0\x90\x80World".to_vec();
    let non_unicode_key = non_unicode_value.to_owned();
    let tbl = KVEngine::default();
    assert!(tbl
        .set(non_unicode_key.into(), non_unicode_value.into())
        .is_ok());
}

#[test]
fn test_bad_unicode_key() {
    let bad_unicode = b"Hello \xF0\x90\x80World".to_vec();
    let tbl = KVEngine::init(true, false);
    assert!(tbl.set(Data::from(bad_unicode), Data::from("123")).is_err());
}

#[test]
fn test_bad_unicode_value() {
    let bad_unicode = b"Hello \xF0\x90\x80World".to_vec();
    let tbl = KVEngine::init(false, true);
    assert!(tbl.set(Data::from("123"), Data::from(bad_unicode)).is_err());
}

#[test]
fn test_bad_unicode_key_value() {
    let bad_unicode = b"Hello \xF0\x90\x80World".to_vec();
    let tbl = KVEngine::init(true, true);
    assert!(tbl
        .set(Data::from(bad_unicode.clone()), Data::from(bad_unicode))
        .is_err());
}

#[test]
fn test_with_bincode() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct User {
        username: String,
        password: String,
        uuid: u128,
        score: u32,
        level: u32,
    }
    let tbl = KVEngine::init(true, false);
    let joe = User {
        username: "Joe".to_owned(),
        password: "Joe123".to_owned(),
        uuid: u128::MAX,
        score: u32::MAX,
        level: u32::MAX,
    };
    assert!(tbl
        .set(
            Data::from("Joe"),
            Data::from(bincode::serialize(&joe).unwrap(),),
        )
        .is_ok(),);
    assert_eq!(
        bincode::deserialize::<User>(&tbl.get(Data::from("Joe")).unwrap().unwrap()).unwrap(),
        joe
    );
}

#[test]
fn test_encoder_ignore() {
    let tbl = KVEngine::default();
    let encoder = tbl.get_encoder();
    assert!(encoder.is_ok("hello".as_bytes(), b"Hello \xF0\x90\x80World"));
}

#[test]
fn test_encoder_validate_with_non_unicode() {
    let tbl = KVEngine::init(true, true);
    let encoder = tbl.get_encoder();
    assert!(!encoder.is_ok("hello".as_bytes(), b"Hello \xF0\x90\x80World"));
}
