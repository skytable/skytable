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

#![allow(dead_code)] // TODO(@ohsayan): Remove this lint once we're done

use crate::coredb::htable::Data;
use crate::coredb::htable::HTable;
use crate::coredb::htable::MapRWLGuard;
use crate::coredb::htable::MapSingleReference;
use crate::coredb::htable::SharedValue;
use core::sync::atomic::AtomicBool;
use core::sync::atomic::Ordering;

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
    _tableref: &'a HTable<Data, Data>,
    /// the shard locks
    shard_locks: Vec<MapRWLGuard<'a, std::collections::HashMap<Data, SharedValue<Data>>>>,
}

impl<'a> ShardLock<'a> {
    /// Initialize a shard lock from a provided table: DARN, **this is blocking** because
    /// it will wait for every writer in every stripe to exit before returning. So, know
    /// what you're doing beforehand!
    pub fn init(_tableref: &'a HTable<Data, Data>) -> Self {
        let shard_locks = _tableref
            .get_shards()
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

// DROP impl isn't required as ShardLock's field types need-drop (std::mem)

/// The key/value engine that acts as the in-memory backing store for the database
pub struct KVEngine {
    /// the atomic table
    table: HTable<Data, Data>,
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
        Self {
            table: HTable::new(),
            encoded_k: AtomicBool::new(encoded_k),
            encoded_v: AtomicBool::new(encoded_v),
        }
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
    /// Truncate the table
    pub fn truncate_table(&self) {
        self.table.clear()
    }
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<MapSingleReference<Data, Data>> {
        self.table.get(key.as_ref())
    }
}

#[test]
fn tbl() {
    let tbl = KVEngine::default();
    assert!(tbl.get("123").is_none());
}
