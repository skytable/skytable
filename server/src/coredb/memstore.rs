/*
 * Created on Fri Jul 02 2021
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

//! # In-memory store
//!
//! This is what things look like:
//! ```text
//! ------------------------------------------------------
//! |                          |                         |
//! |  |-------------------|   |  |-------------------|  |
//! |  |-------------------|   |  |-------------------|  |
//! |  | | TABLE | TABLE | |   |  | | TABLE | TABLE | |  |
//! |  | |-------|-------| |   |  | |-------|-------| |  |
//! |  |      Keyspace     |   |  |      Keyspace     |  |
//! |  |-------------------|   |  |-------------------|  |
//!                            |                         |
//! |  |-------------------|   |  |-------------------|  |
//! |  | |-------|-------| |   |  | |-------|-------| |  |
//! |  | | TABLE | TABLE | |   |  | | TABLE | TABLE | |  |
//! |  | |-------|-------| |   |  | |-------|-------| |  |
//! |  |      Keyspace     |   |  |      Keyspace     |  |
//! |  |-------------------|   |  |-------------------|  |
//! |                          |                         |
//! |                          |                         |
//! |        NAMESPACE         |        NAMESPACE        |
//! ------------------------------------------------------
//! |                         NODE                       |
//! |----------------------------------------------------|
//! ```
//!
//! So, all your data is at the mercy of [`Memstore`]'s constructor
//! and destructor.

#![allow(dead_code)] // TODO(@ohsayan): Remove this onece we're done

use crate::coredb::array::Array;
use crate::coredb::htable::Coremap;
use crate::coredb::htable::Data;
use crate::coredb::SnapshotStatus;
use crate::kvengine::KVEngine;
use core::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[sky_macros::array]
const DEFAULT_ARRAY: [MaybeUninit<u8>; 64] = [b'd', b'e', b'f', b'a', b'u', b'l', b't'];

/// The `DEFAULT` array (with the rest uninit)
pub const DEFAULT: Array<u8, 64> = Array::from_const(DEFAULT_ARRAY, 7);

#[test]
fn test_def_macro_sanity() {
    // just make sure our macro is working as expected
    let mut def = DEFAULT.clone();
    def.push(b'?');
    assert_eq!(
        def.into_iter().map(char::from).collect::<String>(),
        "default?".to_owned()
    );
}

/// typedef for the namespace/keyspace IDs. We don't need too much fancy here,
/// no atomic pointers and all. Just a nice array. With amazing gurantees
type NsKsTblId = Array<u8, 64>;

mod cluster {
    /// This is for the future where every node will be allocated a shard
    #[derive(Debug)]
    pub enum ClusterShardRange {
        SingleNode,
    }

    impl Default for ClusterShardRange {
        fn default() -> Self {
            Self::SingleNode
        }
    }

    /// This is for the future for determining the replication strategy
    #[derive(Debug)]
    pub enum ReplicationStrategy {
        /// Single node, no replica sets
        Default,
    }

    impl Default for ReplicationStrategy {
        fn default() -> Self {
            Self::Default
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DdlError {
    StillInUse,
    ObjectNotFound,
    ProtectedObject,
}

#[derive(Debug)]
/// The core in-memory table
///
/// This in-memory table that houses all keyspaces and namespaces along with other node
/// properties. This is the structure that you should clone and send around connections
/// for connection-level control abilities over the namespace
pub struct Memstore {
    /// the namespaces
    namespaces: Arc<Coremap<NsKsTblId, Arc<Namespace>>>,
}

impl Memstore {
    /// Create a new empty in-memory table with literally nothing in it
    pub fn new_empty() -> Self {
        Self {
            namespaces: Arc::new(Coremap::new()),
        }
    }
    /// Create a new in-memory table with the default namespace, keyspace and the default
    /// tables. So, whenever you're calling this, this is what you get:
    /// ```text
    /// YOURNODE: {
    ///     NAMESPACES: [
    ///         "default" : {
    ///             KEYSPACES: ["default", "_system"]
    ///         }
    ///     ]
    /// }
    /// ```
    ///
    /// When you connect a client without any information about the namespace you're planning to
    /// use, you'll be connected to `ns:default/ks:default`. The `ns:default/ks:_system` is not
    /// for you. It's for the system
    pub fn new_default() -> Self {
        Self {
            namespaces: {
                let n = Coremap::new();
                n.true_if_insert(DEFAULT, Arc::new(Namespace::empty_default()));
                Arc::new(n)
            },
        }
    }
    /// Get an atomic reference to a namespace
    pub fn get_namespace_atomic_ref(
        &self,
        namespace_identifier: NsKsTblId,
    ) -> Option<Arc<Namespace>> {
        self.namespaces
            .get(&namespace_identifier)
            .map(|ns| ns.clone())
    }
    /// Returns true if a new namespace was created
    pub fn create_namespace(&self, namespace_identifier: NsKsTblId) -> bool {
        self.namespaces
            .true_if_insert(namespace_identifier, Arc::new(Namespace::empty()))
    }
}

#[derive(Debug)]
/// Namespaces hold keyspaces
pub struct Namespace {
    /// the keyspaces stored in this namespace
    keyspaces: Coremap<NsKsTblId, Arc<Keyspace>>,
    /// the shard range
    shard_range: cluster::ClusterShardRange,
}

/// The date model of a table
pub enum TableType {
    KeyValue,
}

impl Namespace {
    /// Create an empty namespace with no keyspaces
    pub fn empty() -> Self {
        Self {
            keyspaces: Coremap::new(),
            shard_range: cluster::ClusterShardRange::default(),
        }
    }
    /// Create an empty namespace with the default keyspace that has a table `default` and
    /// a table `system`
    pub fn empty_default() -> Self {
        Self {
            keyspaces: {
                let ks = Coremap::new();
                ks.true_if_insert(DEFAULT, Arc::new(Keyspace::empty_default()));
                ks
            },
            shard_range: cluster::ClusterShardRange::default(),
        }
    }
    /// Get an atomic reference to a keyspace, if it exists
    pub fn get_keyspace_atomic_ref(&self, keyspace_idenitifer: NsKsTblId) -> Option<Arc<Keyspace>> {
        self.keyspaces.get(&keyspace_idenitifer).map(|v| v.clone())
    }
    /// Create a new keyspace if it doesn't exist
    pub fn create_keyspace(&self, keyspace_idenitifer: NsKsTblId) -> bool {
        self.keyspaces
            .true_if_insert(keyspace_idenitifer, Arc::new(Keyspace::empty()))
    }
    /// Drop a keyspace if it is not in use **and** it is empty and not the default
    pub fn drop_keyspace(&self, keyspace_idenitifer: NsKsTblId) -> Result<(), DdlError> {
        if keyspace_idenitifer.eq(&DEFAULT) {
            // can't delete default keyspace
            Err(DdlError::ProtectedObject)
        } else if self.keyspaces.contains_key(&keyspace_idenitifer) {
            // has table
            let did_remove =
                self.keyspaces
                    .true_remove_if(&keyspace_idenitifer, |_ks_id, ks_atomic_ref| {
                        // 1 because this should just be us, the one instance
                        // also the keyspace must be empty
                        ks_atomic_ref.tables.len() == 0 && Arc::strong_count(ks_atomic_ref) == 1
                    });
            if did_remove {
                Ok(())
            } else {
                Err(DdlError::StillInUse)
            }
        } else {
            Err(DdlError::ObjectNotFound)
        }
    }
}

// TODO(@ohsayan): Optimize the memory layouts of the UDFs to ensure that sharing is very cheap

#[derive(Debug)]
/// A keyspace houses all the other tables
pub struct Keyspace {
    /// the tables
    tables: Coremap<Data, Arc<Table>>,
    /// current state of the disk flush status. if this is true, we're safe to
    /// go ahead with writes
    flush_state_healthy: AtomicBool,
    /// the snapshot configuration for this namespace
    snap_config: Option<SnapshotStatus>,
    /// the replication strategy for this namespace
    replication_strategy: cluster::ReplicationStrategy,
}

impl Keyspace {
    /// Create a new empty keyspace with the default tables: a `default` table and a
    /// `system` table
    pub fn empty_default() -> Self {
        Self {
            tables: {
                let ht = Coremap::new();
                // add the default table
                ht.true_if_insert(
                    Data::from("default"),
                    Arc::new(Table::KV(KVEngine::default())),
                );
                // add the system table
                ht.true_if_insert(
                    Data::from("_system"),
                    Arc::new(Table::KV(KVEngine::default())),
                );
                ht
            },
            flush_state_healthy: AtomicBool::new(true),
            snap_config: None,
            replication_strategy: cluster::ReplicationStrategy::default(),
        }
    }
    /// Create a new empty keyspace with zero tables
    pub fn empty() -> Self {
        Self {
            tables: Coremap::new(),
            flush_state_healthy: AtomicBool::new(true),
            snap_config: None,
            replication_strategy: cluster::ReplicationStrategy::default(),
        }
    }
    /// Get an atomic reference to a table in this keyspace if it exists
    pub fn get_table_atomic_ref(&self, table_identifier: Data) -> Option<Arc<Table>> {
        self.tables.get(&table_identifier).map(|v| v.clone())
    }
    /// Create a new table with **default encoding**
    pub fn create_table(&self, table_identifier: Data, table_type: TableType) -> bool {
        self.tables.true_if_insert(table_identifier, {
            match table_type {
                TableType::KeyValue => Arc::new(Table::KV(KVEngine::default())),
            }
        })
    }
    pub fn drop_table(&self, table_identifier: Data) -> Result<(), DdlError> {
        if table_identifier.eq(&Data::from("default"))
            || table_identifier.eq(&Data::from("_system"))
        {
            Err(DdlError::ProtectedObject)
        } else if self.tables.contains_key(&table_identifier) {
            // has table
            let did_remove =
                self.tables
                    .true_remove_if(&table_identifier, |_table_id, table_atomic_ref| {
                        // 1 because this should just be us, the one instance
                        Arc::strong_count(table_atomic_ref) == 1
                    });
            if did_remove {
                Ok(())
            } else {
                Err(DdlError::StillInUse)
            }
        } else {
            Err(DdlError::ObjectNotFound)
        }
    }
}

#[test]
fn test_keyspace_drop_no_atomic_ref() {
    let our_keyspace = Keyspace::empty_default();
    assert!(our_keyspace.create_table(Data::from("apps"), TableType::KeyValue));
    assert!(our_keyspace.drop_table(Data::from("apps")).is_ok());
}

#[test]
fn test_keyspace_drop_fail_with_atomic_ref() {
    let our_keyspace = Keyspace::empty_default();
    assert!(our_keyspace.create_table(Data::from("apps"), TableType::KeyValue));
    let _atomic_tbl_ref = our_keyspace
        .get_table_atomic_ref(Data::from("apps"))
        .unwrap();
    assert_eq!(
        our_keyspace.drop_table(Data::from("apps")).unwrap_err(),
        DdlError::StillInUse
    );
}

#[test]
fn test_keyspace_try_delete_protected_table() {
    let our_keyspace = Keyspace::empty_default();
    assert_eq!(
        our_keyspace.drop_table(Data::from("default")).unwrap_err(),
        DdlError::ProtectedObject
    );
    assert_eq!(
        our_keyspace.drop_table(Data::from("_system")).unwrap_err(),
        DdlError::ProtectedObject
    );
}

// same 8 byte ptrs; any chance of optimizations?

#[derive(Debug)]
/// The underlying table type. This is the place for the other data models (soon!)
pub enum Table {
    /// a key/value store
    KV(KVEngine),
}

impl Table {
    /// Get the key/value store if the table is a key/value store
    pub const fn get_kvstore(&self) -> Option<&KVEngine> {
        #[allow(irrefutable_let_patterns)]
        if let Self::KV(kvs) = self {
            Some(kvs)
        } else {
            None
        }
    }
}
