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
//! -----------------------------------------------------
//! |                                                   |
//! |  |-------------------|     |-------------------|  |
//! |  |-------------------|     |-------------------|  |
//! |  | | TABLE | TABLE | |     | | TABLE | TABLE | |  |
//! |  | |-------|-------| |     | |-------|-------| |  |
//! |  |      Keyspace     |     |      Keyspace     |  |
//! |  |-------------------|     |-------------------|  |
//!                                                     |
//! |  |-------------------|     |-------------------|  |
//! |  | |-------|-------| |     | |-------|-------| |  |
//! |  | | TABLE | TABLE | |     | | TABLE | TABLE | |  |
//! |  | |-------|-------| |     | |-------|-------| |  |
//! |  |      Keyspace     |     |      Keyspace     |  |
//! |  |-------------------|     |-------------------|  |
//! |                                                   |
//! |                                                   |
//! |                                                   |
//! -----------------------------------------------------
//! |                         NODE                      |
//! |---------------------------------------------------|
//! ```
//!
//! So, all your data is at the mercy of [`Memstore`]'s constructor
//! and destructor.

use super::KeyspaceResult;
use crate::corestore::array::Array;
use crate::corestore::htable::Coremap;
use crate::corestore::lock::{QLGuard, QuickLock};
use crate::corestore::table::Table;
use crate::corestore::SnapshotStatus;
use crate::SnapshotConfig;
use core::borrow::Borrow;
use core::hash::Hash;
use core::mem::MaybeUninit;
use std::sync::Arc;

#[sky_macros::array]
const DEFAULT_ARRAY: [MaybeUninit<u8>; 64] = [b'd', b'e', b'f', b'a', b'u', b'l', b't'];

#[sky_macros::array]
const SYSTEM_ARRAY: [MaybeUninit<u8>; 64] = [b's', b'y', b's', b't', b'e', b'm'];

/// typedef for the keyspace/table IDs. We don't need too much fancy here,
/// no atomic pointers and all. Just a nice array. With amazing gurantees
pub type ObjectID = Array<u8, 64>;

/// The `DEFAULT` array (with the rest uninit)
pub const DEFAULT: ObjectID = Array::from_const(DEFAULT_ARRAY, 7);
pub const SYSTEM: ObjectID = Array::from_const(SYSTEM_ARRAY, 6);

#[test]
fn test_def_macro_sanity() {
    // just make sure our macro is working as expected
    let mut def = DEFAULT.clone();
    def.push(b'?');
    unsafe {
        assert_eq!(def.as_str(), "default?");
        let mut sys = SYSTEM.clone();
        sys.push(b'?');
        assert_eq!(sys.as_str(), "system?");
    }
}

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
/// Errors arising from trying to modify/access containers
pub enum DdlError {
    /// The object is still in use
    StillInUse,
    /// The object couldn't be found
    ObjectNotFound,
    /// The object is not user-accessible
    ProtectedObject,
    /// The default object wasn't found
    DefaultNotFound,
    /// Incorrect data model semantics were used on a data model
    WrongModel,
    /// The object already exists
    AlreadyExists,
    /// The target object is not ready
    NotReady,
    /// The DDL transaction failed
    DdlTransactionFailure,
}

#[derive(Debug)]
/// The core in-memory table
///
/// This in-memory table that houses all keyspaces along with other node properties.
/// This is the structure that you should clone in an atomic RC wrapper. This object
/// handles no sort of persistence
pub struct Memstore {
    /// the keyspaces
    pub keyspaces: Coremap<ObjectID, Arc<Keyspace>>,
    /// the snapshot configuration
    pub snap_config: Option<SnapshotStatus>,
    /// A **virtual lock** on the preload file
    preload_lock: QuickLock<()>,
}

impl Memstore {
    /// Create a new empty in-memory table with literally nothing in it
    pub fn new_empty() -> Self {
        Self {
            keyspaces: Coremap::new(),
            snap_config: None,
            preload_lock: QuickLock::new(()),
        }
    }
    pub fn init_with_all(
        keyspaces: Coremap<ObjectID, Arc<Keyspace>>,
        snap_config: &SnapshotConfig,
    ) -> Self {
        Self {
            keyspaces,
            snap_config: if let SnapshotConfig::Enabled(pref) = snap_config {
                Some(SnapshotStatus::new(pref.atmost))
            } else {
                None
            },
            preload_lock: QuickLock::new(()),
        }
    }
    /// Create a new in-memory table with the default keyspace and the default
    /// tables. So, whenever you're calling this, this is what you get:
    /// ```json
    /// "YOURNODE": {
    ///     "KEYSPACES": [
    ///         "default" : {
    ///             "TABLES": ["default"]
    ///         },
    ///         "system": {
    ///             "TABLES": []
    ///         }
    ///     ]
    /// }
    /// ```
    ///
    /// When you connect a client without any information about the keyspace you're planning to
    /// use, you'll be connected to `ks:default/table:default`. The `ks:default/table:_system` is not
    /// for you. It's for the system
    pub fn new_default() -> Self {
        Self {
            keyspaces: {
                let n = Coremap::new();
                n.true_if_insert(DEFAULT, Arc::new(Keyspace::empty_default()));
                n.true_if_insert(SYSTEM, Arc::new(Keyspace::empty()));
                n
            },
            snap_config: None,
            preload_lock: QuickLock::new(()),
        }
    }
    /// Get an atomic reference to a keyspace
    pub fn get_keyspace_atomic_ref<Q>(&self, keyspace_identifier: &Q) -> Option<Arc<Keyspace>>
    where
        ObjectID: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.keyspaces
            .get(&keyspace_identifier)
            .map(|ns| ns.clone())
    }
    /// Returns true if a new keyspace was created
    pub fn create_keyspace(&self, keyspace_identifier: ObjectID) -> bool {
        self.keyspaces
            .true_if_insert(keyspace_identifier, Arc::new(Keyspace::empty()))
    }
    pub fn drop_keyspace<Q>(&self, ksid: &Q) -> KeyspaceResult<()>
    where
        ObjectID: Borrow<Q>,
        Q: Hash + Eq + PartialEq<ObjectID> + ?Sized,
    {
        if ksid.eq(&SYSTEM) || ksid.eq(&DEFAULT) {
            Err(DdlError::ProtectedObject)
        } else if !self.keyspaces.contains_key(&ksid) {
            Err(DdlError::ObjectNotFound)
        } else {
            let good_to_remove = self.keyspaces.true_remove_if(&ksid, |_, arc| {
                Arc::strong_count(arc) == 1 && arc.table_count() == 0
            });
            if good_to_remove {
                Ok(())
            } else {
                Err(DdlError::StillInUse)
            }
        }
    }
}

#[derive(Debug)]
/// A keyspace houses all the other tables
pub struct Keyspace {
    /// the tables
    pub tables: Coremap<ObjectID, Arc<Table>>,
    /// the replication strategy for this keyspace
    replication_strategy: cluster::ReplicationStrategy,
    /// A **virtual lock** on the partmap for this keyspace
    partmap_lock: QuickLock<()>,
}

#[cfg(test)]
macro_rules! unsafe_objectid_from_slice {
    ($slice:expr) => {{
        unsafe { ObjectID::from_slice($slice) }
    }};
}

impl Keyspace {
    /// Create a new empty keyspace with the default tables: a `default` table
    pub fn empty_default() -> Self {
        Self {
            tables: {
                let ht = Coremap::new();
                // add the default table
                ht.true_if_insert(DEFAULT, Arc::new(Table::new_default_kve()));
                ht
            },
            replication_strategy: cluster::ReplicationStrategy::default(),
            partmap_lock: QuickLock::new(()),
        }
    }
    pub fn init_with_all_def_strategy(tables: Coremap<ObjectID, Arc<Table>>) -> Self {
        Self {
            tables,
            replication_strategy: cluster::ReplicationStrategy::default(),
            partmap_lock: QuickLock::new(()),
        }
    }
    /// Create a new empty keyspace with zero tables
    pub fn empty() -> Self {
        Self {
            tables: Coremap::new(),
            replication_strategy: cluster::ReplicationStrategy::default(),
            partmap_lock: QuickLock::new(()),
        }
    }
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
    /// Get an atomic reference to a table in this keyspace if it exists
    pub fn get_table_atomic_ref<Q>(&self, table_identifier: &Q) -> Option<Arc<Table>>
    where
        ObjectID: Borrow<Q>,
        Q: Hash + Eq + PartialEq<ObjectID> + ?Sized,
    {
        self.tables.get(&table_identifier).map(|v| v.clone())
    }
    /// Create a new table
    pub fn create_table(&self, tableid: ObjectID, table: Table) -> bool {
        self.tables.true_if_insert(tableid, Arc::new(table))
    }
    /// Drop a table if it exists, if it is not forbidden and if no one references
    /// back to it. We don't want any looming table references i.e table gets deleted
    /// for the current connection and newer connections, but older instances still
    /// refer to the table.
    // FIXME(@ohsayan): Should we actually care?
    pub fn drop_table<Q>(&self, table_identifier: &Q) -> KeyspaceResult<()>
    where
        ObjectID: Borrow<Q>,
        Q: Hash + Eq + PartialEq<ObjectID> + ?Sized,
    {
        if table_identifier.eq(&DEFAULT) {
            Err(DdlError::ProtectedObject)
        } else if !self.tables.contains_key(&table_identifier) {
            Err(DdlError::ObjectNotFound)
        } else {
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
        }
    }
    /// Remove a table without doing any reference checks. This will just pull it off
    pub unsafe fn force_remove_table(&self, tblid: &ObjectID) {
        // atomic remember? nobody cares about the result
        self.tables.remove(tblid);
    }
    pub fn lock_partmap(&self) -> QLGuard<'_, ()> {
        self.partmap_lock.lock()
    }
}

#[test]
fn test_keyspace_drop_no_atomic_ref() {
    let our_keyspace = Keyspace::empty_default();
    assert!(our_keyspace.create_table(
        unsafe_objectid_from_slice!("apps"),
        Table::new_default_kve()
    ));
    assert!(our_keyspace
        .drop_table(&unsafe_objectid_from_slice!("apps"))
        .is_ok());
}

#[test]
fn test_keyspace_drop_fail_with_atomic_ref() {
    let our_keyspace = Keyspace::empty_default();
    assert!(our_keyspace.create_table(
        unsafe_objectid_from_slice!("apps"),
        Table::new_default_kve()
    ));
    let _atomic_tbl_ref = our_keyspace
        .get_table_atomic_ref(&unsafe_objectid_from_slice!("apps"))
        .unwrap();
    assert_eq!(
        our_keyspace
            .drop_table(&unsafe_objectid_from_slice!("apps"))
            .unwrap_err(),
        DdlError::StillInUse
    );
}

#[test]
fn test_keyspace_try_delete_protected_table() {
    let our_keyspace = Keyspace::empty_default();
    assert_eq!(
        our_keyspace
            .drop_table(&unsafe_objectid_from_slice!("default"))
            .unwrap_err(),
        DdlError::ProtectedObject
    );
}
