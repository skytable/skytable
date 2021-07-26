/*
 * Created on Tue Jul 20 2021
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

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

use crate::coredb::memstore::DdlError;
use crate::coredb::memstore::Keyspace;
use crate::coredb::memstore::Memstore;
use crate::coredb::memstore::ObjectID;
use crate::coredb::memstore::DEFAULT;
use crate::coredb::table::Table;
use crate::kvengine::KVEngine;
use crate::registry;
use crate::storage;
use crate::util::Unwrappable;
use crate::IoResult;
use crate::SnapshotConfig;
use std::sync::Arc;

pub(super) type KeyspaceResult<T> = Result<T, DdlError>;

/// The top level abstraction for the in-memory store. This is free to be shared across
/// threads, cloned and well, whatever. Most importantly, clones have an independent container
/// state that is the state of one connection and its container state preferences are never
/// synced across instances. This is important (see the impl for more info)
pub struct Corestore {
    /// the default keyspace for this instance of the object
    cks: Option<Arc<Keyspace>>,
    /// the current table for this instance of the object
    ctable: Option<Arc<Table>>,
    /// an atomic reference to the actual backing storage
    store: Arc<Memstore>,
}

impl Clone for Corestore {
    fn clone(&self) -> Self {
        // this is very important: DO NOT use the derive macro for clones
        // as it will clone the connection local state over to all connections
        // we never want this!
        Self {
            cks: None,
            ctable: None,
            store: Arc::clone(&self.store),
        }
    }
}

impl Corestore {
    /// This is the only function you'll ever need to either create a new database instance
    /// or restore from an earlier instance
    pub fn init_with_snapcfg(snapcfg: Option<SnapshotConfig>) -> IoResult<Self> {
        let store =
            storage::unflush::read_full(option_unwrap_or!(snapcfg, SnapshotConfig::default()))?;
        Ok(Self::default_with_store(store))
    }
    pub fn default_with_store(store: Memstore) -> Self {
        let cks = unsafe { store.get_keyspace_atomic_ref(DEFAULT).unsafe_unwrap() };
        let ctable = unsafe { cks.get_table_atomic_ref(DEFAULT).unsafe_unwrap() };
        Self {
            cks: Some(cks),
            ctable: Some(ctable),
            store: Arc::new(store),
        }
    }
    /// Swap out the current keyspace with a different one
    ///
    /// If the keyspace is non-existent then false is returned, else true is
    /// returned
    pub fn swap_ks(&mut self, id: ObjectID) -> KeyspaceResult<()> {
        match self.store.get_keyspace_atomic_ref(id) {
            Some(ks) => {
                // important: Don't forget to reset the table when switching keyspaces
                self.ctable = None;
                self.cks = Some(ks)
            }
            None => return Err(DdlError::DefaultNotFound),
        }
        Ok(())
    }
    /// Swap out the current table with a different one
    ///
    /// If the table is non-existent or the default keyspace was unset, then
    /// false is returned. Else true is returned
    pub fn swap_table(&mut self, id: ObjectID) -> KeyspaceResult<()> {
        match &self.cks {
            Some(ks) => match ks.get_table_atomic_ref(id) {
                Some(tbl) => self.ctable = Some(tbl),
                None => return Err(DdlError::ObjectNotFound),
            },
            None => return Err(DdlError::DefaultNotFound),
        }
        Ok(())
    }
    /// Get the key/value store
    ///
    /// `Err`s are propagated if the target table has an incorrect table or if
    /// the default table is unset
    pub fn get_kvstore(&self) -> KeyspaceResult<&KVEngine> {
        match &self.ctable {
            Some(tbl) => match tbl.get_kvstore() {
                Ok(kvs) => Ok(kvs),
                _ => Err(DdlError::WrongModel),
            },
            None => Err(DdlError::DefaultNotFound),
        }
    }
    pub fn is_snapshot_enabled(&self) -> bool {
        self.store.snap_config.is_some()
    }

    /// Create a table: in-memory; **no transactional guarantees**. Two tables can be created
    /// simultaneously, but are never flushed unless we are very lucky. If the global flush
    /// system is close to a flush cycle -- then we are in luck: we pause the flush cycle
    /// through a global flush lock and then allow it to resume once we're done adding the table.
    /// This enables the flush routine to permanently write the table to disk. But it's all about
    /// luck -- the next mutual access may be yielded to the next `create table` command
    pub fn create_table(
        &self,
        ksid: ObjectID,
        tblid: ObjectID,
        modelcode: u8,
        volatile: bool,
    ) -> KeyspaceResult<()> {
        // first lock the global flush state
        let flush_lock = registry::lock_flush_state();
        let ret = match &self.store.get_keyspace_atomic_ref(ksid) {
            Some(ks) => {
                let tbl = Table::from_model_code(modelcode, volatile);
                if let Some(tbl) = tbl {
                    if ks.create_table(tblid.clone(), tbl) {
                        Ok(())
                    } else {
                        Err(DdlError::AlreadyExists)
                    }
                } else {
                    Err(DdlError::WrongModel)
                }
            }
            None => Err(DdlError::ObjectNotFound),
        };
        // free the global flush lock
        drop(flush_lock);
        ret
    }

    /// Drop a table
    pub fn drop_table(&self, ksid: ObjectID, tblid: ObjectID) -> KeyspaceResult<()> {
        match self.store.get_keyspace_atomic_ref(ksid) {
            Some(ks) => ks.drop_table(tblid),
            None => Err(DdlError::ObjectNotFound),
        }
    }

    /// Create a keyspace **without any transactional guarantees**
    pub fn create_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
        // lock the global flush lock (see comment in create_table to know why)
        let flush_lock = registry::lock_flush_state();
        let ret = if self.store.create_keyspace(ksid) {
            // woo, created
            Ok(())
        } else {
            // ugh, already exists
            Err(DdlError::AlreadyExists)
        };
        drop(flush_lock);
        ret
    }

    /// Drop a keyspace
    pub fn drop_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
        self.store.drop_keyspace(ksid)
    }
}
