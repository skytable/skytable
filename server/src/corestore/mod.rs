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

use crate::corestore::lock::QLGuard;
use crate::corestore::memstore::DdlError;
use crate::corestore::memstore::Keyspace;
use crate::corestore::memstore::Memstore;
use crate::corestore::memstore::ObjectID;
use crate::corestore::memstore::DEFAULT;
use crate::corestore::table::Table;
use crate::dbnet::connection::ProtocolConnectionExt;
use crate::kvengine::KVEngine;
use crate::protocol::Query;
use crate::queryengine;
use crate::registry;
use crate::storage;
use crate::util::Unwrappable;
use crate::IoResult;
use crate::SnapshotConfig;
pub use htable::Data;
use libsky::TResult;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub mod array;
pub mod buffers;
pub mod htable;
pub mod iarray;
pub mod lazy;
pub mod lock;
pub mod memstore;
pub mod table;

pub(super) type KeyspaceResult<T> = Result<T, DdlError>;

/// The top level abstraction for the in-memory store. This is free to be shared across
/// threads, cloned and well, whatever. Most importantly, clones have an independent container
/// state that is the state of one connection and its container state preferences are never
/// synced across instances. This is important (see the impl for more info)
#[derive(Debug, Clone)]
pub struct Corestore {
    /// the default keyspace for this instance of the object
    cks: Option<Arc<Keyspace>>,
    /// the current table for this instance of the object
    ctable: Option<Arc<Table>>,
    /// an atomic reference to the actual backing storage
    store: Arc<Memstore>,
}

/// The status and details of the snapshotting service
///
/// The in_progress field is kept behind a mutex to ensure only one snapshot
/// operation can run at a time. Although on the server side this isn't a problem
/// because we don't have multiple snapshot tasks, but can be an issue when external
/// snapshots are triggered, for example via `MKSNAP`
#[derive(Debug)]
pub struct SnapshotStatus {
    /// The maximum number of recent snapshots to keep
    pub max: usize,
    /// The current state of the snapshot service
    pub in_progress: lock::QuickLock<()>,
}

impl SnapshotStatus {
    /// Create a new `SnapshotStatus` instance with preset values
    pub fn new(max: usize) -> Self {
        SnapshotStatus {
            max,
            in_progress: lock::QuickLock::new(()),
        }
    }

    /// Lock the snapshot service
    pub fn lock_snap(&self) -> lock::QLGuard<'_, ()> {
        self.in_progress.lock()
    }

    /// Check if the snapshot service is busy
    pub fn is_busy(&self) -> bool {
        self.in_progress.is_locked()
    }
}

impl Corestore {
    /// This is the only function you'll ever need to either create a new database instance
    /// or restore from an earlier instance
    pub fn init_with_snapcfg(snapcfg: &SnapshotConfig) -> IoResult<Self> {
        let store = storage::unflush::read_full(snapcfg)?;
        Ok(Self::default_with_store(store))
    }
    pub fn lock_snap(&self) -> QLGuard<'_, ()> {
        match &self.store.snap_config {
            Some(lck) => lck.lock_snap(),
            None => unsafe { impossible!() },
        }
    }
    pub fn get_snapstatus(&self) -> &SnapshotStatus {
        match &self.store.snap_config {
            Some(sc) => sc,
            None => unsafe { impossible!() },
        }
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

    pub fn get_store(&self) -> &Memstore {
        &self.store
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
        tblid: ObjectID,
        modelcode: u8,
        volatile: bool,
    ) -> KeyspaceResult<()> {
        // first lock the global flush state
        let flush_lock = registry::lock_flush_state();
        let ret = match &self.cks {
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
            None => Err(DdlError::DefaultNotFound),
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

    /// Execute a query that has already been validated by `Connection::read_query`
    pub async fn execute_query<T, Strm>(&self, query: Query, con: &mut T) -> TResult<()>
    where
        T: ProtocolConnectionExt<Strm>,
        Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
    {
        match query {
            Query::SimpleQuery(q) => {
                con.write_simple_query_header().await?;
                queryengine::execute_simple(self, con, q).await?;
                con.flush_stream().await?;
            }
            // TODO(@ohsayan): Pipeline commands haven't been implemented yet
            Query::PipelinedQuery(_) => unimplemented!(),
        }
        Ok(())
    }
    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
}
