/*
 * Created on Mon Jul 13 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # The core database engine

use crate::config::SnapshotConfig;
use crate::config::SnapshotPref;
use crate::coredb::htable::HTable;
use crate::dbnet::connection::prelude::*;
use crate::diskstore;
use crate::protocol::Query;
use crate::queryengine;
pub use htable::Data;
use libsky::TResult;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::sync::Arc;
pub mod htable;

/// This is a thread-safe database handle, which on cloning simply
/// gives another atomic reference to the `shared` which is a `Shared` object
#[derive(Debug, Clone)]
pub struct CoreDB {
    /// The shared object, which contains a `Shared` object wrapped in a thread-safe
    /// RC
    pub shared: Arc<Shared>,
    /// The number of snapshots that are to be kept at the most
    ///
    /// If this is set to Some(0), then all the snapshots will be kept. Otherwise, if it is set to
    /// Some(n), n ∈ Z<sup>+</sup> — then _n_ snapshots will be kept at the maximum. If set to `None`, snapshotting is disabled.
    pub snapcfg: Arc<Option<SnapshotStatus>>,
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
    pub in_progress: RwLock<bool>,
}

impl SnapshotStatus {
    /// Create a new `SnapshotStatus` instance with preset values
    ///
    /// **Note: ** The initial state of the snapshot service is set to false
    pub fn new(max: usize) -> Self {
        SnapshotStatus {
            max,
            in_progress: RwLock::new(false),
        }
    }

    /// Set `in_progress` to true
    pub fn lock_snap(&self) {
        *self.in_progress.write() = true;
    }

    /// Set `in_progress` to false
    pub fn unlock_snap(&self) {
        *self.in_progress.write() = false;
    }

    /// Check if `in_progress` is set to true
    pub fn is_busy(&self) -> bool {
        *self.in_progress.read()
    }
}

/// A shared _state_
#[derive(Debug)]
pub struct Shared {
    /// A `Coretable` wrapped in a R/W lock
    pub table: RwLock<Coretable>,
}

/// The `Coretable` holds all the key-value pairs in a `HTable`
#[derive(Debug)]
pub struct Coretable {
    /// The core table contain key-value pairs
    coremap: HTable<Data, Data>,
    /// Whether the database is poisoned or not
    ///
    /// If the database is poisoned -> the database can no longer accept writes
    /// but can only accept reads
    pub poisoned: bool,
}

impl Coretable {
    /// Get a reference to the inner `HTable`
    pub const fn get_ref<'a>(&'a self) -> &'a HTable<Data, Data> {
        &self.coremap
    }
    /// Get a **mutable** reference to the inner `HTable`
    pub fn get_mut_ref<'a>(&'a mut self) -> &'a mut HTable<Data, Data> {
        &mut self.coremap
    }
}

impl CoreDB {
    #[cfg(debug_assertions)]
    #[allow(dead_code)] // This has been kept for debugging purposes, so we'll suppress this lint
    /// Flush the coretable entries when in debug mode
    pub fn print_debug_table(&self) {
        if self.acquire_read().coremap.len() == 0 {
            println!("In-memory table is empty");
        } else {
            println!("{:#?}", self.acquire_read());
        }
    }
    pub fn poison(&self) {
        (*self.shared).table.write().poisoned = true;
    }

    pub fn unpoison(&self) {
        (*self.shared).table.write().poisoned = false;
    }

    /// Check if snapshotting is enabled
    pub fn is_snapshot_enabled(&self) -> bool {
        self.snapcfg.is_some()
    }

    /// Mark the snapshotting service to be busy
    ///
    /// ## Panics
    /// If snapshotting is disabled, this will panic
    pub fn lock_snap(&self) {
        (*self.snapcfg).as_ref().unwrap().lock_snap();
    }

    /// Mark the snapshotting service to be free
    ///
    /// ## Panics
    /// If snapshotting is disabled, this will panic
    pub fn unlock_snap(&self) {
        (*self.snapcfg).as_ref().unwrap().unlock_snap();
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
                queryengine::execute_simple(&self, con, q).await?;
                con.flush_stream().await?;
            }
            // TODO(@ohsayan): Pipeline commands haven't been implemented yet
            Query::PipelinedQuery(_) => unimplemented!(),
        }
        Ok(())
    }

    /// Create a new `CoreDB` instance
    ///
    /// This also checks if a local backup of previously saved data is available.
    /// If it is - it restores the data. Otherwise it creates a new in-memory table
    pub fn new(snapshot_cfg: &SnapshotConfig, restore_file: Option<String>) -> TResult<Self> {
        let coretable = diskstore::get_saved(restore_file)?;
        let mut snap_count = None;
        if let SnapshotConfig::Enabled(SnapshotPref { every: _, atmost }) = snapshot_cfg {
            snap_count = Some(atmost);
        }
        let snapcfg = snap_count
            .map(|max| Arc::new(Some(SnapshotStatus::new(*max))))
            .unwrap_or(Arc::new(None));
        let db = if let Some(coretable) = coretable {
            CoreDB {
                shared: Arc::new(Shared {
                    table: RwLock::new(Coretable {
                        coremap: coretable,
                        poisoned: false,
                    }),
                }),
                snapcfg,
            }
        } else {
            CoreDB::new_empty(snapcfg)
        };
        Ok(db)
    }
    /// Create an empty in-memory table
    pub fn new_empty(snapcfg: Arc<Option<SnapshotStatus>>) -> Self {
        CoreDB {
            shared: Arc::new(Shared {
                table: RwLock::new(Coretable {
                    coremap: HTable::<Data, Data>::new(),
                    poisoned: false,
                }),
            }),
            snapcfg,
        }
    }
    /// Check if the database object is poisoned, that is, data couldn't be written
    /// to disk once, and hence, we have disabled write operations
    pub fn is_poisoned(&self) -> bool {
        (*self.shared).table.read().poisoned
    }
    /// Acquire a write lock
    pub fn acquire_write(&self) -> Option<RwLockWriteGuard<'_, Coretable>> {
        if self.is_poisoned() {
            None
        } else {
            Some(self.shared.table.write())
        }
    }
    /// Acquire a read lock
    pub fn acquire_read(&self) -> RwLockReadGuard<'_, Coretable> {
        self.shared.table.read()
    }

    #[cfg(test)]
    /// Get a deep copy of the `HTable`
    ///
    /// **⚠ Do note**: This is super inefficient since it performs an actual
    /// clone of the `HTable` and doesn't do any `Arc`-business! This function
    /// can be used by test functions and the server, but **use with caution!**
    pub fn get_htable_deep_clone(&self) -> HTable<Data, Data> {
        (*self.acquire_read().get_ref()).clone()
    }
}
