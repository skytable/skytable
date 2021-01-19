/*
 * Created on Mon Jul 13 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

use crate::config::BGSave;
use crate::config::SnapshotConfig;
use crate::config::SnapshotPref;
use crate::dbnet::Con;
use crate::diskstore;
use crate::protocol::Query;
use crate::queryengine;
use bytes::Bytes;
use diskstore::PERSIST_FILE;
use libtdb::TResult;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio;
use tokio::sync::Notify;

/// This is a thread-safe database handle, which on cloning simply
/// gives another atomic reference to the `shared` which is a `Shared` object
#[derive(Debug, Clone)]
pub struct CoreDB {
    /// The shared object, which contains a `Shared` object wrapped in a thread-safe
    /// RC
    pub shared: Arc<Shared>,
    /// The number of background tasks that should be expected
    ///
    /// This is used by the `Drop` implementation to avoid killing the database in the event
    /// that a background service is still working. The calculation is pretty straightforward:
    /// ```text
    /// 1 (for the current process) + if bgsave is running + if snapshotting is enabled
    /// ```
    /// This should **not be changed** during runtime, and should only be initialized when `CoreDB`
    /// is first initialized
    background_tasks: usize,
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
    /// This is used by the `BGSAVE` task. `Notify` is used to signal a task
    /// to wake up
    pub bgsave_task: Notify,
    /// The snapshot service notifier
    pub snapshot_service: Notify,
    /// A `Coretable` wrapped in a R/W lock
    pub table: RwLock<Coretable>,
}

impl Shared {
    /// This task performs a `sync`hronous background save operation
    ///
    /// It runs BGSAVE and then returns control to the caller. The caller is responsible
    /// for periodically calling BGSAVE. This returns `false`, **if** the database
    /// is shutting down. Otherwise `true` is returned
    pub fn run_bgsave(&self) -> bool {
        log::trace!("BGSAVE started");
        let rlock = self.table.read();
        if rlock.terminate || rlock.poisoned {
            drop(rlock);
            return false;
        }
        // Kick in BGSAVE
        match diskstore::flush_data(&PERSIST_FILE, rlock.get_ref()) {
            Ok(_) => {
                log::info!("BGSAVE completed successfully");
                return true;
            }
            Err(e) => {
                // IMPORTANT! Drop the read lock first fella!
                drop(rlock);
                // IMPORTANT! POISON THE DATABASE, NO MORE WRITES FOR YOU!
                self.table.write().poisoned = true;
                log::error!("BGSAVE failed with error: '{}'", e);
                return false;
            }
        }
    }
    /// Check if the server has received a termination signal
    pub fn is_termsig(&self) -> bool {
        self.table.read().terminate
    }
}

/// The `Coretable` holds all the key-value pairs in a `HashMap`
/// and the `terminate` field, which when set to true will cause all other
/// background tasks to terminate
#[derive(Debug)]
pub struct Coretable {
    /// The core table contain key-value pairs
    coremap: HashMap<String, Data>,
    /// The termination signal flag
    pub terminate: bool,
    /// Whether the database is poisoned or not
    ///
    /// If the database is poisoned -> the database can no longer accept writes
    /// but can only accept reads
    pub poisoned: bool,
}

impl Coretable {
    /// Get a reference to the inner `HashMap`
    pub const fn get_ref<'a>(&'a self) -> &'a HashMap<String, Data> {
        &self.coremap
    }
    /// Get a **mutable** reference to the inner `HashMap`
    pub fn get_mut_ref<'a>(&'a mut self) -> &'a mut HashMap<String, Data> {
        &mut self.coremap
    }
}

/// A wrapper for `Bytes`
#[derive(Debug, PartialEq, Clone)]
pub struct Data {
    /// The blob of data
    blob: Bytes,
}

impl Data {
    /// Create a new blob from a string
    pub fn from_string(val: String) -> Self {
        Data {
            blob: Bytes::from(val.into_bytes()),
        }
    }
    /// Create a new blob from an existing `Bytes` instance
    pub const fn from_blob(blob: Bytes) -> Self {
        Data { blob }
    }
    /// Get the inner blob (raw `Bytes`)
    pub const fn get_blob(&self) -> &Bytes {
        &self.blob
    }
    /// Get the inner blob as an `u8` slice (coerced)
    pub fn get_inner_ref(&self) -> &[u8] {
        &self.blob
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

    /// Returns the expected `Arc::strong_count` for the `CoreDB` object
    pub const fn expected_strong_count(&self) -> usize {
        self.background_tasks + 1
    }

    /// Execute a query that has already been validated by `Connection::read_query`
    pub async fn execute_query(&self, query: Query, mut con: &mut Con<'_>) -> TResult<()> {
        match query {
            Query::Simple(q) => {
                queryengine::execute_simple(&self, &mut con, q).await?;
                // Once we're done executing, flush the stream
                con.flush_stream().await
            }
            // TODO(@ohsayan): Pipeline commands haven't been implemented yet
            Query::Pipelined(_) => unimplemented!(),
        }
    }

    /// Create a new `CoreDB` instance
    ///
    /// This also checks if a local backup of previously saved data is available.
    /// If it is - it restores the data. Otherwise it creates a new in-memory table
    pub fn new(
        bgsave: BGSave,
        snapshot_cfg: SnapshotConfig,
        restore_file: Option<PathBuf>,
    ) -> TResult<Self> {
        let coretable = diskstore::get_saved(restore_file)?;
        let mut background_tasks: usize = 0;
        if !bgsave.is_disabled() {
            background_tasks += 1;
        }
        let mut snap_count = None;
        if let SnapshotConfig::Enabled(SnapshotPref { every: _, atmost }) = snapshot_cfg {
            background_tasks += 1;
            snap_count = Some(atmost);
        }
        let snapcfg = if let Some(max) = snap_count {
            Arc::new(Some(SnapshotStatus::new(max)))
        } else {
            Arc::new(None)
        };
        let db = if let Some(coretable) = coretable {
            CoreDB {
                shared: Arc::new(Shared {
                    bgsave_task: Notify::new(),
                    table: RwLock::new(Coretable {
                        coremap: coretable,
                        terminate: false,
                        poisoned: false,
                    }),
                    snapshot_service: Notify::new(),
                }),
                background_tasks,
                snapcfg,
            }
        } else {
            CoreDB::new_empty(background_tasks, snapcfg)
        };
        // Spawn the background save task in a separate task
        tokio::spawn(diskstore::bgsave_scheduler(db.clone(), bgsave));
        // Spawn the snapshot service in a separate task
        tokio::spawn(diskstore::snapshot::snapshot_service(
            db.clone(),
            snapshot_cfg,
        ));
        Ok(db)
    }
    /// Create an empty in-memory table
    pub fn new_empty(background_tasks: usize, snapcfg: Arc<Option<SnapshotStatus>>) -> Self {
        CoreDB {
            shared: Arc::new(Shared {
                bgsave_task: Notify::new(),
                table: RwLock::new(Coretable {
                    coremap: HashMap::<String, Data>::new(),
                    terminate: false,
                    poisoned: false,
                }),
                snapshot_service: Notify::new(),
            }),
            background_tasks,
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
    /// Flush the contents of the in-memory table onto disk
    pub fn flush_db(&self) -> TResult<()> {
        let data = match self.acquire_write() {
            Some(wlock) => wlock,
            None => return Err("Can no longer flush data; coretable is poisoned".into()),
        };
        diskstore::flush_data(&PERSIST_FILE, &data.coremap)?;
        Ok(())
    }

    #[cfg(test)]
    /// Get a deep copy of the `HashMap`
    ///
    /// **⚠ Do note**: This is super inefficient since it performs an actual
    /// clone of the `HashMap` and doesn't do any `Arc`-business! This function
    /// can be used by test functions and the server, but **use with caution!**
    pub fn get_hashmap_deep_clone(&self) -> HashMap<String, Data> {
        (*self.acquire_read().get_ref()).clone()
    }

    #[cfg(test)]
    /// **⚠⚠⚠ This deletes everything stored in the in-memory table**
    pub fn finish_db(&self) {
        self.acquire_write().unwrap().coremap.clear()
    }
}

impl Drop for CoreDB {
    // This prevents us from killing the database, in the event someone tries
    // to access it
    // If this is indeed the last DB instance, we should tell BGSAVE and the snapshot
    // service to quit
    fn drop(&mut self) {
        // If the strong count is equal to the `expected_strong_count()`
        // then the background services are still running, so don't terminate
        // the database
        if Arc::strong_count(&self.shared) == self.expected_strong_count() {
            // Acquire a lock to prevent anyone from writing something
            let mut coretable = self.shared.table.write();
            coretable.terminate = true;
            // Drop the write lock first to avoid BGSAVE ending up in failing
            // to get a read lock
            drop(coretable);
            // Notify the background tasks to quit
            self.shared.bgsave_task.notify_one();
            self.shared.snapshot_service.notify_one();
        }
    }
}
