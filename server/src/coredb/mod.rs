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

use crate::config::BGSave;
use crate::config::SnapshotConfig;
use crate::config::SnapshotPref;
use crate::coredb::htable::HTable;
use crate::dbnet::connection::prelude::*;
use crate::diskstore;
use crate::protocol::Query;
use crate::queryengine;
use bytes::Bytes;
use diskstore::flock;
use diskstore::PERSIST_FILE;
use libsky::TResult;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::sync::Arc;
use tokio;
pub mod htable;
use tokio::sync::Notify;

#[macro_export]
macro_rules! flush_db {
    ($db:expr) => {
        crate::coredb::CoreDB::flush_db(&$db, None)
    };
    ($db:expr, $file:expr) => {
        crate::coredb::CoreDB::flush_db(&$db, Some(&mut $file))
    };
}

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
    pub fn run_bgsave(&self, file: &mut flock::FileLock) -> bool {
        log::trace!("BGSAVE started");
        let rlock = self.table.read();
        if rlock.terminate {
            drop(rlock);
            return false;
        }
        // Kick in BGSAVE
        match diskstore::flush_data(file, rlock.get_ref()) {
            Ok(_) => {
                drop(rlock);
                {
                    // just scope it to ensure dropping of the lock
                    // since this bgsave succeeded, mark the service as !poisoned, enabling it to recover
                    self.table.write().poisoned = false;
                }
                log::info!("BGSAVE completed successfully");
                return true;
            }
            Err(e) => {
                // IMPORTANT! Drop the read lock first fella!
                drop(rlock);
                // IMPORTANT! POISON THE DATABASE, NO MORE WRITES FOR YOU!
                {
                    // scope to ensure dropping of the lock
                    self.table.write().poisoned = true;
                }
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

/// The `Coretable` holds all the key-value pairs in a `HTable`
/// and the `terminate` field, which when set to true will cause all other
/// background tasks to terminate
#[derive(Debug)]
pub struct Coretable {
    /// The core table contain key-value pairs
    coremap: HTable<String, Data>,
    /// The termination signal flag
    pub terminate: bool,
    /// Whether the database is poisoned or not
    ///
    /// If the database is poisoned -> the database can no longer accept writes
    /// but can only accept reads
    pub poisoned: bool,
}

impl Coretable {
    /// Get a reference to the inner `HTable`
    pub const fn get_ref<'a>(&'a self) -> &'a HTable<String, Data> {
        &self.coremap
    }
    /// Get a **mutable** reference to the inner `HTable`
    pub fn get_mut_ref<'a>(&'a mut self) -> &'a mut HTable<String, Data> {
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

    /// Returns the expected `Arc::strong_count` for the `CoreDB` object when it is about to be dropped
    ///
    /// This is the deal:
    /// 1. Runtime starts
    /// 2. [`dbnet::run`] creates a coredb, so strong count is 1
    /// 4. [`coredb::CoreDB::new()`] spawns the background task who's count we have here, so strong count is 2/3
    /// 3. [`dbnet::run`] distributes clones to listeners, so strong count is either 4/5
    /// 4. Listeners further distributed clones per-stream, so strong count can potentially cross 50000 (semaphore)
    /// 5. Now all workers terminate and we should be back to a strong count of 2/3
    ///
    /// Step 5 is where CoreDB should notify the background services to stop. So, at step 5 we have this ingenious
    /// listener who should tell the services to terminate. At this point, our listener itself holds an atomic
    /// reference, the [`dbnet::run`] holds one and the background tasks hold some. So there should be:
    /// `background_tasks + 2` number of atomic references when we should signal a quit; in other words, this
    /// the last active listener who is about to bring down the server xD
    pub const fn expected_strong_count_at_drop(&self) -> usize {
        self.background_tasks + 2
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
    pub fn new(
        bgsave: BGSave,
        snapshot_cfg: SnapshotConfig,
        restore_file: Option<String>,
    ) -> TResult<(Self, Option<flock::FileLock>)> {
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
        let snapcfg = snap_count
            .map(|max| Arc::new(Some(SnapshotStatus::new(max))))
            .unwrap_or(Arc::new(None));
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
        // Spawn the snapshot service in a separate task
        tokio::spawn(diskstore::snapshot::snapshot_service(
            db.clone(),
            snapshot_cfg,
        ));
        let lock = flock::FileLock::lock(&*PERSIST_FILE)
            .map_err(|e| format!("Failed to acquire lock on data file with error '{}'", e))?;
        if bgsave.is_disabled() {
            Ok((db, Some(lock)))
        } else {
            // Spawn the BGSAVE service in a separate task
            tokio::spawn(diskstore::bgsave_scheduler(db.clone(), bgsave, lock));
            Ok((db, None))
        }
    }
    /// Create an empty in-memory table
    pub fn new_empty(background_tasks: usize, snapcfg: Arc<Option<SnapshotStatus>>) -> Self {
        CoreDB {
            shared: Arc::new(Shared {
                bgsave_task: Notify::new(),
                table: RwLock::new(Coretable {
                    coremap: HTable::<String, Data>::new(),
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
    pub fn flush_db(&self, file: Option<&mut flock::FileLock>) -> TResult<()> {
        let data = match self.acquire_write() {
            Some(wlock) => wlock,
            None => return Err("Can no longer flush data; coretable is poisoned".into()),
        };
        if let Some(mut file) = file {
            diskstore::flush_data(&mut file, &data.coremap)?;
        } else {
            diskstore::write_to_disk(&PERSIST_FILE, &data.coremap)?;
        }
        Ok(())
    }

    #[cfg(test)]
    /// Get a deep copy of the `HTable`
    ///
    /// **⚠ Do note**: This is super inefficient since it performs an actual
    /// clone of the `HTable` and doesn't do any `Arc`-business! This function
    /// can be used by test functions and the server, but **use with caution!**
    pub fn get_htable_deep_clone(&self) -> HTable<String, Data> {
        (*self.acquire_read().get_ref()).clone()
    }
}

impl Drop for CoreDB {
    fn drop(&mut self) {
        // If the strong count is equal to the `expected_strong_count_at_drop()`
        // then the background services are still running, so tell them to terminate
        if Arc::strong_count(&self.shared) == self.expected_strong_count_at_drop() {
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
