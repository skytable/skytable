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

use crate::diskstore;
use crate::protocol::Connection;
use crate::protocol::Query;
use crate::queryengine;
use bytes::Bytes;
use libtdb::util::terminal;
use libtdb::TResult;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::sync::Notify;
use tokio::time::Instant;

/// This is a thread-safe database handle, which on cloning simply
/// gives another atomic reference to the `shared` which is a `Shared` object
#[derive(Debug, Clone)]
pub struct CoreDB {
    /// The shared object, which contains a `Shared` object wrapped in a thread-safe
    /// RC
    pub shared: Arc<Shared>,
}

/// A shared _state_
#[derive(Debug)]
pub struct Shared {
    /// This is used by the `BGSAVE` task. `Notify` is used to signal a task
    /// to wake up
    pub bgsave_task: Notify,
    /// A `Coretable` wrapped in a R/W lock
    pub table: RwLock<Coretable>,
}

impl Shared {
    /// This task performs a `sync`hronous background save operation and returns
    /// the duration for which the background thread for BGSAVE must sleep for, before
    /// it calls this function again. If the server has received a termination signal
    /// then we just return `None`
    pub fn run_bgsave_and_get_next_point(&self) -> Option<Instant> {
        let state = self.table.read();
        if state.terminate {
            return None;
        }
        // Kick in BGSAVE
        match diskstore::flush_data(&self.table.read().get_ref()) {
            Ok(_) => log::info!("BGSAVE completed successfully"),
            Err(e) => log::error!("BGSAVE failed with error: '{}'", e),
        }
        Some(Instant::now() + Duration::from_secs(120))
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
#[derive(Debug)]
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
}

impl CoreDB {
    #[cfg(debug_assertions)]
    /// Flush the coretable entries when in debug mode
    pub fn print_debug_table(&self) {
        if self.acquire_read().coremap.len() == 0 {
            println!("In-memory table is empty");
        } else {
            println!("{:#?}", self.acquire_read());
        }
    }

    /// Execute a query that has already been validated by `Connection::read_query`
    pub async fn execute_query(&self, query: Query, con: &mut Connection) -> TResult<()> {
        match query {
            Query::Simple(q) => queryengine::execute_simple(&self, con, q).await?,
            // TODO(@ohsayan): Pipeline commands haven't been implemented yet
            Query::Pipelined(_) => unimplemented!(),
        }
        // Once we're done executing, flush the stream
        con.flush_stream().await
    }

    /// Create a new `CoreDB` instance
    ///
    /// This also checks if a local backup of previously saved data is available.
    /// If it is - it restores the data. Otherwise it creates a new in-memory table
    pub fn new() -> TResult<Self> {
        let coretable = diskstore::get_saved()?;
        let db = if let Some(coretable) = coretable {
            CoreDB {
                shared: Arc::new(Shared {
                    bgsave_task: Notify::new(),
                    table: RwLock::new(Coretable {
                        coremap: coretable,
                        terminate: false,
                    }),
                }),
            }
        } else {
            CoreDB {
                shared: Arc::new(Shared {
                    bgsave_task: Notify::new(),
                    table: RwLock::new(Coretable {
                        coremap: HashMap::new(),
                        terminate: false,
                    }),
                }),
            }
        };
        // Spawn the background save task in a separate task
        tokio::spawn(diskstore::bgsave_scheduler(db.clone()));
        Ok(db)
    }
    /// Acquire a write lock
    pub fn acquire_write(&self) -> RwLockWriteGuard<'_, Coretable> {
        self.shared.table.write()
    }
    /// Acquire a read lock
    pub fn acquire_read(&self) -> RwLockReadGuard<'_, Coretable> {
        self.shared.table.read()
    }
    /// Flush the contents of the in-memory table onto disk
    pub fn flush_db(&self) -> TResult<()> {
        let data = &self.acquire_write();
        diskstore::flush_data(&data.coremap)?;
        Ok(())
    }

    /// **⚠⚠⚠ This deletes everything stored in the in-memory table**
    pub fn finish_db(self, areyousure: bool, areyouverysure: bool, areyousupersure: bool) {
        if areyousure && areyouverysure && areyousupersure {
            self.acquire_write().coremap.clear()
        }
    }
}

impl Drop for CoreDB {
    // This prevents us from killing the database, in the event someone tries
    // to access it
    // If this is indeed the last DB instance, we should tell BGSAVE to terminate
    fn drop(&mut self) {
        // The strong count should be
        if Arc::strong_count(&self.shared) == 2 {
            // Acquire a lock to prevent anyone from writing something
            let mut coretable = self.shared.table.write();
            coretable.terminate = true;
            drop(coretable);
            // Drop the write lock first to avoid BGSAVE ending up in failing
            // to get a read lock
            self.shared.bgsave_task.notify();
        }
    }
}
