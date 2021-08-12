/*
 * Created on Sun Aug 08 2021
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

use self::queue::Queue;
use super::interface::DIR_SNAPROOT;
use crate::corestore::iarray::IArray;
use crate::corestore::lazy::Lazy;
use crate::corestore::lock::QuickLock;
use crate::storage::interface::DIR_RSNAPROOT;
use crate::Memstore;
use bytes::Bytes;
use chrono::prelude::Utc;
use core::fmt;
use core::str;
use regex::Regex;
use std::fs;
use std::io::Error as IoError;
use std::sync::Arc;

type QStore = IArray<[String; 64]>;
type SnapshotResult<T> = Result<T, SnapshotEngineError>;

/// Matches any string which is in the following format:
/// ```text
/// YYYYMMDD-HHMMSS
/// ```
pub static SNAP_MATCH: Lazy<Regex, fn() -> Regex> = Lazy::new(|| {
    Regex::new("^\\d{4}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])(-)(?:(?:([01]?\\d|2[0-3]))?([0-5]?\\d))?([0-5]?\\d)$").unwrap()
});

pub enum SnapshotEngineError {
    Io(IoError),
    Engine(&'static str),
}

impl From<IoError> for SnapshotEngineError {
    fn from(e: IoError) -> SnapshotEngineError {
        SnapshotEngineError::Io(e)
    }
}

impl From<&'static str> for SnapshotEngineError {
    fn from(e: &'static str) -> SnapshotEngineError {
        SnapshotEngineError::Engine(e)
    }
}

impl fmt::Display for SnapshotEngineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        match self {
            Self::Engine(estr) => {
                formatter.write_str("Snapshot engine error")?;
                formatter.write_str(estr)?;
            }
            Self::Io(e) => {
                formatter.write_str("Snapshot engine IOError:")?;
                formatter.write_str(&e.to_string())?;
            }
        }
        Ok(())
    }
}

/// The snapshot engine
#[derive(Debug)]
pub struct SnapshotEngine {
    local_enabled: bool,
    /// the local snapshot queue
    local_queue: QuickLock<Queue>,
    /// the remote snapshot lock
    remote_lock: QuickLock<()>,
}

macro_rules! parse_dir {
    ($queue:expr, $dirname:expr) => {
        let dir = fs::read_dir($dirname)?;
        for entry in dir {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let fname = entry.file_name();
                let name = fname.to_string_lossy();
                if !SNAP_MATCH.is_match(&name) {
                    return Err("unknown file in snapshot directory".into());
                }
                $queue.lock().push(name.to_string());
            } else {
                return Err("unrecognized file in snapshot directory".into());
            }
        }
    };
}

impl SnapshotEngine {
    /// Returns a fresh, uninitialized snapshot engine instance
    pub const fn new(maxlen: usize) -> Self {
        Self {
            local_enabled: true,
            local_queue: QuickLock::new(Queue::new(maxlen, maxlen == 0)),
            remote_lock: QuickLock::new(()),
        }
    }
    pub const fn new_disabled() -> Self {
        Self {
            local_enabled: false,
            local_queue: QuickLock::new(Queue::new(0, true)),
            remote_lock: QuickLock::new(()),
        }
    }
    pub fn parse_dir(&self) -> SnapshotResult<()> {
        parse_dir!(self.local_queue, DIR_SNAPROOT);
        println!("Queue: {:?}", self.local_queue);
        Ok(())
    }
    /// Generate the snapshot name
    fn get_snapname(&self) -> String {
        Utc::now().format("%Y%m%d-%H%M%S").to_string()
    }
    fn _mksnap_blocking_section(store: &Memstore, name: &str) -> SnapshotResult<()> {
        super::flush::snap_flush_full(DIR_SNAPROOT, name, store)?;
        Ok(())
    }
    fn _rmksnap_blocking_section(store: &Memstore, name: &str) -> SnapshotResult<()> {
        super::flush::snap_flush_full(DIR_RSNAPROOT, name, store)?;
        Ok(())
    }
    /// Spawns a blocking task on a threadpool for blocking tasks. Returns either of:
    /// - `0` => Okay (returned **even if old snap deletion failed**)
    /// - `1` => Error
    /// - `2` => Disabled
    /// - `3` => Busy
    pub async fn mksnap(&self, store: Arc<Memstore>) -> u8 {
        if self.local_enabled {
            // try to lock the local queue
            let mut queue = match self.local_queue.try_lock() {
                Some(lck) => lck,
                None => return 3,
            };
            let name = self.get_snapname();
            let nameclone = name.clone();
            let todel = queue.add_new(name);
            let snap_create_result = tokio::task::spawn_blocking(move || {
                Self::_mksnap_blocking_section(&store, &nameclone)
            })
            .await
            .expect("mksnap thread panicked");

            // First create the new snap
            match snap_create_result {
                Ok(_) => {
                    log::info!("Successfully created snapshot");
                }
                Err(e) => {
                    log::info!("Failed to create snapshot with error: {}", e);
                    // so it failed, remove it from queue
                    let _ = queue.pop_last().unwrap();
                    return 1;
                }
            }

            // Now delete the older snap (if any)
            if let Some(snap) = todel {
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = fs::remove_dir_all(concat_path!(DIR_SNAPROOT, snap)) {
                        log::warn!("Failed to remove older snapshot (ignored): {}", e);
                    } else {
                        log::info!("Successfully removed older snapshot");
                    }
                })
                .await
                .expect("mksnap thread panicked");
            }
            0
        } else {
            2
        }
    }
    /// Spawns a blocking task to create a remote snapshot. Returns either of:
    /// - `0` => Okay
    /// - `1` => Error
    /// - `3` => Busy
    /// (consistent with mksnap)
    pub async fn mkrsnap(&self, name: Bytes, store: Arc<Memstore>) -> u8 {
        let _lck = match self.remote_lock.try_lock() {
            Some(q) => q,
            None => return 3,
        };
        tokio::task::spawn_blocking(move || {
            let name_str = unsafe {
                // SAFETY: We have already checked if name is UTF-8
                str::from_utf8_unchecked(&name)
            };
            if let Err(e) = Self::_rmksnap_blocking_section(&store, name_str) {
                log::error!("Remote snapshot failed with: {}", e);
                1
            } else {
                log::info!("Remote snapshot succeeded");
                0
            }
        })
        .await
        .expect("rmksnap thread panicked")
    }
}

mod queue {
    //! An extremely simple queue implementation which adds more items to the queue
    //! freely and once the threshold limit is reached, it pops off the oldest element and returns it
    //!
    //! This implementation is specifically built for use with the snapshotting utility
    use super::QStore;
    use crate::corestore::iarray;
    #[derive(Debug, PartialEq)]
    pub struct Queue {
        queue: QStore,
        maxlen: usize,
        dontpop: bool,
    }

    impl Queue {
        pub const fn new(maxlen: usize, dontpop: bool) -> Self {
            Queue {
                queue: iarray::new_const_iarray(),
                maxlen,
                dontpop,
            }
        }
        pub fn push(&mut self, item: String) {
            self.queue.push(item)
        }
        /// This returns a `String` only if the queue is full. Otherwise, a `None` is returned most of the time
        pub fn add_new(&mut self, item: String) -> Option<String> {
            if self.dontpop {
                // We don't need to pop anything since the user
                // wants to keep all the items in the queue
                self.queue.push(item);
                None
            } else {
                // The user wants to keep a maximum of `maxtop` items
                // so we will check if the current queue is full
                // if it is full, then the `maxtop` limit has been reached
                // so we will remove the oldest item and then push the
                // new item onto the queue
                let x = if self.is_overflow() { self.pop() } else { None };
                self.queue.push(item);
                x
            }
        }
        /// Check if we have reached the maximum queue size limit
        fn is_overflow(&self) -> bool {
            self.queue.len() == self.maxlen
        }
        /// Remove the last item inserted
        fn pop(&mut self) -> Option<String> {
            if self.queue.is_empty() {
                None
            } else {
                Some(unsafe {
                    // SAFETY: We have already checked if the queue is empty or not
                    self.queue.remove(0)
                })
            }
        }
        pub fn pop_last(&mut self) -> Option<String> {
            self.queue.pop()
        }
    }

    #[test]
    fn test_queue() {
        let mut q = Queue::new(4, false);
        assert!(q.add_new(String::from("snap1")).is_none());
        assert!(q.add_new(String::from("snap2")).is_none());
        assert!(q.add_new(String::from("snap3")).is_none());
        assert!(q.add_new(String::from("snap4")).is_none());
        assert_eq!(
            q.add_new(String::from("snap5")),
            Some(String::from("snap1"))
        );
        assert_eq!(
            q.add_new(String::from("snap6")),
            Some(String::from("snap2"))
        );
    }

    #[test]
    fn test_queue_dontpop() {
        // This means that items can only be added or all of them can be deleted
        let mut q = Queue::new(4, true);
        assert!(q.add_new(String::from("snap1")).is_none());
        assert!(q.add_new(String::from("snap2")).is_none());
        assert!(q.add_new(String::from("snap3")).is_none());
        assert!(q.add_new(String::from("snap4")).is_none());
        assert!(q.add_new(String::from("snap5")).is_none());
        assert!(q.add_new(String::from("snap6")).is_none());
    }
}
