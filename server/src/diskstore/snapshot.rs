/*
 * Created on Thu Oct 01 2020
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

//! Tools for creating snapshots

use crate::coredb::CoreDB;
use crate::diskstore;
use chrono::prelude::*;
use libtdb::TResult;
use std::fs;
use std::io::ErrorKind;

const DIR_SNAPSHOT: &'static str = "snapshots";

/// # Snapshot Engine
///
/// This object provides methods to create and delete snapshots. There should be a
/// `snapshot_scheduler` which should hold an instance of this object, on startup.
/// Whenever the duration expires, the caller should call `mksnap()`
pub struct SnapshotEngine {
    /// File names of the snapshots (relative paths)
    snaps: queue::Queue,
    /// An atomic reference to the coretable
    dbref: CoreDB,
}

impl SnapshotEngine {
    /// Create a new `Snapshot` instance
    ///
    /// This also attempts to check if the snapshots directory exists;
    /// If the directory doesn't exist, then it is created
    pub fn new(maxtop: usize, dbref: CoreDB) -> TResult<Self> {
        match fs::create_dir(DIR_SNAPSHOT) {
            Ok(_) => (),
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => (),
                _ => return Err(e.into()),
            },
        }
        Ok(SnapshotEngine {
            snaps: queue::Queue::new(maxtop),
            dbref,
        })
    }
    /// Generate the snapshot name
    fn get_snapname(&self) -> String {
        Utc::now()
            .format("./snapshots/%Y%m%d-%H%M%S.snapshot")
            .to_string()
    }
    /// Create a snapshot
    pub fn mksnap(&mut self) -> TResult<()> {
        let getread = self.dbref.acquire_read();
        let snapname = self.get_snapname();
        diskstore::flush_data(&snapname, &getread.get_ref())?;
        // Release the read lock for the poor clients who are waiting for a write lock
        drop(getread);
        log::info!("Snapshot created");
        if let Some(old_snapshot) = self.snaps.add(snapname) {
            fs::remove_file(old_snapshot)?;
        }
        Ok(())
    }
    /// Delete all snapshots
    pub fn clearall(&mut self) -> TResult<()> {
        for snap in self.snaps.iter() {
            fs::remove_file(snap)?;
        }
        Ok(())
    }
    /// Get the name of snapshots
    pub fn get_snapshots(&self) -> std::slice::Iter<String> {
        self.snaps.iter()
    }
}

#[test]
fn test_snapshot() {
    use std::iter::FromIterator;
    let db = CoreDB::new_empty();
    let mut write = db.acquire_write();
    let _ = write.get_mut_ref().insert(
        String::from("ohhey"),
        crate::coredb::Data::from_string(String::from("heya!")),
    );
    drop(write);
    let mut snapengine = SnapshotEngine::new(4, db.clone()).unwrap();
    snapengine.mksnap().unwrap();
    let current = snapengine.get_snapshots().next().unwrap();
    let read_hmap = diskstore::get_saved(Some(current)).unwrap().unwrap();
    let dbhmap = std::collections::HashMap::from_iter(db.acquire_read().get_ref().iter().map(
        |(key, value)| {
            (
                key.clone(),
                crate::coredb::Data::from_blob(value.get_blob().clone()),
            )
        },
    ));
    assert_eq!(read_hmap, dbhmap);
    snapengine.clearall().unwrap();
}

mod queue {
    //! An extremely simple queue implementation which adds more items to the queue
    //! freely and once the threshold limit is reached, it pops off the oldest element and returns it
    //!
    //! This implementation is specifically built for use with the snapshotting utility
    use std::slice::Iter;
    #[derive(Debug, PartialEq)]
    pub struct Queue {
        queue: Vec<String>,
        maxlen: usize,
    }
    impl Queue {
        pub fn new(maxlen: usize) -> Self {
            Queue {
                queue: Vec::with_capacity(maxlen),
                maxlen,
            }
        }
        /// This returns a `String` only if the queue is full. Otherwise, a `None` is returned most of the time
        pub fn add(&mut self, item: String) -> Option<String> {
            let x = if self.is_overflow() { self.pop() } else { None };
            self.queue.push(item);
            x
        }
        /// Returns an iterator over the slice of strings
        pub fn iter(&self) -> Iter<String> {
            self.queue.iter()
        }
        /// Check if we have reached the maximum queue size limit
        fn is_overflow(&self) -> bool {
            self.queue.len() == self.maxlen
        }
        /// Remove the last item inserted
        fn pop(&mut self) -> Option<String> {
            if self.queue.len() != 0 {
                Some(self.queue.remove(0))
            } else {
                None
            }
        }
    }

    #[test]
    fn test_queue() {
        let mut q = Queue::new(4);
        assert!(q.add(String::from("snap1")).is_none());
        assert!(q.add(String::from("snap2")).is_none());
        assert!(q.add(String::from("snap3")).is_none());
        assert!(q.add(String::from("snap4")).is_none());
        assert_eq!(q.add(String::from("snap5")), Some(String::from("snap1")));
        assert_eq!(q.add(String::from("snap6")), Some(String::from("snap2")));
    }
}
