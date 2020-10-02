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
use chrono::prelude::*;
use libtdb::TResult;

/// # Snapshot
///
/// This object provides methods to create and delete snapshots. There should be a
/// `snapshot_scheduler` which should hold an instance of this object, on startup.
/// Whenever the duration expires, the caller should call `mksnap()`
pub struct Snapshot {
    /// File names of the snapshots (relative paths)
    snaps: Vec<String>,
    /// The maximum number of snapshots to be kept
    maxtop: usize,
    /// An atomic reference to the coretable
    dbref: CoreDB,
}

impl Snapshot {
    /// Create a new `Snapshot` instance
    pub fn new(maxtop: usize, dbref: CoreDB) -> Self {
        Snapshot {
            snaps: Vec::with_capacity(maxtop),
            maxtop,
            dbref,
        }
    }
    /// Generate the snapshot name
    fn get_snapname(&self) -> String {
        Utc::now()
            .format("./snapshots/%Y%m%d-%H%M%S.snapshot")
            .to_string()
    }
    pub fn mksnap(&mut self) -> TResult<()> {
        todo!("Snapshotting hasn't been implemented yet!")
    }
}
