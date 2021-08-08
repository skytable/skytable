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

#![allow(dead_code)] // TODO(@ohsayan): Remove this lint once we're done

use crate::corestore::lock::QuickLock;
use crossbeam_queue::SegQueue;

pub type VoidLock = QuickLock<()>;

/// The snapshot engine
pub struct SnapshotEngine {
    /// the local snapshot lock
    local_lock: VoidLock,
    /// the remote snapshot lock
    remote_lock: VoidLock,
    /// the local snapshot queue
    local_queue: SegQueue<String>,
    /// the remote snapshot queue
    remote_queue: SegQueue<String>,
}

impl SnapshotEngine {
    /// Returns a fresh, uninitialized snapshot engine instance
    pub const fn new() -> Self {
        Self {
            local_lock: VoidLock::new_void(),
            remote_lock: VoidLock::new_void(),
            local_queue: SegQueue::new(),
            remote_queue: SegQueue::new(),
        }
    }
}
