/*
 * Created on Fri Jul 02 2021
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

#![allow(dead_code)] // TODO(@ohsayan): Remove this onece we're done

use crate::coredb::htable::Data;
use crate::coredb::htable::HTable;
use crate::coredb::SnapshotStatus;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// This is for the future where every node will be allocated a shard
#[derive(Debug)]
pub enum ClusterShardRange {
    SingleNode,
}

impl Default for ClusterShardRange {
    fn default() -> Self {
        Self::SingleNode
    }
}

/// This is for the future for determining the replication strategy
#[derive(Debug)]
pub enum ReplicationStrategy {
    /// Single node, no replica sets
    Default,
}

impl Default for ReplicationStrategy {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug)]
/// The core in-memory table
///
/// This in-memory table that houses all keyspaces and namespaces along with other node
/// properties
pub struct Memstore {
    /// the namespaces
    namespace: HTable<Data, Arc<Namespace>>,
    /// the shard range
    shard_range: ClusterShardRange,
}

// TODO(@ohsayan): Optimize the memory layouts of the UDFs to ensure that sharing is very cheap

#[derive(Debug)]
/// The namespace that houses all the other tables
pub struct Namespace {
    /// the tables
    tables: HTable<Data, Arc<Keyspace>>,
    /// current state of the disk flush status. if this is true, we're safe to
    /// go ahead with writes
    flush_state_healthy: AtomicBool,
    /// the snapshot configuration for this namespace
    snap_config: Option<SnapshotStatus>,
    /// the replication strategy for this namespace
    replication_strategy: ReplicationStrategy,
}

// same 8 byte ptrs; any chance of optimizations?

#[derive(Debug)]
/// The underlying keyspace type. This is the place for the other data models (soon!)
pub enum Keyspace {
    /// a key/value store
    KV(KVStore),
}

#[derive(Debug)]
/// The keyspace that houses atomic references to the actual key value pairs. Again, no one
/// owns anything: just pointers
pub struct KVStore {
    /// the inner table
    table: HTable<Data, Data>,
}
