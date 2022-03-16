/*
 * Created on Sat Jul 17 2021
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

//! # Flush routines
//!
//! This module contains multiple flush routines: at the memstore level, the keyspace level and
//! the table level

use super::{bytemarks, interface};
use crate::corestore::memstore::SYSTEM;
use crate::corestore::{
    map::iter::BorrowedIter,
    memstore::{Keyspace, Memstore, ObjectID, SystemKeyspace},
    table::{DataModel, SystemDataModel, SystemTable, Table},
};
use crate::registry;
use crate::util::Wrapper;
use crate::IoResult;
use core::ops::Deref;
use std::io::Write;
use std::sync::Arc;

pub trait StorageTarget {
    /// This storage target needs a reinit of the tree despite no preload trip.
    /// Exempli gratia: rsnap, snap
    const NEEDS_TREE_INIT: bool;
    /// The root for this storage target. **Must not be separator terminated!**
    fn root(&self) -> String;
    /// Returns the path to the `PRELOAD_` **temporary file** ($ROOT/PRELOAD)
    fn preload_target(&self) -> String {
        let mut p = self.root();
        p.push('/');
        p.push_str("PRELOAD_");
        p
    }
    /// Returns the path to the keyspace folder. ($ROOT/{keyspace})
    fn keyspace_target(&self, keyspace: &str) -> String {
        let mut p = self.root();
        p.push('/');
        p.push_str(keyspace);
        p
    }
    /// Returns the path to a `PARTMAP_` for the given keyspace. **temporary file**
    /// ($ROOT/{keyspace}/PARTMAP)
    fn partmap_target(&self, keyspace: &str) -> String {
        let mut p = self.keyspace_target(keyspace);
        p.push('/');
        p.push_str("PARTMAP_");
        p
    }
    /// Returns the path to the table file. **temporary file** ($ROOT/{keyspace}/{table}_)
    fn table_target(&self, keyspace: &str, table: &str) -> String {
        let mut p = self.keyspace_target(keyspace);
        p.push('/');
        p.push_str(table);
        p.push('_');
        p
    }
}

/// The autoflush target (BGSAVE target)
pub struct Autoflush;

impl StorageTarget for Autoflush {
    const NEEDS_TREE_INIT: bool = false;
    fn root(&self) -> String {
        String::from(interface::DIR_KSROOT)
    }
}

/// A remote snapshot storage target
pub struct RemoteSnapshot<'a> {
    name: &'a str,
}

impl<'a> RemoteSnapshot<'a> {
    pub fn new(name: &'a str) -> Self {
        Self { name }
    }
}

impl<'a> StorageTarget for RemoteSnapshot<'a> {
    const NEEDS_TREE_INIT: bool = true;
    fn root(&self) -> String {
        let mut p = String::from(interface::DIR_RSNAPROOT);
        p.push('/');
        p.push_str(&self.name);
        p
    }
}

/// A snapshot storage target
pub struct LocalSnapshot {
    name: String,
}

impl LocalSnapshot {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl StorageTarget for LocalSnapshot {
    const NEEDS_TREE_INIT: bool = true;
    fn root(&self) -> String {
        let mut p = String::from(interface::DIR_SNAPROOT);
        p.push('/');
        p.push_str(&self.name);
        p
    }
}

/// A keyspace that can be flushed
pub trait FlushableKeyspace<T: FlushableTable, U: Deref<Target = T>> {
    /// The number of tables in this keyspace
    fn table_count(&self) -> usize;
    /// An iterator to the tables in this keyspace.
    /// All of them implement [`FlushableTable`]
    fn get_iter(&self) -> BorrowedIter<'_, ObjectID, U>;
}

impl FlushableKeyspace<Table, Arc<Table>> for Keyspace {
    fn table_count(&self) -> usize {
        self.tables.len()
    }
    fn get_iter(&self) -> BorrowedIter<'_, ObjectID, Arc<Table>> {
        self.tables.iter()
    }
}

impl FlushableKeyspace<SystemTable, Wrapper<SystemTable>> for SystemKeyspace {
    fn table_count(&self) -> usize {
        self.tables.len()
    }
    fn get_iter(&self) -> BorrowedIter<'_, ObjectID, Wrapper<SystemTable>> {
        self.tables.iter()
    }
}

pub trait FlushableTable {
    /// Table is volatile
    fn is_volatile(&self) -> bool;
    /// Returns the storage code bytemark
    fn storage_code(&self) -> u8;
    /// Serializes the table and writes it to the provided buffer
    fn write_table_to<W: Write>(&self, writer: &mut W) -> IoResult<()>;
    /// Returns the model code bytemark
    fn model_code(&self) -> u8;
}

impl FlushableTable for Table {
    fn is_volatile(&self) -> bool {
        self.is_volatile()
    }
    fn write_table_to<W: Write>(&self, writer: &mut W) -> IoResult<()> {
        match self.get_model_ref() {
            DataModel::KV(ref kve) => super::se::raw_serialize_map(kve.get_inner_ref(), writer),
            DataModel::KVExtListmap(ref kvl) => {
                super::se::raw_serialize_list_map(kvl.get_inner_ref(), writer)
            }
        }
    }
    fn storage_code(&self) -> u8 {
        self.storage_type()
    }
    fn model_code(&self) -> u8 {
        self.get_model_code()
    }
}

impl FlushableTable for SystemTable {
    fn is_volatile(&self) -> bool {
        false
    }
    fn write_table_to<W: Write>(&self, writer: &mut W) -> IoResult<()> {
        match self.get_model_ref() {
            SystemDataModel::Auth(amap) => super::se::raw_serialize_map(amap.as_ref(), writer),
        }
    }
    fn storage_code(&self) -> u8 {
        0
    }
    fn model_code(&self) -> u8 {
        match self.get_model_ref() {
            SystemDataModel::Auth(_) => bytemarks::SYSTEM_TABLE_AUTH,
        }
    }
}

/// Flush the entire **preload + keyspaces + their partmaps**
pub fn flush_full<T: StorageTarget>(target: T, store: &Memstore) -> IoResult<()> {
    // IMPORTANT: Just untrip and get the status at this exact point in time
    // don't spread it over two atomic accesses because another thread may have updated
    // it in-between. Even if it was untripped, we'll get the expected outcome here: false
    let has_tripped = registry::get_preload_tripswitch().check_and_untrip();
    if has_tripped || T::NEEDS_TREE_INIT {
        // re-init the tree as new tables/keyspaces may have been added
        super::interface::create_tree(&target, store)?;
        self::oneshot::flush_preload(&target, store)?;
    }
    // flush userspace keyspaces
    for keyspace in store.keyspaces.iter() {
        self::flush_keyspace_full(&target, keyspace.key(), keyspace.value().as_ref())?;
    }
    // flush system tables
    // HACK(@ohsayan): DO NOT REORDER THIS. THE above loop will flush a PARTMAP once. But
    // this has to be done again! The system keyspace in the above loop is a dummy one
    // because it is located in a different field. So, we need to flush the actual list
    // of tables
    self::oneshot::flush_partmap(&target, &SYSTEM, &store.system)?;
    Ok(())
}

/// Flushes the entire **keyspace + partmap**
pub fn flush_keyspace_full<T, U, Tbl, K>(target: &T, ksid: &ObjectID, keyspace: &K) -> IoResult<()>
where
    T: StorageTarget,
    U: Deref<Target = Tbl>,
    Tbl: FlushableTable,
    K: FlushableKeyspace<Tbl, U>,
{
    self::oneshot::flush_partmap(target, ksid, keyspace)?;
    self::oneshot::flush_keyspace(target, ksid, keyspace)
}

pub mod oneshot {
    //! # Irresponsible flushing
    //!
    //! Every function does **exactly what it says** and nothing more. No partition
    //! files et al are handled
    //!
    use super::*;
    use std::fs::{self, File};

    /// No `partmap` handling. Just flushes the table to the expected location
    pub fn flush_table<T: StorageTarget, U: FlushableTable>(
        target: &T,
        tableid: &ObjectID,
        ksid: &ObjectID,
        table: &U,
    ) -> IoResult<()> {
        if table.is_volatile() {
            // no flushing needed
            Ok(())
        } else {
            let path = unsafe { target.table_target(ksid.as_str(), tableid.as_str()) };
            // fine, this needs to be flushed
            let mut file = File::create(&path)?;
            super::interface::serialize_into_slow_buffer(&mut file, table)?;
            file.sync_all()?;
            fs::rename(&path, &path[..path.len() - 1])
        }
    }

    /// Flushes an entire keyspace to the expected location. No `partmap` or `preload` handling
    pub fn flush_keyspace<T, U, Tbl, K>(target: &T, ksid: &ObjectID, keyspace: &K) -> IoResult<()>
    where
        T: StorageTarget,
        U: Deref<Target = Tbl>,
        Tbl: FlushableTable,
        K: FlushableKeyspace<Tbl, U>,
    {
        for table in keyspace.get_iter() {
            self::flush_table(target, table.key(), ksid, table.value().deref())?;
        }
        Ok(())
    }

    /// Flushes a single partmap
    pub fn flush_partmap<T, U, Tbl, K>(target: &T, ksid: &ObjectID, keyspace: &K) -> IoResult<()>
    where
        T: StorageTarget,
        U: Deref<Target = Tbl>,
        Tbl: FlushableTable,
        K: FlushableKeyspace<Tbl, U>,
    {
        let path = unsafe { target.partmap_target(ksid.as_str()) };
        let mut file = File::create(&path)?;
        super::interface::serialize_partmap_into_slow_buffer(&mut file, keyspace)?;
        file.sync_all()?;
        fs::rename(&path, &path[..path.len() - 1])?;
        Ok(())
    }

    // Flush the `PRELOAD`
    pub fn flush_preload<T: StorageTarget>(target: &T, store: &Memstore) -> IoResult<()> {
        let preloadtmp = target.preload_target();
        let mut file = File::create(&preloadtmp)?;
        super::interface::serialize_preload_into_slow_buffer(&mut file, store)?;
        file.sync_all()?;
        fs::rename(&preloadtmp, &preloadtmp[..preloadtmp.len() - 1])?;
        Ok(())
    }
}
