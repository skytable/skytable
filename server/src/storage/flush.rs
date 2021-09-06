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

use super::interface;
use crate::corestore::memstore::Keyspace;
use crate::corestore::memstore::Memstore;
use crate::corestore::memstore::ObjectID;
use crate::registry;
use crate::IoResult;

/// Flushes the entire **keyspace + partmap**
pub fn flush_keyspace_full(ksid: &ObjectID, keyspace: &Keyspace) -> IoResult<()> {
    self::oneshot::flush_partmap(ksid, keyspace)?;
    self::oneshot::flush_keyspace(ksid, keyspace)
}

/// Flush the entire **preload + keyspaces + their partmaps**
pub fn flush_full(store: &Memstore) -> IoResult<()> {
    // IMPORTANT: Just untrip and get the status at this exact point in time
    // don't spread it over two atomic accesses because another thread may have updated
    // it in-between. Even if it was untripped, we'll get the expected outcome here: false
    let has_tripped = registry::get_preload_tripswitch().check_and_untrip();
    if has_tripped {
        // re-init the tree as new tables/keyspaces may have been added
        super::interface::create_tree(store)?;
        self::oneshot::flush_preload(store)?;
    }
    for keyspace in store.keyspaces.iter() {
        self::flush_keyspace_full(keyspace.key(), keyspace.value())?;
    }
    Ok(())
}

pub fn snap_flush_keyspace_full(
    snapdir: &str,
    snapid: &str,
    ksid: &ObjectID,
    keyspace: &Keyspace,
) -> IoResult<()> {
    self::oneshot::snap_flush_partmap(snapdir, snapid, ksid, keyspace)?;
    self::oneshot::snap_flush_keyspace(snapdir, snapid, ksid, keyspace)
}

pub fn snap_flush_full(snapdir: &str, snapid: &str, store: &Memstore) -> IoResult<()> {
    super::interface::snap_create_tree(snapdir, snapid, store)?;
    self::oneshot::snap_flush_preload(snapdir, snapid, store)?;
    for keyspace in store.keyspaces.iter() {
        self::snap_flush_keyspace_full(snapdir, snapid, keyspace.key(), keyspace.value())?;
    }
    Ok(())
}

pub mod oneshot {
    //! # Irresponsible flushing
    //!
    //! Every function does **exactly what it says** and nothing more. No partition
    //! files et al are handled
    //!
    use super::*;
    use crate::corestore::table::{DataModel, Table};
    use crate::storage::interface::DIR_KSROOT;
    use std::fs::{self, File};

    const PRELOAD_FILE_PATH_TEMP: &str = "data/ks/PRELOAD_";
    const PRELOAD_FILE_PATH: &str = "data/ks/PRELOAD";

    macro_rules! tbl_path {
        ($ksid:expr, $tableid:expr) => {
            unsafe { concat_str!(DIR_KSROOT, "/", $ksid.as_str(), "/", $tableid.as_str(), "_") }
        };
    }

    macro_rules! snap_tbl_path {
        ($root:expr, $snapid:expr, $ksid:expr, $tableid:expr) => {
            unsafe {
                concat_str!(
                    $root,
                    "/",
                    $snapid,
                    "/",
                    $ksid.as_str(),
                    "/",
                    $tableid.as_str(),
                    "_"
                )
            }
        };
    }

    macro_rules! routine_flushtable {
        ($table:ident, $path:expr) => {
            if $table.is_volatile() {
                // no flushing needed
                Ok(())
            } else {
                // fine, this needs to be flushed
                let mut file = File::create(&$path)?;
                match $table.get_model_ref() {
                    DataModel::KV(kve) => super::interface::serialize_map_into_slow_buffer(
                        &mut file,
                        kve.__get_inner_ref(),
                    )?,
                    _ => {
                        // TODO(@ohsayan): Implement this
                        unimplemented!("Listmap se/de has not been implemented")
                    }
                }
                file.sync_all()?;
                fs::rename(&$path, &$path[..$path.len() - 1])
            }
        };
    }
    /// No `partmap` handling. Just flushes the table to the expected location
    pub fn flush_table(tableid: &ObjectID, ksid: &ObjectID, table: &Table) -> IoResult<()> {
        routine_flushtable!(table, tbl_path!(ksid, tableid))
    }

    /// Same as flush_table, except for it being built specifically for snapshots
    pub fn snap_flush_table(
        snapdir: &str,
        snapid: &str,
        ksid: &ObjectID,
        tableid: &ObjectID,
        table: &Table,
    ) -> IoResult<()> {
        routine_flushtable!(table, snap_tbl_path!(snapdir, snapid, ksid, tableid))
    }

    /// Flushes an entire keyspace to the expected location. No `partmap` or `preload` handling
    pub fn flush_keyspace(ksid: &ObjectID, keyspace: &Keyspace) -> IoResult<()> {
        for table in keyspace.tables.iter() {
            self::flush_table(table.key(), ksid, table.value())?;
        }
        Ok(())
    }

    /// Flushes an entire keyspace to the expected location. No `partmap` or `preload` handling
    pub fn snap_flush_keyspace(
        snapdir: &str,
        snapid: &str,
        ksid: &ObjectID,
        keyspace: &Keyspace,
    ) -> IoResult<()> {
        for table in keyspace.tables.iter() {
            self::snap_flush_table(snapdir, snapid, table.key(), ksid, table.value())?;
        }
        Ok(())
    }

    macro_rules! routine_flushpartmap {
        ($path:expr, $keyspace:ident) => {{
            let mut file = File::create(&$path)?;
            super::interface::serialize_partmap_into_slow_buffer(&mut file, $keyspace)?;
            file.sync_all()?;
            fs::rename(&$path, &$path[..$path.len() - 1])?;
            Ok(())
        }};
    }

    /// Flushes a single partmap
    pub fn flush_partmap(ksid: &ObjectID, keyspace: &Keyspace) -> IoResult<()> {
        let path = unsafe { concat_str!(DIR_KSROOT, "/", ksid.as_str(), "/", "PARTMAP_") };
        routine_flushpartmap!(path, keyspace)
    }

    /// Flushes a single partmap
    pub fn snap_flush_partmap(
        snapdir: &str,
        snapid: &str,
        ksid: &ObjectID,
        keyspace: &Keyspace,
    ) -> IoResult<()> {
        let path =
            unsafe { concat_str!(snapdir, "/", snapid, "/", ksid.as_str(), "/", "PARTMAP_") };
        routine_flushpartmap!(path, keyspace)
    }

    macro_rules! routine_flushpreload {
        ($store:expr, $preloadtmp:expr, $preloadfinal:expr) => {{
            let mut file = File::create(&$preloadtmp)?;
            super::interface::serialize_preload_into_slow_buffer(&mut file, $store)?;
            file.sync_all()?;
            fs::rename(&$preloadtmp, &$preloadfinal)?;
            Ok(())
        }};
    }

    // Flush the `PRELOAD`
    pub fn flush_preload(store: &Memstore) -> IoResult<()> {
        routine_flushpreload!(store, PRELOAD_FILE_PATH_TEMP, PRELOAD_FILE_PATH)
    }

    /// Same as flush_preload, but for snapshots
    pub fn snap_flush_preload(snapdir: &str, snapid: &str, store: &Memstore) -> IoResult<()> {
        let preload_tmp = concat_str!(snapdir, "/", snapid, "/", "PRELOAD_");
        let preload = &preload_tmp[..preload_tmp.len() - 1];
        routine_flushpreload!(store, preload_tmp, preload)
    }
}
