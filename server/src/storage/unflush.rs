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

//! # Unflush routines
//!
//! Routines for unflushing data

use super::bytemarks;
use crate::corestore::memstore::Keyspace;
use crate::corestore::memstore::Memstore;
use crate::corestore::memstore::ObjectID;
use crate::corestore::table::Table;
use crate::storage::interface::DIR_KSROOT;
use crate::storage::preload::LoadedPartfile;
use crate::storage::Coremap;
use crate::IoResult;
use std::fs;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

type PreloadSet = std::collections::HashSet<ObjectID>;
const PRELOAD_PATH: &str = "data/ks/PRELOAD";

/// Read a given table into a [`Table`] object
///
/// This will take care of volatility and the model_code. Just make sure that you pass the proper
/// keyspace ID and a valid table ID
pub fn read_table(
    ksid: &ObjectID,
    tblid: &ObjectID,
    volatile: bool,
    model_code: u8,
) -> IoResult<Table> {
    let filepath = unsafe { concat_path!(DIR_KSROOT, ksid.as_str(), tblid.as_str()) };
    let data = if volatile {
        // no need to read anything; table is volatile and has no file
        Coremap::new()
    } else {
        // not volatile, so read this in
        let f = fs::read(filepath)?;
        super::de::deserialize_map(f).ok_or_else(|| bad_data!())?
    };
    let tbl = match model_code {
        bytemarks::BYTEMARK_MODEL_KV_BIN_BIN => {
            Table::new_kve_with_data(data, volatile, false, false)
        }
        bytemarks::BYTEMARK_MODEL_KV_BIN_STR => {
            Table::new_kve_with_data(data, volatile, false, true)
        }
        bytemarks::BYTEMARK_MODEL_KV_STR_STR => {
            Table::new_kve_with_data(data, volatile, true, true)
        }
        bytemarks::BYTEMARK_MODEL_KV_STR_BIN => {
            Table::new_kve_with_data(data, volatile, true, false)
        }
        _ => return Err(IoError::from(ErrorKind::Unsupported)),
    };
    Ok(tbl)
}

/// Read an entire keyspace into a Coremap. You'll need to initialize the rest
pub fn read_keyspace(ksid: &ObjectID) -> IoResult<Coremap<ObjectID, Arc<Table>>> {
    let partmap = self::read_partmap(ksid)?;
    let ks: Coremap<ObjectID, Arc<Table>> = Coremap::with_capacity(partmap.len());
    for (tableid, (table_storage_type, model_code)) in partmap.into_iter() {
        if table_storage_type > 1 {
            return Err(bad_data!());
        }
        let is_volatile = table_storage_type == bytemarks::BYTEMARK_STORAGE_VOLATILE;
        let tbl = self::read_table(ksid, &tableid, is_volatile, model_code)?;
        ks.true_if_insert(tableid, Arc::new(tbl));
    }
    Ok(ks)
}

/// Read the `PARTMAP` for a given keyspace
pub fn read_partmap(ksid: &ObjectID) -> IoResult<LoadedPartfile> {
    let filepath = unsafe { concat_path!(DIR_KSROOT, ksid.as_str(), "PARTMAP") };
    super::preload::read_partfile_raw(fs::read(filepath)?)
}

/// Read the `PRELOAD`
pub fn read_preload() -> IoResult<PreloadSet> {
    let read = fs::read(PRELOAD_PATH)?;
    super::preload::read_preload_raw(read)
}

/// Read everything and return a [`Memstore`]
///
/// If this is a new instance an empty store is returned while the directory tree
/// is also created. If this is an already initialized instance then the store
/// is read and returned (and any possible errors that are encountered are returned)
pub fn read_full() -> IoResult<Memstore> {
    if is_new_instance() {
        log::trace!("Detected new instance. Creating data directory");
        /*
        Since the `PRELOAD` file doesn't exist -- this is a new instance
        This means that we need to:
        1. Create the tree (this only creates the directories)
        2. Create the PRELOAD (this is not created by flush_full!)
        3. Do a full flush (this flushes, but doesn't do anything to the PRELOAD!!!)
        */
        // init an empty store
        let store = Memstore::new_default();

        // (1) create the tree
        super::interface::create_tree(&store)?;
        // (2) create the preload
        super::flush::oneshot::flush_preload(&store)?;
        // (3) do a full flush
        super::flush::flush_full(&store)?;
        return Ok(store);
    }
    let preload = self::read_preload()?;
    let ksmap = Coremap::with_capacity(preload.len());
    for ksid in preload {
        let ks = Keyspace::init_with_all_def_strategy(self::read_keyspace(&ksid)?);
        ksmap.upsert(ksid, Arc::new(ks));
    }
    Ok(Memstore::init_with_all(ksmap))
}

/// Check if the data/ks/PRELOAD file exists (if not: we're on a new instance)
pub fn is_new_instance() -> bool {
    let path = Path::new("data/ks/PRELOAD");
    !(path.exists() && path.is_file())
}
