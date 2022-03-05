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
use crate::corestore::memstore::SystemKeyspace;
use crate::corestore::memstore::SYSTEM;
use crate::corestore::table::SystemTable;
use crate::corestore::table::Table;
use crate::storage::v1::de::DeserializeInto;
use crate::storage::v1::flush::Autoflush;
use crate::storage::v1::interface::DIR_KSROOT;
use crate::storage::v1::preload::LoadedPartfile;
use crate::storage::v1::Coremap;
use crate::util::Wrapper;
use crate::IoResult;
use core::mem::transmute;
use std::fs;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

type PreloadSet = std::collections::HashSet<ObjectID>;
const PRELOAD_PATH: &str = "data/ks/PRELOAD";

/// A keyspace that can be restored from disk storage
pub trait UnflushableKeyspace: Sized {
    /// Unflush routine for a keyspace
    fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID) -> IoResult<Self>;
}

impl UnflushableKeyspace for Keyspace {
    fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID) -> IoResult<Self> {
        let ks: Coremap<ObjectID, Arc<Table>> = Coremap::with_capacity(partmap.len());
        for (tableid, (table_storage_type, model_code)) in partmap.into_iter() {
            if table_storage_type > 1 {
                return Err(bad_data!());
            }
            let is_volatile = table_storage_type == bytemarks::BYTEMARK_STORAGE_VOLATILE;
            let tbl = self::read_table::<Table>(ksid, &tableid, is_volatile, model_code)?;
            ks.true_if_insert(tableid, Arc::new(tbl));
        }
        Ok(Keyspace::init_with_all_def_strategy(ks))
    }
}

impl UnflushableKeyspace for SystemKeyspace {
    fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID) -> IoResult<Self> {
        let ks: Coremap<ObjectID, Wrapper<SystemTable>> = Coremap::with_capacity(partmap.len());
        for (tableid, (table_storage_type, model_code)) in partmap.into_iter() {
            if table_storage_type > 1 {
                return Err(bad_data!());
            }
            let is_volatile = table_storage_type == bytemarks::BYTEMARK_STORAGE_VOLATILE;
            let tbl = self::read_table::<SystemTable>(ksid, &tableid, is_volatile, model_code)?;
            ks.true_if_insert(tableid, Wrapper::new(tbl));
        }
        Ok(SystemKeyspace::new(ks))
    }
}

/// Tables that can be restored from disk storage
pub trait UnflushableTable: Sized {
    /// Procedure to restore (deserialize) table from disk storage
    fn unflush_table(filepath: impl AsRef<Path>, model_code: u8, volatile: bool) -> IoResult<Self>;
}

impl UnflushableTable for Table {
    fn unflush_table(filepath: impl AsRef<Path>, model_code: u8, volatile: bool) -> IoResult<Self> {
        let ret = match model_code {
            // pure KVE: [0, 3]
            x if x < 4 => {
                let data = decode(filepath, volatile)?;
                let (k_enc, v_enc) = unsafe {
                    // UNSAFE(@ohsayan): Safe because of the above match. Just a lil bitmagic
                    let key: bool = transmute(model_code >> 1);
                    let value: bool = transmute(((model_code >> 1) + (model_code & 1)) % 2);
                    (key, value)
                };
                Table::new_pure_kve_with_data(data, volatile, k_enc, v_enc)
            }
            // KVExtlistmap: [4, 7]
            x if x < 8 => {
                let data = decode(filepath, volatile)?;
                let (k_enc, v_enc) = unsafe {
                    // UNSAFE(@ohsayan): Safe because of the above match. Just a lil bitmagic
                    let code = model_code - 4;
                    let key: bool = transmute(code >> 1);
                    let value: bool = transmute(code % 2);
                    (key, value)
                };
                Table::new_kve_listmap_with_data(data, volatile, k_enc, v_enc)
            }
            _ => return Err(IoError::from(ErrorKind::Unsupported)),
        };
        Ok(ret)
    }
}

impl UnflushableTable for SystemTable {
    fn unflush_table(filepath: impl AsRef<Path>, model_code: u8, volatile: bool) -> IoResult<Self> {
        match model_code {
            0 => {
                // this is the authmap
                let authmap = decode(filepath, volatile)?;
                Ok(SystemTable::new_auth(Arc::new(authmap)))
            }
            _ => Err(IoError::from(ErrorKind::Unsupported)),
        }
    }
}

#[inline(always)]
fn decode<T: DeserializeInto>(filepath: impl AsRef<Path>, volatile: bool) -> IoResult<T> {
    if volatile {
        Ok(T::new_empty())
    } else {
        let data = fs::read(filepath)?;
        super::de::deserialize_into(&data).ok_or_else(|| bad_data!())
    }
}

/// Read a given table into a [`Table`] object
///
/// This will take care of volatility and the model_code. Just make sure that you pass the proper
/// keyspace ID and a valid table ID
pub fn read_table<T: UnflushableTable>(
    ksid: &ObjectID,
    tblid: &ObjectID,
    volatile: bool,
    model_code: u8,
) -> IoResult<T> {
    let filepath = unsafe { concat_path!(DIR_KSROOT, ksid.as_str(), tblid.as_str()) };
    let tbl = T::unflush_table(filepath, model_code, volatile)?;
    Ok(tbl)
}

/// Read an entire keyspace into a Coremap. You'll need to initialize the rest
pub fn read_keyspace<K: UnflushableKeyspace>(ksid: &ObjectID) -> IoResult<K> {
    let partmap = self::read_partmap(ksid)?;
    K::unflush_keyspace(partmap, ksid)
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
        let target = Autoflush;
        // (1) create the tree
        super::interface::create_tree_fresh(&target, &store)?;
        // (2) create the preload
        super::flush::oneshot::flush_preload(&target, &store)?;
        // (3) do a full flush
        super::flush::flush_full(target, &store)?;
        return Ok(store);
    }
    let mut preload = self::read_preload()?;
    // HACK(@ohsayan): Pop off the preload from the serial read_keyspace list. It will fail
    assert!(preload.remove(&SYSTEM));
    let system_keyspace = self::read_keyspace::<SystemKeyspace>(&SYSTEM)?;
    let ksmap = Coremap::with_capacity(preload.len());
    for ksid in preload {
        let ks = self::read_keyspace::<Keyspace>(&ksid)?;
        ksmap.upsert(ksid, Arc::new(ks));
    }
    Ok(Memstore::init_with_all(ksmap, system_keyspace))
}

/// Check if the data/ks/PRELOAD file exists (if not: we're on a new instance)
pub fn is_new_instance() -> bool {
    let path = Path::new("data/ks/PRELOAD");
    !(path.exists() && path.is_file())
}
