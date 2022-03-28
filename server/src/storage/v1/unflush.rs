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
use crate::{
    corestore::{
        memstore::{Keyspace, Memstore, ObjectID, SystemKeyspace, SYSTEM},
        table::{SystemTable, Table},
    },
    storage::v1::{
        de::DeserializeInto,
        error::{ErrorContext, StorageEngineError, StorageEngineResult},
        flush::Autoflush,
        interface::DIR_KSROOT,
        preload::LoadedPartfile,
        Coremap,
    },
    util::Wrapper,
};
use core::mem::transmute;
use std::{fs, io::ErrorKind, path::Path, sync::Arc};

type PreloadSet = std::collections::HashSet<ObjectID>;
const PRELOAD_PATH: &str = "data/ks/PRELOAD";

/// A keyspace that can be restored from disk storage
pub trait UnflushableKeyspace: Sized {
    /// Unflush routine for a keyspace
    fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID) -> StorageEngineResult<Self>;
}

impl UnflushableKeyspace for Keyspace {
    fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID) -> StorageEngineResult<Self> {
        let ks: Coremap<ObjectID, Arc<Table>> = Coremap::with_capacity(partmap.len());
        for (tableid, (table_storage_type, model_code)) in partmap.into_iter() {
            if table_storage_type > 1 {
                return Err(StorageEngineError::bad_metadata_in_table(ksid, &tableid));
            }
            let is_volatile = table_storage_type == bytemarks::BYTEMARK_STORAGE_VOLATILE;
            let tbl = self::read_table::<Table>(ksid, &tableid, is_volatile, model_code)?;
            ks.true_if_insert(tableid, Arc::new(tbl));
        }
        Ok(Keyspace::init_with_all_def_strategy(ks))
    }
}

impl UnflushableKeyspace for SystemKeyspace {
    fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID) -> StorageEngineResult<Self> {
        let ks: Coremap<ObjectID, Wrapper<SystemTable>> = Coremap::with_capacity(partmap.len());
        for (tableid, (table_storage_type, model_code)) in partmap.into_iter() {
            if table_storage_type > 1 {
                return Err(StorageEngineError::bad_metadata_in_table(ksid, &tableid));
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
    fn unflush_table(
        filepath: impl AsRef<Path>,
        model_code: u8,
        volatile: bool,
    ) -> StorageEngineResult<Self>;
}

#[allow(clippy::transmute_int_to_bool)]
impl UnflushableTable for Table {
    fn unflush_table(
        filepath: impl AsRef<Path>,
        model_code: u8,
        volatile: bool,
    ) -> StorageEngineResult<Self> {
        let ret = match model_code {
            // pure KVEBlob: [0, 3]
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
            _ => {
                return Err(StorageEngineError::BadMetadata(
                    filepath.as_ref().to_string_lossy().to_string(),
                ))
            }
        };
        Ok(ret)
    }
}

impl UnflushableTable for SystemTable {
    fn unflush_table(
        filepath: impl AsRef<Path>,
        model_code: u8,
        volatile: bool,
    ) -> StorageEngineResult<Self> {
        match model_code {
            0 => {
                // this is the authmap
                let authmap = decode(filepath, volatile)?;
                Ok(SystemTable::new_auth(Arc::new(authmap)))
            }
            _ => Err(StorageEngineError::BadMetadata(
                filepath.as_ref().to_string_lossy().to_string(),
            )),
        }
    }
}

#[inline(always)]
fn decode<T: DeserializeInto>(
    filepath: impl AsRef<Path>,
    volatile: bool,
) -> StorageEngineResult<T> {
    if volatile {
        Ok(T::new_empty())
    } else {
        let data = fs::read(filepath.as_ref()).map_err_context(format!(
            "reading file {}",
            filepath.as_ref().to_string_lossy()
        ))?;
        super::de::deserialize_into(&data).ok_or_else(|| {
            StorageEngineError::CorruptedFile(filepath.as_ref().to_string_lossy().to_string())
        })
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
) -> StorageEngineResult<T> {
    let filepath = unsafe { concat_path!(DIR_KSROOT, ksid.as_str(), tblid.as_str()) };
    let tbl = T::unflush_table(filepath, model_code, volatile)?;
    Ok(tbl)
}

/// Read an entire keyspace into a Coremap. You'll need to initialize the rest
pub fn read_keyspace<K: UnflushableKeyspace>(ksid: &ObjectID) -> StorageEngineResult<K> {
    let partmap = self::read_partmap(ksid)?;
    K::unflush_keyspace(partmap, ksid)
}

/// Read the `PARTMAP` for a given keyspace
pub fn read_partmap(ksid: &ObjectID) -> StorageEngineResult<LoadedPartfile> {
    let ksid_str = unsafe { ksid.as_str() };
    let filepath = concat_path!(DIR_KSROOT, ksid_str, "PARTMAP");
    let partmap_raw = fs::read(&filepath)
        .map_err_context(format!("while reading {}", filepath.to_string_lossy()))?;
    super::de::deserialize_set_ctype_bytemark(&partmap_raw)
        .ok_or_else(|| StorageEngineError::corrupted_partmap(ksid))
}

/// Read the `PRELOAD`
pub fn read_preload() -> StorageEngineResult<PreloadSet> {
    let read = fs::read(PRELOAD_PATH).map_err_context("reading PRELOAD")?;
    super::preload::read_preload_raw(read)
}

/// Read everything and return a [`Memstore`]
///
/// If this is a new instance an empty store is returned while the directory tree
/// is also created. If this is an already initialized instance then the store
/// is read and returned (and any possible errors that are encountered are returned)
pub fn read_full() -> StorageEngineResult<Memstore> {
    if is_new_instance()? {
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
    // HACK(@ohsayan): Now pop system back in here
    ksmap.upsert(SYSTEM, Arc::new(Keyspace::empty()));
    Ok(Memstore::init_with_all(ksmap, system_keyspace))
}

/// Check if the `data` directory is non-empty (if not: we're on a new instance)
pub fn is_new_instance() -> StorageEngineResult<bool> {
    match fs::read_dir("data") {
        Ok(mut dir) => Ok(dir.next().is_none()),
        Err(e) if e.kind().eq(&ErrorKind::NotFound) => Ok(true),
        Err(e) => Err(StorageEngineError::ioerror_extra(
            e,
            "while checking data directory",
        )),
    }
}
