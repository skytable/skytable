/*
 * Created on Sat Jul 10 2021
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

//! Interfaces with the file system

use crate::corestore::htable::Coremap;
use crate::corestore::htable::Data;
use crate::corestore::memstore::Keyspace;
use crate::corestore::memstore::Memstore;
use crate::registry;
use crate::IoResult;
use std::collections::HashSet;
use std::fs;
use std::io::{BufWriter, Write};

pub const DIR_KSROOT: &str = "data/ks";
pub const DIR_SNAPROOT: &str = "data/snaps";
pub const DIR_RSNAPROOT: &str = "data/rsnap";
pub const DIR_BACKUPS: &str = "data/backups";
pub const DIR_ROOT: &str = "data";

/// This creates the root directory structure:
/// ```
/// data/
///     ks/
///         ks1/
///         ks2/
///         ks3/
///     snaps/
///     backups/
/// ```
///
/// If any directories exist, they are simply ignored
pub fn create_tree(memroot: &Memstore) -> IoResult<()> {
    try_dir_ignore_existing!(
        DIR_ROOT,
        DIR_KSROOT,
        DIR_BACKUPS,
        DIR_SNAPROOT,
        DIR_RSNAPROOT
    );
    for ks in memroot.keyspaces.iter() {
        unsafe {
            try_dir_ignore_existing!(concat_path!(DIR_KSROOT, ks.key().as_str()))?;
        }
    }
    Ok(())
}

pub fn snap_create_tree(snapdir: &str, snapid: &str, memroot: &Memstore) -> IoResult<()> {
    for ks in memroot.keyspaces.iter() {
        unsafe {
            try_dir_ignore_existing!(concat_path!(snapdir, snapid, ks.key().as_str()))?;
        }
    }
    Ok(())
}

/// Clean up the tree
///
/// **Warning**: Calling this is quite inefficient so consider calling it once or twice
/// throughout the lifecycle of the server
pub fn cleanup_tree(memroot: &Memstore) -> IoResult<()> {
    if registry::get_preload_tripswitch().is_tripped() {
        // only run a cleanup if someone tripped the switch
        // hashset because the fs itself will not allow duplicate entries
        let dir_keyspaces: HashSet<String> = read_dir_to_col!(DIR_KSROOT);
        let our_keyspaces: HashSet<String> = memroot
            .keyspaces
            .iter()
            .map(|kv| unsafe { kv.key().as_str() }.to_owned())
            .collect();
        // these are the folders that we need to remove; plonk the deleted keyspaces first
        for folder in dir_keyspaces.difference(&our_keyspaces) {
            if folder != "PRELOAD" {
                let ks_path = concat_str!(DIR_KSROOT, "/", folder);
                fs::remove_dir_all(ks_path)?;
            }
        }
        // now plonk the data files
        for keyspace in memroot.keyspaces.iter() {
            let ks_path = unsafe { concat_str!(DIR_KSROOT, "/", keyspace.key().as_str()) };
            let dir_tbls: HashSet<String> = read_dir_to_col!(&ks_path);
            let our_tbls: HashSet<String> = keyspace
                .value()
                .tables
                .iter()
                .map(|v| unsafe { v.key().as_str() }.to_owned())
                .collect();
            for old_file in dir_tbls.difference(&our_tbls) {
                if old_file != "PARTMAP" {
                    // plonk this data file; we don't need it anymore
                    fs::remove_file(concat_path!(&ks_path, old_file))?;
                }
            }
        }
    }
    Ok(())
}

/// Uses a buffered writer under the hood to improve write performance as the provided
/// writable interface might be very slow. The buffer does flush once done, however, it
/// is important that you fsync yourself!
pub fn serialize_map_into_slow_buffer<T: Write>(
    buffer: &mut T,
    map: &Coremap<Data, Data>,
) -> IoResult<()> {
    let mut buffer = BufWriter::new(buffer);
    super::se::raw_serialize_map(map, &mut buffer)?;
    buffer.flush()?;
    Ok(())
}

pub fn serialize_partmap_into_slow_buffer<T: Write>(buffer: &mut T, ks: &Keyspace) -> IoResult<()> {
    let mut buffer = BufWriter::new(buffer);
    super::se::raw_serialize_partmap(&mut buffer, ks)?;
    buffer.flush()?;
    Ok(())
}

pub fn serialize_preload_into_slow_buffer<T: Write>(
    buffer: &mut T,
    store: &Memstore,
) -> IoResult<()> {
    let mut buffer = BufWriter::new(buffer);
    super::preload::raw_generate_preload(&mut buffer, store)?;
    buffer.flush()?;
    Ok(())
}
