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

use {
    crate::{
        corestore::memstore::Memstore,
        registry,
        storage::v1::flush::{FlushableKeyspace, FlushableTable, StorageTarget},
        IoResult,
    },
    core::ops::Deref,
    std::{
        collections::{HashMap, HashSet},
        fs,
        io::{BufWriter, Write},
    },
};

pub const DIR_KSROOT: &str = "data/ks";
pub const DIR_SNAPROOT: &str = "data/snaps";
pub const DIR_RSNAPROOT: &str = "data/rsnap";
pub const DIR_BACKUPS: &str = "data/backups";
pub const DIR_ROOT: &str = "data";

/// Creates the directories for the keyspaces
pub fn create_tree<T: StorageTarget>(target: &T, memroot: &Memstore) -> IoResult<()> {
    for ks in memroot.keyspaces.iter() {
        unsafe {
            try_dir_ignore_existing!(target.keyspace_target(ks.key().as_str()))?;
        }
    }
    Ok(())
}

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
pub fn create_tree_fresh<T: StorageTarget>(target: &T, memroot: &Memstore) -> IoResult<()> {
    try_dir_ignore_existing!(
        DIR_ROOT,
        DIR_KSROOT,
        DIR_BACKUPS,
        DIR_SNAPROOT,
        DIR_RSNAPROOT
    );
    self::create_tree(target, memroot)
}

/// Clean up the tree
///
/// **Warning**: Calling this is quite inefficient so consider calling it once or twice
/// throughout the lifecycle of the server
pub fn cleanup_tree(memroot: &Memstore) -> IoResult<()> {
    if registry::get_cleanup_tripswitch().is_tripped() {
        log::info!("We're cleaning up ...");
        // only run a cleanup if someone tripped the switch
        // hashset because the fs itself will not allow duplicate entries
        // the keyspaces directory will contain the PRELOAD file, but we'll just
        // remove it from the list
        let mut dir_keyspaces: HashSet<String> = read_dir_to_col!(DIR_KSROOT);
        dir_keyspaces.remove("PRELOAD");
        let our_keyspaces: HashMap<String, HashSet<String>> = memroot
            .keyspaces
            .iter()
            .map(|kv| {
                let ksid = unsafe { kv.key().as_str() }.to_owned();
                let tables: HashSet<String> = kv
                    .value()
                    .tables
                    .iter()
                    .map(|tbl| unsafe { tbl.key().as_str() }.to_owned())
                    .collect();
                (ksid, tables)
            })
            .collect();

        // these are the folders that we need to remove; plonk the deleted keyspaces first
        let keyspaces_to_remove: Vec<&String> = dir_keyspaces
            .iter()
            .filter(|ksname| !our_keyspaces.contains_key(ksname.as_str()))
            .collect();
        for folder in keyspaces_to_remove {
            let ks_path = concat_str!(DIR_KSROOT, "/", folder);
            fs::remove_dir_all(ks_path)?;
        }

        // HACK(@ohsayan): Due to the nature of how system tables are stored in v1, we need to get rid of this
        // ensuring that system tables don't end up being removed (since no system tables are actually
        // purged at this time)
        let mut our_keyspaces = our_keyspaces;
        our_keyspaces.remove("system").unwrap();
        let our_keyspaces = our_keyspaces;

        // now remove the dropped tables
        for (keyspace, tables) in our_keyspaces {
            let ks_path = concat_str!(DIR_KSROOT, "/", keyspace.as_str());
            // read what is present in the tables directory
            let mut dir_tbls: HashSet<String> = read_dir_to_col!(&ks_path);
            // in the list of directories we collected, remove PARTMAP because we should NOT
            // delete it
            dir_tbls.remove("PARTMAP");
            // find what tables we should remove
            let tables_to_remove = dir_tbls.difference(&tables);
            for removed_table in tables_to_remove {
                let fpath = concat_path!(&ks_path, removed_table);
                fs::remove_file(&fpath)?;
            }
        }
    }
    Ok(())
}

/// Uses a buffered writer under the hood to improve write performance as the provided
/// writable interface might be very slow. The buffer does flush once done, however, it
/// is important that you fsync yourself!
pub fn serialize_into_slow_buffer<T: Write, U: FlushableTable>(
    buffer: &mut T,
    writable_item: &U,
) -> IoResult<()> {
    let mut buffer = BufWriter::new(buffer);
    writable_item.write_table_to(&mut buffer)?;
    buffer.flush()?;
    Ok(())
}

pub fn serialize_partmap_into_slow_buffer<T, U, Tbl, K>(buffer: &mut T, ks: &K) -> IoResult<()>
where
    T: Write,
    U: Deref<Target = Tbl>,
    Tbl: FlushableTable,
    K: FlushableKeyspace<Tbl, U>,
{
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
