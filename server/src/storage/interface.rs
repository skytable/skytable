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

use super::PartitionID;
use crate::coredb::buffers::Integer32Buffer;
use crate::coredb::htable::Coremap;
use crate::coredb::htable::Data;
use crate::coredb::memstore::Memstore;
use std::fs;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

const DIR_KSROOT: &str = "data/ks";
const DIR_SNAPROOT: &str = "data/snaps";
const DIR_BACKUPS: &str = "data/backups";
const DIR_ROOT: &str = "data";

macro_rules! try_dir_ignore_existing {
    ($dir:expr) => {{
        match fs::create_dir_all($dir) {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => Ok(()),
                _ => Err(e),
            },
        }
    }};
    ($($dir:expr),*) => {
        $(try_dir_ignore_existing!($dir)?;)*
    }
}

macro_rules! concat_path {
    ($($s:expr),*) => {{ {
        let mut path = PathBuf::with_capacity($(($s).len()+)*0);
        $(path.push($s);)*
        path
    }}};
}

/// This creates the root directory structure:
/// ```
/// data/
///     ks/
///         ks1/
///         ks2/
///         ks3/
///     snaps/
///         ks1/
///             tbl1/
///             tbl2/
///         ks2/
///             tbl1/
///             tbl2/
///         ks3/
///             tbl1/
///             tbl2/
///     backups/
/// ```
///
/// If any directories exist, they are simply ignored
pub fn create_tree(memroot: Memstore) -> IoResult<()> {
    try_dir_ignore_existing!(DIR_ROOT, DIR_KSROOT, DIR_BACKUPS, DIR_SNAPROOT);
    for ks in memroot.keyspaces.iter() {
        unsafe {
            try_dir_ignore_existing!(concat_path!(DIR_KSROOT, ks.key().as_str()))?;
            for tbl in ks.value().tables.iter() {
                try_dir_ignore_existing!(concat_path!(
                    DIR_SNAPROOT,
                    ks.key().as_str(),
                    tbl.key().as_str()
                ))?;
            }
        }
    }
    Ok(())
}

#[test]
fn test_tree() {
    create_tree(Memstore::new_default()).unwrap();
    let read_ks: Vec<String> = fs::read_dir(DIR_KSROOT)
        .unwrap()
        .map(|dir| {
            let v = dir.unwrap().file_name();
            v.to_string_lossy().to_string()
        })
        .collect();
    assert_eq!(read_ks, vec!["default".to_owned()]);
    // just read one level of the snaps dir
    let read_snaps: Vec<String> = fs::read_dir(DIR_SNAPROOT)
        .unwrap()
        .map(|dir| {
            let v = dir.unwrap().file_name();
            v.to_string_lossy().to_string()
        })
        .collect();
    assert_eq!(read_snaps, vec!["default".to_owned()]);
    // now read level two: snaps/default
    let read_snaps: Vec<String> = fs::read_dir(concat_path!(DIR_SNAPROOT, "default"))
        .unwrap()
        .map(|dir| {
            let v = dir.unwrap().file_name();
            v.to_string_lossy().to_string()
        })
        .collect();
    assert_veceq!(read_snaps, vec!["_system".to_owned(), "default".to_owned()]);
    assert!(PathBuf::from("data/backups").is_dir());
    // clean up
    fs::remove_dir_all(DIR_ROOT).unwrap();
}

/// Uses a buffered writer under the hood to improve write performance as the provided
/// writable interface might be very slow. The buffer does flush once done, however, it
/// is important that you fsync yourself!
pub fn serialize_map_into_slow_buffer<T: Write>(
    buffer: &mut T,
    map: &Coremap<Data, Data>,
) -> std::io::Result<()> {
    let mut buffer = BufWriter::new(buffer);
    super::raw_serialize_map(map, &mut buffer)?;
    buffer.flush()?;
    Ok(())
}

/// Get the file for COW. If the parition ID is 0000
fn cow_file(id: PartitionID) -> Integer32Buffer {
    let mut buffer = Integer32Buffer::init(id);
    unsafe {
        // UNSAFE(@ohsayan): We know we're just pushing in one thing
        buffer.push(b'_');
    }
    buffer
}

#[test]
fn test_cowfile() {
    let cow_file = cow_file(10);
    assert_eq!(cow_file, "10_".to_owned());
    assert_eq!(&cow_file[..cow_file.len() - 1], "10".to_owned());
}
