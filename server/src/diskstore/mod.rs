/*
 * Created on Wed Aug 05 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! This module provides tools for handling persistently stored data

use crate::coredb::htable::HTable;
use crate::coredb::Data;
use crate::diskstore::snapshot::{DIR_OLD_SNAPSHOT, DIR_SNAPSHOT};
use bincode;
use bytes::Bytes;
use libsky::TResult;
use std::fs;
use std::io::{ErrorKind, Write};
use std::iter::FromIterator;
use std::path::PathBuf;
pub mod flock;
pub mod snapshot;
mod snapstore;

/// This type alias is to be used when deserializing binary data from disk
type DiskStoreFromDisk = (Vec<String>, Vec<Vec<u8>>);
/// This type alias is to be used when serializing data from the in-memory table
/// onto disk
type DiskStoreFromMemory<'a> = (Vec<&'a String>, Vec<&'a [u8]>);
lazy_static::lazy_static! {
    pub static ref PERSIST_FILE: PathBuf = PathBuf::from("./data/data.bin");
    pub static ref OLD_PATH: PathBuf = PathBuf::from("./data.bin");
}

fn get_snapshot(path: String) -> TResult<Option<HTable<String, Data>>> {
    // the path just has the snapshot name, let's improve that
    let mut snap_location = PathBuf::from(DIR_SNAPSHOT);
    snap_location.push(&path);
    let file = match fs::read(snap_location) {
        Ok(f) => f,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                // Probably the old snapshot directory?
                let mut old_snaploc = PathBuf::from(DIR_OLD_SNAPSHOT);
                old_snaploc.push(path);
                match fs::read(old_snaploc) {
                    Ok(f) => {
                        log::warn!("The new snapshot directory is under the data directory");
                        if let Err(e) = fs::rename(DIR_OLD_SNAPSHOT, DIR_SNAPSHOT) {
                            log::error!(
                                "Failed to migrate snapshot directory into new structure: {}",
                                e
                            );
                            return Err(e.into());
                        } else {
                            log::info!(
                                "Migrated old snapshot directory structure to newer structure"
                            );
                            log::warn!("This backwards compat will be removed in the future");
                        }
                        f
                    }
                    _ => return Err(e.into()),
                }
            }
            _ => return Err(e.into()),
        },
    };
    let parsed = deserialize(file)?;
    Ok(Some(parsed))
}

/// Try to get the saved data from disk. This returns `None`, if the `data/data.bin` wasn't found
/// otherwise the `data/data.bin` file is deserialized and parsed into a `HTable`
pub fn get_saved(path: Option<String>) -> TResult<Option<HTable<String, Data>>> {
    if let Some(path) = path {
        get_snapshot(path)
    } else {
        let file = match fs::read(&*PERSIST_FILE) {
            Ok(f) => f,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    // TODO(@ohsayan): Drop support for this in the future
                    // This might be an old installation still not using the data/data.bin path
                    match fs::read(OLD_PATH.to_path_buf()) {
                        Ok(f) => {
                            log::warn!("Your data file was found to be in the current directory and not in data/data.bin");
                            if let Err(e) = fs::rename("data.bin", "data/data.bin") {
                                log::error!("Failed to move data.bin into data/data.bin directory. Consider moving it manually");
                                return Err(format!(
                                    "Failed to move data.bin into data/data.bin: {}",
                                    e
                                )
                                .into());
                            } else {
                                log::info!("The data file has been moved into the new directory");
                                log::warn!("This backwards compat directory support will be removed in the future");
                            }
                            f
                        }
                        Err(e) => match e.kind() {
                            ErrorKind::NotFound => return Ok(None),
                            _ => {
                                return Err(
                                    format!("Coudln't read flushed data from disk: {}", e).into()
                                )
                            }
                        },
                    }
                }
                _ => return Err(format!("Couldn't read flushed data from disk: {}", e).into()),
            },
        };
        let parsed = deserialize(file)?;
        Ok(Some(parsed))
    }
}

#[cfg(test)]
pub fn test_deserialize(file: Vec<u8>) -> TResult<HTable<String, Data>> {
    deserialize(file)
}
fn deserialize(file: Vec<u8>) -> TResult<HTable<String, Data>> {
    let parsed: DiskStoreFromDisk = bincode::deserialize(&file)?;
    let parsed: HTable<String, Data> = HTable::from_iter(
        parsed
            .0
            .into_iter()
            .zip(parsed.1.into_iter())
            .map(|(key, value)| {
                let data = Data::from_blob(Bytes::from(value));
                (key, data)
            }),
    );
    Ok(parsed)
}

/// Flush the in-memory table onto disk
///
/// This functions takes the entire in-memory table and writes it to the disk,
/// more specifically, the `data/data.bin` file
pub fn flush_data(file: &mut flock::FileLock, data: &HTable<String, Data>) -> TResult<()> {
    let encoded = serialize(&data)?;
    file.write(&encoded)?;
    Ok(())
}

pub fn write_to_disk(file: &PathBuf, data: &HTable<String, Data>) -> TResult<()> {
    let mut file = fs::File::create(&file)?;
    let encoded = serialize(&data)?;
    file.write_all(&encoded)?;
    Ok(())
}

fn serialize(data: &HTable<String, Data>) -> TResult<Vec<u8>> {
    let ds: DiskStoreFromMemory = (
        data.keys().into_iter().collect(),
        data.values().map(|val| val.get_inner_ref()).collect(),
    );
    let encoded = bincode::serialize(&ds)?;
    Ok(encoded)
}
