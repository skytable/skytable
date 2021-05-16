/*
 * Created on Sun May 16 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! Compatibility suite for Skytable
//!
//! This module will enable users from an earlier version of Skytable to migrate their data to match
//! the latest format

use crate::coredb::{htable::HTable, Data};
use crate::diskstore::snapshot::SNAP_MATCH;
use bytes::Bytes;
use libsky::TResult;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::iter::FromIterator;
use std::path::PathBuf;

/// The disk storage type since 0.3.1
type DiskStoreType = (Vec<String>, Vec<Vec<u8>>);

const SKY_UPGRADE_FOLDER: &str = "newdata";
const SKY_COMPLETE_UPGRADE_FOLDER: &str = "newdata/snapshots/remote";

pub fn concat_path(other: impl Into<PathBuf>) -> PathBuf {
    let mut path = PathBuf::from(SKY_UPGRADE_FOLDER);
    path.push(other.into());
    path
}

pub fn upgrade() -> TResult<()> {
    fs::create_dir_all(SKY_COMPLETE_UPGRADE_FOLDER)?;
    // first attempt to upgrade the data file
    log::info!("Upgrading data file");
    upgrade_file("data/data.bin", concat_path("data.bin"))
        .map_err(|e| format!("Failed to upgrade data.bin file with error: {}", e))?;
    log::info!("Finished upgrading data file");
    // now let's check what files are there in the snapshots directory
    log::info!("Upgrading snapshots");
    let snapshot_dir = fs::read_dir("data/snapshots")?;
    for path in snapshot_dir {
        let path = path?.path();
        if path.is_dir() && path != PathBuf::from("data/snapshots/remote") {
            return Err("The snapshot directory contains unrecognized files".into());
        }
        if path.is_file() {
            let fname = path
                .file_name()
                .ok_or("Failed to get path name in snapshot directory")?
                .to_string_lossy();
            if !SNAP_MATCH.is_match(&fname) {
                return Err("The snapshot directory contains unexpected files".into());
            }
            upgrade_file(path.clone(), concat_path(format!("snapshots/{}", fname)))?;
        }
    }
    log::info!("Finished upgrading snapshots");
    log::info!("Upgrading remote snapshots");
    let remote_snapshot_dir = fs::read_dir("data/snapshots/remote")?;
    for path in remote_snapshot_dir {
        let path = path?.path();
        if path.is_file() {
            let fname = path
                .file_name()
                .ok_or("Failed to get filename in remote snapshot directory")?
                .to_string_lossy();
            upgrade_file(
                path.clone(),
                concat_path(format!("snapshots/remote/{}", fname)),
            )?;
        } else {
            return Err("Unexpected files in the remote snapshot directory".into());
        }
    }
    log::info!("Finished upgrading remote snapshots");
    log::info!("All files were upgraded. Updating directories");
    fs::rename("data", "olddata")?;
    log::info!("Moved old data into folder 'olddata'");
    fs::rename(SKY_UPGRADE_FOLDER, "data")?;
    log::info!("Successfully finished upgrade");
    Ok(())
}

fn upgrade_file(src: impl Into<PathBuf>, destination: impl Into<PathBuf>) -> TResult<()> {
    let file = src.into();
    log::info!("Upgrading file: {}", file.to_string_lossy());
    let old_data_file = fs::read(&file)?;
    let data_from_old_file: DiskStoreType = bincode::deserialize(&old_data_file)?;
    let data_from_old_file: HashMap<String, Data> = HashMap::from_iter(
        data_from_old_file
            .0
            .into_iter()
            .zip(data_from_old_file.1.into_iter())
            .map(|(key, value)| (key, Data::from_blob(Bytes::from(value)))),
    );
    let data_in_new_format: HTable<Data, Data> = HTable::from_iter(
        data_from_old_file
            .into_iter()
            .map(|(key, value)| (Data::from_string(key), value)),
    );
    let data_in_new_format = bincode::serialize(&data_in_new_format)?;
    let destination = destination.into();
    let mut file = fs::File::create(&destination)?;
    log::info!("Writing upgraded file to {}", destination.to_string_lossy());
    file.write_all(&data_in_new_format)?;
    Ok(())
}
