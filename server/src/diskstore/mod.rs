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

use crate::config::BGSave;
use crate::coredb::{self, Data};
use bincode;
use bytes::Bytes;
use libsky::TResult;
use std::collections::HashMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::process;
use std::time::Duration;
use tokio::time;
pub mod flock;
pub mod snapshot;
mod snapstore;

/// This type alias is to be used when deserializing binary data from disk
type DiskStoreFromDisk = (Vec<String>, Vec<Vec<u8>>);
/// This type alias is to be used when serializing data from the in-memory table
/// onto disk
type DiskStoreFromMemory<'a> = (Vec<&'a String>, Vec<&'a [u8]>);
lazy_static::lazy_static! {
    pub static ref PERSIST_FILE: PathBuf = PathBuf::from("./data.bin");
}

/// Try to get the saved data from disk. This returns `None`, if the `data.bin` wasn't found
/// otherwise the `data.bin` file is deserialized and parsed into a `HashMap`
pub fn get_saved(location: Option<PathBuf>) -> TResult<Option<HashMap<String, Data>>> {
    let file = match fs::read(
        location
            .map(|loc| loc.to_path_buf())
            .unwrap_or(PERSIST_FILE.to_path_buf()),
    ) {
        Ok(f) => f,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => return Ok(None),
            _ => return Err(format!("Couldn't read flushed data from disk: {}", e).into()),
        },
    };
    let parsed: DiskStoreFromDisk = bincode::deserialize(&file)?;
    let parsed: HashMap<String, Data> = HashMap::from_iter(
        parsed
            .0
            .into_iter()
            .zip(parsed.1.into_iter())
            .map(|(key, value)| {
                let data = Data::from_blob(Bytes::from(value));
                (key, data)
            }),
    );
    Ok(Some(parsed))
}

/// Flush the in-memory table onto disk
///
/// This functions takes the entire in-memory table and writes it to the disk,
/// more specifically, the `data.bin` file
pub fn flush_data(file: &mut flock::FileLock, data: &HashMap<String, Data>) -> TResult<()> {
    let encoded = serialize(&data)?;
    file.write(&encoded)?;
    Ok(())
}

pub fn write_to_disk(file: &PathBuf, data: &HashMap<String, Data>) -> TResult<()> {
    let mut file = fs::File::create(&file)?;
    let encoded = serialize(&data)?;
    file.write_all(&encoded)?;
    Ok(())
}

fn serialize(data: &HashMap<String, Data>) -> TResult<Vec<u8>> {
    let ds: DiskStoreFromMemory = (
        data.keys().into_iter().collect(),
        data.values().map(|val| val.get_inner_ref()).collect(),
    );
    let encoded = bincode::serialize(&ds)?;
    Ok(encoded)
}

/// The bgsave_scheduler calls the bgsave task in `CoreDB` after `every` seconds
///
/// The time after which the scheduler will wake up the BGSAVE task is determined by
/// `bgsave_cfg` which is to be passed as an argument. If BGSAVE is disabled, this function
/// immediately returns
pub async fn bgsave_scheduler(
    handle: coredb::CoreDB,
    bgsave_cfg: BGSave,
    mut file: flock::FileLock,
) {
    let duration = match bgsave_cfg {
        BGSave::Disabled => {
            // So, there's no BGSAVE! Looks like our user's pretty confident
            // that there won't be any power failures! Never mind, we'll just
            // shut down the BGSAVE task, and immediately return
            handle.shared.bgsave_task.notified().await;
            return;
        }
        BGSave::Enabled(duration) => {
            // If we're here - the user doesn't trust his power supply or just values
            // his data - which is good! So we'll turn this into a `Duration`
            Duration::from_secs(duration)
        }
    };
    while !handle.shared.is_termsig() {
        if handle.shared.run_bgsave(&mut file) {
            tokio::select! {
                // Sleep until `duration` from the current time instant
                _ = time::sleep_until(time::Instant::now() + duration) => {}
                // Otherwise wait for a notification
                _ = handle.shared.bgsave_task.notified() => {}
            }
        } else {
            handle.shared.bgsave_task.notified().await
        }
    }
    if let Err(e) = tokio::task::spawn_blocking(move || {
        if let Err(e) = file.unlock() {
            log::error!("Failed to release lock on data file with error '{}'", e);
            process::exit(0x100);
        }
    })
    .await
    {
        log::error!(
            "Blocking operation to unlock data file failed with error '{}'",
            e
        );
    }
}
