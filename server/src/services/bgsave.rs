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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use crate::config::BGSave;
use crate::coredb::CoreDB;
use crate::dbnet::Terminator;
use crate::diskstore::{self, flock};
use crate::registry;
#[cfg(not(test))]
use diskstore::PERSIST_FILE;
use libsky::TResult;
use std::fs;
use tokio::time::{self, Duration};

const SKY_TEMP_FILE: &str = "__skydata_file.bin";
#[cfg(test)]
/// The **test location** for the PERSIST_FILE
pub const BGSAVE_DIRECTORY_TESTING_LOC: &str = "skydata_bgsavetest.bin";

/// The bgsave_scheduler calls the bgsave task in `CoreDB` after `every` seconds
///
/// The time after which the scheduler will wake up the BGSAVE task is determined by
/// `bgsave_cfg` which is to be passed as an argument. If BGSAVE is disabled, this function
/// immediately returns
pub async fn bgsave_scheduler(handle: CoreDB, bgsave_cfg: BGSave, mut terminator: Terminator) {
    match bgsave_cfg {
        BGSave::Enabled(duration) => {
            // If we're here - the user doesn't trust his power supply or just values
            // his data - which is good! So we'll turn this into a `Duration`
            let duration = Duration::from_secs(duration);
            loop {
                tokio::select! {
                    // Sleep until `duration` from the current time instant
                    _ = time::sleep_until(time::Instant::now() + duration) => {
                        let cloned_handle = handle.clone();
                        // we spawn this process just to ensure that it doesn't block the runtime's workers
                        // dedicated to async tasks (non-blocking)
                        tokio::task::spawn_blocking(move || {
                            let owned_handle = cloned_handle;
                            let _ = bgsave_blocking_section(owned_handle);
                        }).await.expect("Something caused the background service to panic");
                    }
                    // Otherwise wait for a notification
                    _ = terminator.receive_signal() => {
                        // we got a notification to quit; so break out
                        break;
                    }
                }
            }
        }
        BGSave::Disabled => {
            // the user doesn't bother about his data; cool, let's not bother about it either
        }
    }
    log::info!("BGSAVE service has exited");
}

/// This is a _raw_ version of what Sky's persistence does and is **blocking in nature** since it does
/// a good amount of disk I/O (which totally depends on the size of the dataset though)
/// There's nothing dangerous about this really and hence it isn't as _raw_ as it sounds. This method accepts
/// a handle to a [`coredb::CoreDB`] and uses that to acquire a read lock. This method will create a temporary
/// file and lock it. It then passes an immutable HTable reference to [`diskstore::flush_data`] which flushes the data to our
/// temporary locked file. Once the data is successfully flushed, the new temporary file replaces the old data file
/// by using [`fs::rename`]. This provides us with two gurantees:
/// 1. No silly logic is seen if the user deletes the data.bin file and yet BGSAVE doesn't complain
/// 2. If this method crashes midway, we can still be sure that the old file is intact
fn _bgsave_blocking_section(handle: &CoreDB) -> TResult<()> {
    // first lock our temporary file
    let mut file = flock::FileLock::lock(SKY_TEMP_FILE)?;
    // get a read lock on the coretable
    let tbl_ref = handle.get_ref();
    diskstore::flush_data(&mut file, &*tbl_ref)?;
    // now rename the file
    #[cfg(not(test))]
    fs::rename(SKY_TEMP_FILE, &*PERSIST_FILE)?;
    #[cfg(test)]
    fs::rename(SKY_TEMP_FILE, BGSAVE_DIRECTORY_TESTING_LOC)?;
    // now unlock the file
    file.unlock()?;
    // close the file
    drop(file);
    Ok(())
}

/// Run bgsave
///
/// This function just hides away the BGSAVE blocking section from the _public API_
pub fn run_bgsave(handle: &CoreDB) -> TResult<()> {
    _bgsave_blocking_section(handle)
}

/// This just wraps around [`_bgsave_blocking_section`] and prints nice log messages depending on the outcome
fn bgsave_blocking_section(handle: CoreDB) -> bool {
    match _bgsave_blocking_section(&handle) {
        Ok(_) => {
            log::info!("BGSAVE completed successfully");
            registry::unpoison();
            true
        }
        Err(e) => {
            log::error!("BGSAVE failed with error: {}", e);
            registry::poison();
            false
        }
    }
}
