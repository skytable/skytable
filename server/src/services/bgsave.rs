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
use crate::corestore::Corestore;
use crate::dbnet::Terminator;
use crate::registry;
use crate::storage;
use libsky::TResult;
use tokio::time::{self, Duration};

/// The bgsave_scheduler calls the bgsave task in `Corestore` after `every` seconds
///
/// The time after which the scheduler will wake up the BGSAVE task is determined by
/// `bgsave_cfg` which is to be passed as an argument. If BGSAVE is disabled, this function
/// immediately returns
pub async fn bgsave_scheduler(handle: Corestore, bgsave_cfg: BGSave, mut terminator: Terminator) {
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

/// Run bgsave
///
/// This function just hides away the BGSAVE blocking section from the _public API_
pub fn run_bgsave(handle: &Corestore) -> TResult<()> {
    storage::flush::flush_full(handle.get_store()).map_err(|e| e.into())
}

/// This just wraps around [`_bgsave_blocking_section`] and prints nice log messages depending on the outcome
fn bgsave_blocking_section(handle: Corestore) -> bool {
    registry::lock_flush_state();
    match run_bgsave(&handle) {
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
