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

use {
    crate::{
        config::SnapshotConfig,
        corestore::Corestore,
        registry,
        storage::v1::sengine::{SnapshotActionResult, SnapshotEngine},
    },
    std::sync::Arc,
    tokio::{
        sync::broadcast::Receiver,
        time::{self, Duration},
    },
};

/// The snapshot service
///
/// This service calls `SnapEngine::mksnap()` periodically to create snapshots. Whenever
/// the interval for snapshotting expires or elapses, we create a snapshot. The snapshot service
/// keeps creating snapshots, as long as the database keeps running. Once [`dbnet::run`] broadcasts
/// a termination signal, we're ready to quit. This function will, by default, poison the database
/// if snapshotting fails, unless customized by the user.
pub async fn snapshot_service(
    engine: Arc<SnapshotEngine>,
    handle: Corestore,
    ss_config: SnapshotConfig,
    mut termination_signal: Receiver<()>,
) {
    match ss_config {
        SnapshotConfig::Disabled => {
            // since snapshotting is disabled, we'll imediately return
            return;
        }
        SnapshotConfig::Enabled(configuration) => {
            let (duration, _, failsafe) = configuration.decompose();
            let duration = Duration::from_secs(duration);
            loop {
                tokio::select! {
                    _ = time::sleep_until(time::Instant::now() + duration) => {
                        let succeeded = engine.mksnap(handle.clone_store()).await == SnapshotActionResult::Ok;
                        #[cfg(test)]
                        {
                            use std::env::set_var;
                            if succeeded {
                                set_var("SKYTEST_SNAPSHOT_OKAY", "true");
                            } else {
                                set_var("SKYTEST_SNAPSHOT_OKAY", "false");
                            }
                        }
                        if succeeded {
                            // it passed, so unpoison the handle
                            registry::unpoison();
                        } else if failsafe {
                            // mksnap returned false and we are set to stop writes if snapshotting failed
                            // so let's poison the handle
                            registry::poison();
                        }
                    },
                    _ = termination_signal.recv() => {
                        // time to terminate; goodbye!
                        break;
                    }
                }
            }
        }
    }
    log::info!("Snapshot service has exited");
}
