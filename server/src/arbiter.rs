/*
 * Created on Sat Jun 26 2021
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
        auth::AuthProvider,
        config::{ConfigurationSet, SnapshotConfig, SnapshotPref},
        corestore::Corestore,
        dbnet::{self, Terminator},
        diskstore::flock::FileLock,
        services,
        storage::v1::sengine::SnapshotEngine,
        util::{
            error::{Error, SkyResult},
            os::TerminationSignal,
        },
    },
    std::{sync::Arc, thread::sleep},
    tokio::{
        sync::{
            broadcast,
            mpsc::{self, Sender},
        },
        task::{self, JoinHandle},
        time::Duration,
    },
};

const TERMSIG_THRESHOLD: usize = 3;

/// Start the server waiting for incoming connections or a termsig
pub async fn run(
    ConfigurationSet {
        ports,
        bgsave,
        snapshot,
        maxcon,
        auth,
        protocol,
        ..
    }: ConfigurationSet,
    restore_filepath: Option<String>,
) -> SkyResult<Corestore> {
    // Intialize the broadcast channel
    let (signal, _) = broadcast::channel(1);
    let engine = match &snapshot {
        SnapshotConfig::Enabled(SnapshotPref { atmost, .. }) => SnapshotEngine::new(*atmost),
        SnapshotConfig::Disabled => SnapshotEngine::new_disabled(),
    };
    let engine = Arc::new(engine);
    // restore data
    services::restore_data(restore_filepath)
        .map_err(|e| Error::ioerror_extra(e, "restoring data from backup"))?;
    // init the store
    let db = Corestore::init_with_snapcfg(engine.clone())?;
    // refresh the snapshotengine state
    engine.parse_dir()?;
    let auth_provider = match auth.origin_key {
        Some(key) => {
            let authref = db.get_store().setup_auth();
            AuthProvider::new(authref, Some(key.into_inner()))
        }
        None => AuthProvider::new_disabled(),
    };

    // initialize the background services
    let bgsave_handle = tokio::spawn(services::bgsave::bgsave_scheduler(
        db.clone(),
        bgsave,
        Terminator::new(signal.subscribe()),
    ));
    let snapshot_handle = tokio::spawn(services::snapshot::snapshot_service(
        engine,
        db.clone(),
        snapshot,
        Terminator::new(signal.subscribe()),
    ));

    // bind to signals
    let termsig =
        TerminationSignal::init().map_err(|e| Error::ioerror_extra(e, "binding to signals"))?;
    // start the server (single or multiple listeners)
    let mut server = dbnet::connect(
        ports,
        protocol,
        maxcon,
        db.clone(),
        auth_provider,
        signal.clone(),
    )
    .await?;

    tokio::select! {
        _ = server.run_server() => {},
        _ = termsig => {}
    }

    log::info!("Signalling all workers to shut down");
    // drop the signal and let others exit
    drop(signal);
    server.finish_with_termsig().await;

    // wait for the background services to terminate
    let _ = snapshot_handle.await;
    let _ = bgsave_handle.await;
    Ok(db)
}

fn spawn_task(tx: Sender<bool>, db: Corestore, do_sleep: bool) -> JoinHandle<()> {
    task::spawn_blocking(move || {
        if do_sleep {
            log::info!("Waiting for 10 seconds before retrying ...");
            sleep(Duration::from_secs(10));
        }
        let ret = match crate::services::bgsave::run_bgsave(&db) {
            Ok(()) => {
                log::info!("Save before termination successful");
                true
            }
            Err(e) => {
                log::error!("Failed to run save on termination: {e}");
                false
            }
        };
        tx.blocking_send(ret).expect("Receiver dropped");
    })
}

pub fn finalize_shutdown(corestore: Corestore, pid_file: FileLock) {
    assert_eq!(
        corestore.strong_count(),
        1,
        "Correctness error. finalize_shutdown called before dropping server runtime"
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("server-final")
        .enable_all()
        .build()
        .unwrap();
    let dbc = corestore.clone();
    let mut okay: bool = rt.block_on(async move {
        let db = dbc;
        let (tx, mut rx) = mpsc::channel::<bool>(1);
        spawn_task(tx.clone(), db.clone(), false);
        let spawn_again = || {
            // we failed, so we better sleep
            // now spawn it again to see the state
            spawn_task(tx.clone(), db.clone(), true)
        };
        let mut threshold = TERMSIG_THRESHOLD;
        loop {
            let termsig = match TerminationSignal::init().map_err(|e| e.to_string()) {
                Ok(sig) => sig,
                Err(e) => {
                    log::error!("Failed to bind to signal with error: {e}");
                    crate::exit_error();
                }
            };
            tokio::select! {
                ret = rx.recv() => {
                    if ret.unwrap() {
                        // that's good to go
                        break true;
                    } else {
                        spawn_again();
                    }
                }
                _ = termsig => {
                    threshold -= 1;
                    if threshold == 0 {
                        log::error!("Termination signal received but failed to flush data. Quitting because threshold exceeded");
                        break false;
                    } else {
                        log::error!("Termination signal received but failed to flush data. Threshold is at {threshold}");
                        continue;
                    }
                },
            }
        }
    });
    okay &= services::pre_shutdown_cleanup(pid_file, Some(corestore.get_store()));
    if okay {
        log::info!("Goodbye :)");
    } else {
        log::error!("Didn't terminate successfully");
        crate::exit_error();
    }
}
