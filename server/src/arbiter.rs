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

use crate::config::ConfigurationSet;
use crate::config::SnapshotConfig;
use crate::config::SnapshotPref;
use crate::corestore::Corestore;
use crate::dbnet::{self, Terminator};
use crate::diskstore::flock::FileLock;
use crate::services;
use crate::storage::sengine::SnapshotEngine;
use libsky::util::terminal;
use std::sync::Arc;
use std::thread::sleep;
use tokio::{
    signal::ctrl_c,
    sync::{
        broadcast,
        mpsc::{self, Sender},
    },
    task::{self, JoinHandle},
    time::Duration,
};

const TERMSIG_THRESHOLD: usize = 3;

#[cfg(unix)]
use core::{future::Future, pin::Pin, task::Context, task::Poll};
#[cfg(unix)]
use tokio::signal::unix::{signal as fnsignal, Signal, SignalKind};
#[cfg(unix)]
/// Object to bind to unix-specific signals
pub struct UnixTerminationSignal {
    sigterm: Signal,
}

#[cfg(unix)]
impl UnixTerminationSignal {
    pub fn init() -> Result<Self, String> {
        let sigterm = fnsignal(SignalKind::terminate())
            .map_err(|e| format!("Failed to bind to signal with: {}", e))?;
        Ok(Self { sigterm })
    }
}

#[cfg(unix)]
impl Future for UnixTerminationSignal {
    type Output = Option<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.sigterm.poll_recv(ctx)
    }
}

/// Start the server waiting for incoming connections or a termsig
pub async fn run(
    ConfigurationSet {
        ports,
        bgsave,
        snapshot,
        maxcon,
        ..
    }: ConfigurationSet,
    restore_filepath: Option<String>,
) -> Result<Corestore, String> {
    // Intialize the broadcast channel
    let (signal, _) = broadcast::channel(1);
    let engine;
    match &snapshot {
        SnapshotConfig::Enabled(SnapshotPref { atmost, .. }) => {
            engine = SnapshotEngine::new(*atmost);
            engine
                .parse_dir()
                .map_err(|e| format!("Failed to init snapshot engine: {}", e))?;
        }
        SnapshotConfig::Disabled => {
            engine = SnapshotEngine::new_disabled();
        }
    }
    let engine = Arc::new(engine);
    // restore data
    services::restore_data(restore_filepath)
        .map_err(|e| format!("Failed to restore data from backup with error: {}", e))?;
    let db = Corestore::init_with_snapcfg(engine.clone())
        .map_err(|e| format!("Error while initializing database: {}", e))?;

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

    // bind the ctrlc handler
    let sig = tokio::signal::ctrl_c();

    // start the server (single or multiple listeners)
    let mut server = dbnet::connect(ports, maxcon, db.clone(), signal.clone()).await?;

    #[cfg(not(unix))]
    {
        // Non-unix, usually Windows specific signal handling.
        // FIXME(@ohsayan): For now, let's just
        // bother with ctrl+c, we'll move ahead as users require them
        tokio::select! {
            _ = server.run_server() => {}
            _ = sig => {}
        }
    }
    #[cfg(unix)]
    {
        let sigterm = UnixTerminationSignal::init()?;
        // apart from CTRLC, the only other thing we care about is SIGTERM
        // FIXME(@ohsayan): Maybe we should respond to SIGHUP too?
        tokio::select! {
            _ = server.run_server() => {},
            _ = sig => {},
            _ = sigterm => {}
        }
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
            Ok(()) => true,
            Err(e) => {
                log::error!("Failed to run save on termination: {e}");
                false
            }
        };
        tx.blocking_send(ret).expect("Receiver dropped");
    })
}

pub fn finalize_shutdown(corestore: Corestore, pid_file: FileLock) {
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
        let mut threshold = TERMSIG_THRESHOLD;
        loop {
            tokio::select! {
                ret = rx.recv() => {
                    if ret.unwrap() {
                        // that's good to go
                        log::info!("Save before termination successful");
                        break true;
                    } else {
                        let txc = tx.clone();
                        let dbc = db.clone();
                        // we failed, so we better sleep
                        // now spawn it again to see the state
                        spawn_task(txc, dbc, true);
                    }
                }
                _ = ctrl_c() => {
                    if threshold == 0 {
                        log::error!("SIGTERM received but failed to flush data. Quitting because threshold exceeded");
                        break false;
                    } else {
                        log::error!("SIGTERM received but failed to flush data. Threshold is at {threshold}");
                        threshold -= 1;
                        continue;
                    }
                }
            }
        }
    });
    okay &= services::pre_shutdown_cleanup(pid_file, Some(corestore.get_store()));
    if okay {
        terminal::write_success("Goodbye :)").unwrap()
    } else {
        log::error!("Didn't terminate successfully");
        crate::exit_error();
    }
}
