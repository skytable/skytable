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

use crate::config::BGSave;
use crate::config::SnapshotConfig;
use crate::config::SnapshotPref;
use crate::corestore::Corestore;
use crate::dbnet::{self, Terminator};
use crate::services;
use crate::storage::sengine::SnapshotEngine;
use crate::PortConfig;
use std::sync::Arc;
use tokio::sync::broadcast;

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
    ports: PortConfig,
    bgsave_cfg: BGSave,
    snapshot_cfg: SnapshotConfig,
    _restore_filepath: Option<String>,
    maxcon: usize,
) -> Result<Corestore, String> {
    // Intialize the broadcast channel
    let (signal, _) = broadcast::channel(1);
    let engine;
    match &snapshot_cfg {
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
    let db = Corestore::init_with_snapcfg(engine.clone())
        .map_err(|e| format!("Error while initializing database: {}", e))?;

    // initialize the background services
    let bgsave_handle = tokio::spawn(services::bgsave::bgsave_scheduler(
        db.clone(),
        bgsave_cfg,
        Terminator::new(signal.subscribe()),
    ));
    let snapshot_handle = tokio::spawn(services::snapshot::snapshot_service(
        engine,
        db.clone(),
        snapshot_cfg,
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
