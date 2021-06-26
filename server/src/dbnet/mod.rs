/*
 * Created on Tue Jul 21 2020
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # `DBNET` - Database Networking
//! This module provides low-level interaction with sockets. It handles the creation of
//! a task for an incoming connection, handling errors if required and finally processing an incoming
//! query.
//!
//! ## Typical flow
//! This is how connections are handled:
//! 1. A remote client creates a TCP connection to the server
//! 2. An asynchronous is spawned on the Tokio runtime
//! 3. Data from the socket is asynchronously read into an 8KB read buffer
//! 4. Once the data is read completely (i.e the source sends an EOF byte), the `protocol` module
//! is used to parse the stream
//! 5. Now errors are handled if they occur. Otherwise, the query is executed by `CoreDB::execute_query()`
//!

use crate::config::BGSave;
use crate::config::PortConfig;
use crate::config::SnapshotConfig;
use crate::config::SslOpts;
use crate::dbnet::tcp::Listener;
use crate::diskstore;
use crate::services;
use diskstore::snapshot::DIR_REMOTE_SNAPSHOT;
mod tcp;
use crate::coredb::CoreDB;
use libsky::TResult;
use std::fs;
use std::future::Future;
use std::io::Error as IoError;
use std::net::IpAddr;
use std::sync::Arc;
use tls::SslListener;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc};
pub mod connection;
mod tls;

pub const MAXIMUM_CONNECTION_LIMIT: usize = 50000;

/// Responsible for gracefully shutting down the server instead of dying randomly
// Sounds very sci-fi ;)
pub struct Terminator {
    terminate: bool,
    signal: broadcast::Receiver<()>,
}

impl Terminator {
    /// Create a new `Terminator` instance
    pub const fn new(signal: broadcast::Receiver<()>) -> Self {
        Terminator {
            // Don't terminate on creation!
            terminate: false,
            signal,
        }
    }
    /// Check if the signal is a termination signal
    pub const fn is_termination_signal(&self) -> bool {
        self.terminate
    }
    /// Wait to receive a shutdown signal
    pub async fn receive_signal(&mut self) {
        // The server may have already been terminated
        // In that event, just return
        if self.terminate {
            return;
        }
        let _ = self.signal.recv().await;
        self.terminate = true;
    }
}

/// The base TCP listener
pub struct BaseListener {
    /// An atomic reference to the coretable
    pub db: CoreDB,
    /// The incoming connection listener (binding)
    pub listener: TcpListener,
    /// The maximum number of connections
    pub climit: Arc<Semaphore>,
    /// The shutdown broadcaster
    pub signal: broadcast::Sender<()>,
    // When all `Sender`s are dropped - the `Receiver` gets a `None` value
    // We send a clone of `terminate_tx` to each `CHandler`
    pub terminate_tx: mpsc::Sender<()>,
    pub terminate_rx: mpsc::Receiver<()>,
}

impl BaseListener {
    pub async fn init(
        db: &CoreDB,
        host: IpAddr,
        port: u16,
        semaphore: Arc<Semaphore>,
        signal: broadcast::Sender<()>,
    ) -> Result<Self, IoError> {
        let (terminate_tx, terminate_rx) = mpsc::channel(1);
        Ok(Self {
            db: db.clone(),
            listener: TcpListener::bind((host, port)).await?,
            climit: semaphore,
            signal,
            terminate_tx,
            terminate_rx,
        })
    }
    pub async fn release_self(self) {
        let Self {
            mut terminate_rx,
            terminate_tx,
            signal,
            ..
        } = self;
        drop(signal);
        drop(terminate_tx);
        let _ = terminate_rx.recv().await;
    }
}

/// This macro returns the bind address of a listener
///
/// We were just very lazy, so we just used a macro instead of a member function
macro_rules! bindaddr {
    ($base:ident) => {
        $base
            .listener
            .local_addr()
            .map_err(|e| format!("Failed to get bind address: {}", e))?;
    };
}

/// Multiple Listener Interface
///
/// A `MultiListener` is an abstraction over an `SslListener` or a `Listener` to facilitate
/// easier asynchronous listening on multiple ports.
///
/// - The `SecureOnly` variant holds an `SslListener`
/// - The `InsecureOnly` variant holds a `Listener`
/// - The `Multi` variant holds both an `SslListener` and a `Listener`
///     This variant enables listening to both secure and insecure sockets at the same time
///     asynchronously
enum MultiListener {
    SecureOnly(SslListener),
    InsecureOnly(Listener),
    Multi(Listener, SslListener),
}

impl MultiListener {
    /// Create a new `InsecureOnly` listener
    pub fn new_insecure_only(base: BaseListener) -> Result<Self, String> {
        log::info!("Server started on: skyhash://{}", bindaddr!(base));
        Ok(MultiListener::InsecureOnly(Listener { base }))
    }
    /// Create a new `SecureOnly` listener
    pub fn new_secure_only(base: BaseListener, ssl: SslOpts) -> Result<Self, String> {
        let bindaddr = bindaddr!(base);
        let slf = MultiListener::SecureOnly(
            SslListener::new_pem_based_ssl_connection(ssl.key, ssl.chain, base)
                .map_err(|e| format!("Couldn't bind to secure port: {}", e))?,
        );
        log::info!("Server started on: skyhash-secure://{}", bindaddr);
        Ok(slf)
    }
    /// Create a new `Multi` listener that has both a secure and an insecure listener
    pub async fn new_multi(
        ssl_base_listener: BaseListener,
        tcp_base_listener: BaseListener,
        ssl: SslOpts,
    ) -> Result<Self, String> {
        let sec_bindaddr = bindaddr!(ssl_base_listener);
        let insec_binaddr = bindaddr!(tcp_base_listener);
        let secure_listener =
            SslListener::new_pem_based_ssl_connection(ssl.key, ssl.chain, ssl_base_listener)
                .map_err(|e| format!("Couldn't bind to secure port: {}", e))?;
        let insecure_listener = Listener {
            base: tcp_base_listener,
        };
        log::info!(
            "Server started on: skyhash://{} and skyhash-secure://{}",
            insec_binaddr,
            sec_bindaddr
        );
        Ok(MultiListener::Multi(insecure_listener, secure_listener))
    }
    /// Start the server
    ///
    /// The running of single and/or parallel listeners is handled by this function by
    /// exploiting the working of async functions
    pub async fn run_server(&mut self) -> TResult<()> {
        match self {
            MultiListener::SecureOnly(secure_listener) => secure_listener.run().await,
            MultiListener::InsecureOnly(insecure_listener) => insecure_listener.run().await,
            MultiListener::Multi(insecure_listener, secure_listener) => {
                let insec = insecure_listener.run();
                let sec = secure_listener.run();
                let (e1, e2) = tokio::join!(insec, sec);
                if let Err(e) = e1 {
                    log::error!("Insecure listener failed with: {}", e);
                }
                if let Err(e) = e2 {
                    log::error!("Secure listener failed with: {}", e);
                }
                Ok(())
            }
        }
    }
    /// Signal the ports to shut down and only return after they have shut down
    ///
    /// **Do note:** This function doesn't flush the `CoreDB` object! The **caller has to
    /// make sure that the data is saved!**
    pub async fn finish_with_termsig(self) {
        match self {
            MultiListener::InsecureOnly(server) => server.base.release_self().await,
            MultiListener::SecureOnly(server) => server.base.release_self().await,
            MultiListener::Multi(insecure, secure) => {
                insecure.base.release_self().await;
                secure.base.release_self().await;
            }
        }
    }
}

#[cfg(unix)]
use core::{pin::Pin, task::Context, task::Poll};
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

/// Start the server waiting for incoming connections or a CTRL+C signal
pub async fn run(
    ports: PortConfig,
    bgsave_cfg: BGSave,
    snapshot_cfg: SnapshotConfig,
    sig: impl Future,
    restore_filepath: Option<String>,
    maxcon: usize,
) -> Result<CoreDB, String> {
    let climit = Arc::new(Semaphore::const_new(maxcon));
    let (signal, _) = broadcast::channel(1);
    fs::create_dir_all(&*DIR_REMOTE_SNAPSHOT)
        .map_err(|e| format!("Failed to create data directories: '{}'", e))?;
    let db = CoreDB::new(&snapshot_cfg, restore_filepath)
        .map_err(|e| format!("Error while initializing database: {}", e))?;
    let bgsave_handle = tokio::spawn(services::bgsave::bgsave_scheduler(
        db.clone(),
        bgsave_cfg,
        Terminator::new(signal.subscribe()),
    ));
    let snapshot_handle = tokio::spawn(services::snapshot::snapshot_service(
        db.clone(),
        snapshot_cfg,
        Terminator::new(signal.subscribe()),
    ));
    let mut server = match ports {
        PortConfig::InsecureOnly { host, port } => MultiListener::new_insecure_only(
            BaseListener::init(&db, host, port, climit.clone(), signal.clone())
                .await
                .map_err(|e| format!("Failed to bind to TCP port with error: {}", e))?,
        )?,
        PortConfig::SecureOnly { host, ssl } => MultiListener::new_secure_only(
            BaseListener::init(&db, host, ssl.port, climit.clone(), signal.clone())
                .await
                .map_err(|e| format!("Failed to initialize secure port with error: {}", e))?,
            ssl,
        )?,
        PortConfig::Multi { host, port, ssl } => {
            let secure_listener =
                BaseListener::init(&db, host, ssl.port, climit.clone(), signal.clone())
                    .await
                    .map_err(|e| format!("Failed to bind to TCP port with error: {}", e))?;
            let insecure_listener =
                BaseListener::init(&db, host, port, climit.clone(), signal.clone())
                    .await
                    .map_err(|e| format!("Failed to initialize secure port with error: {}", e))?;
            MultiListener::new_multi(secure_listener, insecure_listener, ssl).await?
        }
    };
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
    let _ = snapshot_handle.await;
    let _ = bgsave_handle.await;
    Ok(db)
}
