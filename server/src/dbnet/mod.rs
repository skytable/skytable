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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
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
use crate::CoreDB;
use libsky::TResult;
use std::fs;
use std::future::Future;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::process;
use std::sync::Arc;
use tls::SslListener;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc};
pub mod connection;
mod tls;

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
    pub async fn new_insecure_only(
        host: IpAddr,
        port: u16,
        climit: Arc<Semaphore>,
        db: CoreDB,
        signal: broadcast::Sender<()>,
        terminate_tx: mpsc::Sender<()>,
        terminate_rx: mpsc::Receiver<()>,
    ) -> Self {
        let listener = TcpListener::bind((host, port))
            .await
            .expect("Failed to bind to port");
        MultiListener::InsecureOnly(Listener {
            listener,
            db,
            climit,
            signal,
            terminate_tx,
            terminate_rx,
        })
    }
    /// Create a new `SecureOnly` listener
    pub async fn new_secure_only(
        host: IpAddr,
        climit: Arc<Semaphore>,
        db: CoreDB,
        signal: broadcast::Sender<()>,
        terminate_tx: mpsc::Sender<()>,
        terminate_rx: mpsc::Receiver<()>,
        ssl: SslOpts,
    ) -> Self {
        let listener = TcpListener::bind((host, ssl.port))
            .await
            .expect("Failed to bind to port");
        MultiListener::SecureOnly(
            SslListener::new_pem_based_ssl_connection(
                ssl.key,
                ssl.chain,
                db,
                listener,
                climit,
                signal,
                terminate_tx,
                terminate_rx,
            )
            .expect("Couldn't bind to secure port"),
        )
    }
    /// Create a new `Multi` listener that has both a secure and an insecure listener
    pub async fn new_multi(
        host: IpAddr,
        port: u16,
        climit: Arc<Semaphore>,
        db: CoreDB,
        signal: broadcast::Sender<()>,
        terminate_tx: mpsc::Sender<()>,
        terminate_rx: mpsc::Receiver<()>,
        ssl_terminate_tx: mpsc::Sender<()>,
        ssl_terminate_rx: mpsc::Receiver<()>,
        ssl: SslOpts,
    ) -> Self {
        let listener = TcpListener::bind((host, ssl.port))
            .await
            .expect("Failed to bind to port");
        let secure_listener = SslListener::new_pem_based_ssl_connection(
            ssl.key,
            ssl.chain,
            db.clone(),
            listener,
            climit.clone(),
            signal.clone(),
            ssl_terminate_tx,
            ssl_terminate_rx,
        )
        .expect("Couldn't bind to secure port");
        let listener = TcpListener::bind((host, port))
            .await
            .expect("Failed to bind to port");
        let insecure_listener = Listener {
            listener,
            db,
            climit,
            signal,
            terminate_tx,
            terminate_rx,
        };
        MultiListener::Multi(insecure_listener, secure_listener)
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
    /// Print the port binding status
    pub fn print_binding(&self) {
        match self {
            MultiListener::SecureOnly(secure_listener) => {
                log::info!(
                    "Server started on tps://{}",
                    secure_listener
                        .listener
                        .local_addr()
                        .expect("Failed to get bind address")
                )
            }
            MultiListener::InsecureOnly(insecure_listener) => {
                log::info!(
                    "Server started on skyhash://{}",
                    insecure_listener
                        .listener
                        .local_addr()
                        .expect("Failed to get bind address")
                )
            }
            MultiListener::Multi(insecure_listener, secure_listener) => {
                log::info!(
                    "Listening to skyhash://{} and tps://{}",
                    insecure_listener
                        .listener
                        .local_addr()
                        .expect("Failed to get bind address"),
                    secure_listener
                        .listener
                        .local_addr()
                        .expect("Failed to get bind address")
                )
            }
        }
    }
    /// Signal the ports to shut down and only return after they have shut down
    ///
    /// **Do note:** This function doesn't flush the `CoreDB` object! The **caller has to
    /// make sure that the data is saved!**
    pub async fn finish_with_termsig(self) {
        match self {
            MultiListener::InsecureOnly(server) => {
                let Listener {
                    mut terminate_rx,
                    terminate_tx,
                    signal,
                    ..
                } = server;
                drop(signal);
                drop(terminate_tx);
                let _ = terminate_rx.recv().await;
            }
            MultiListener::SecureOnly(server) => {
                let SslListener {
                    mut terminate_rx,
                    terminate_tx,
                    signal,
                    ..
                } = server;
                drop(signal);
                drop(terminate_tx);
                let _ = terminate_rx.recv().await;
            }
            MultiListener::Multi(insecure, secure) => {
                let Listener {
                    mut terminate_rx,
                    terminate_tx,
                    signal,
                    ..
                } = insecure;
                drop((signal, terminate_tx));
                let _ = terminate_rx.recv().await;
                let SslListener {
                    mut terminate_rx,
                    terminate_tx,
                    signal,
                    ..
                } = secure;
                drop((signal, terminate_tx));
                let _ = terminate_rx.recv().await;
            }
        }
    }
}

/// Start the server waiting for incoming connections or a CTRL+C signal
pub async fn run(
    ports: PortConfig,
    bgsave_cfg: BGSave,
    snapshot_cfg: SnapshotConfig,
    sig: impl Future,
    restore_filepath: Option<String>,
) -> CoreDB {
    let (signal, _) = broadcast::channel(1);
    let (terminate_tx, terminate_rx) = mpsc::channel(1);
    match fs::create_dir_all(&*DIR_REMOTE_SNAPSHOT) {
        Ok(_) => (),
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => (),
            _ => {
                log::error!("Failed to create data directories: '{}'", e);
                process::exit(0x100);
            }
        },
    }
    let db = match CoreDB::new(&snapshot_cfg, restore_filepath) {
        Ok(db) => db,
        Err(e) => {
            log::error!("ERROR: {}", e);
            process::exit(0x100);
        }
    };
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
    let climit = Arc::new(Semaphore::const_new(50000));
    let mut server = match ports {
        PortConfig::InsecureOnly { host, port } => {
            MultiListener::new_insecure_only(
                host,
                port,
                climit.clone(),
                db.clone(),
                signal,
                terminate_tx,
                terminate_rx,
            )
            .await
        }
        PortConfig::SecureOnly { host, ssl } => {
            MultiListener::new_secure_only(
                host,
                climit.clone(),
                db.clone(),
                signal,
                terminate_tx,
                terminate_rx,
                ssl,
            )
            .await
        }
        PortConfig::Multi { host, port, ssl } => {
            let (ssl_terminate_tx, ssl_terminate_rx) = mpsc::channel::<()>(1);
            let server = MultiListener::new_multi(
                host,
                port,
                climit,
                db.clone(),
                signal,
                terminate_tx,
                terminate_rx,
                ssl_terminate_tx,
                ssl_terminate_rx,
                ssl,
            )
            .await;
            server
        }
    };
    server.print_binding();
    tokio::select! {
        _ = server.run_server() => {}
        _ = sig => {
            log::info!("Signalling all workers to shut down");
        }
    }
    server.finish_with_termsig().await;
    let _ = snapshot_handle.await;
    let _ = bgsave_handle.await;
    db
}
