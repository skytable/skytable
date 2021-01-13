/*
 * Created on Tue Jul 21 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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
use crate::diskstore::snapshot::DIR_REMOTE_SNAPSHOT;
use crate::protocol::tls::SslConnection;
use crate::protocol::tls::SslListener;
use crate::protocol::{Connection, QueryResult::*};
use crate::resp::Writable;
use crate::CoreDB;
use libtdb::util::terminal;
use libtdb::TResult;
use std::fs;
use std::future::Future;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, Duration};
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
    /// Check if a shutdown signal was received
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

// We'll use the idea of gracefully shutting down from tokio

/// A listener
pub struct Listener {
    /// An atomic reference to the coretable
    db: CoreDB,
    /// The incoming connection listener (binding)
    listener: TcpListener,
    /// The maximum number of connections
    climit: Arc<Semaphore>,
    /// The shutdown broadcaster
    signal: broadcast::Sender<()>,
    // When all `Sender`s are dropped - the `Receiver` gets a `None` value
    // We send a clone of `terminate_tx` to each `CHandler`
    terminate_tx: mpsc::Sender<()>,
    terminate_rx: mpsc::Receiver<()>,
}

/// A per-connection handler
pub struct CHandler {
    db: CoreDB,
    con: Connection,
    climit: Arc<Semaphore>,
    terminator: Terminator,
    _term_sig_tx: mpsc::Sender<()>,
}

impl Listener {
    /// Accept an incoming connection
    async fn accept(&mut self) -> TResult<TcpStream> {
        // We will steal the idea of Ethernet's backoff for connection errors
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                // We don't need the bindaddr
                Ok((stream, _)) => return Ok(stream),
                Err(e) => {
                    if backoff > 64 {
                        // Too many retries, goodbye user
                        return Err(e.into());
                    }
                }
            }
            // Wait for the `backoff` duration
            time::sleep(Duration::from_secs(backoff)).await;
            // We're using exponential backoff
            backoff *= 2;
        }
    }
    /// Run the server
    pub async fn run(&mut self) -> TResult<()> {
        loop {
            // Take the permit first, but we won't use it right now
            // that's why we will forget it
            self.climit.acquire().await.unwrap().forget();
            let stream = self.accept().await?;
            let mut chandle = CHandler {
                db: self.db.clone(),
                con: Connection::new(stream),
                climit: self.climit.clone(),
                terminator: Terminator::new(self.signal.subscribe()),
                _term_sig_tx: self.terminate_tx.clone(),
            };
            tokio::spawn(async move {
                if let Err(e) = chandle.run().await {
                    log::error!("Error: {}", e);
                }
            });
        }
    }
}

/// # Connection Wrapper
///
/// A `Con` object holds a mutable reference to a standard TCP stream or to
/// an encrypted connection (over the `SslListener` object). It provides a few
/// methods which are provided by the underlying interface.
pub enum Con<'a> {
    /// A secure TLS connection
    Secure(&'a mut SslConnection),
    /// An insecure ('standard') TCP connection
    Insecure(&'a mut Connection),
}

impl<'a> Con<'a> {
    /// Create a new **unencrypted** connection instance
    pub fn init<'b>(con: &'b mut Connection) -> Self
    where
        'b: 'a,
    {
        Con::Insecure(con)
    }
    /// Create a new **encrypted** connection instance
    pub fn init_secure<'b>(con: &'b mut SslConnection) -> Self
    where
        'b: 'a,
    {
        Con::Secure(con)
    }
    /// Flush the stream that is held by the underlying connection
    pub async fn flush_stream(&mut self) -> TResult<()> {
        match self {
            Con::Secure(con) => con.flush_stream().await,
            Con::Insecure(con) => con.flush_stream().await,
        }
    }
    /// Write bytes to the underlying stream that implement the `Writable` trait
    pub async fn write_response(&mut self, resp: impl Writable) -> TResult<()> {
        match self {
            Con::Insecure(con) => con.write_response(resp).await,
            Con::Secure(con) => con.write_response(resp).await,
        }
    }
}

impl CHandler {
    /// Process the incoming connection
    pub async fn run(&mut self) -> TResult<()> {
        while !self.terminator.is_termination_signal() {
            let try_df = tokio::select! {
                tdf = self.con.read_query() => tdf,
                _ = self.terminator.receive_signal() => {
                    return Ok(());
                }
            };
            match try_df {
                Ok(Q(s)) => {
                    self.db
                        .execute_query(s, &mut Con::init(&mut self.con))
                        .await?
                }
                Ok(E(r)) => self.con.close_conn_with_error(r).await?,
                Ok(Empty) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

impl Drop for CHandler {
    fn drop(&mut self) {
        // Make sure that the permit is returned to the semaphore
        // in the case that there is a panic inside
        self.climit.add_permits(1);
    }
}
use std::io::{self, prelude::*};

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
                let (e1, e2) = futures::join!(insec, sec);
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
                    secure_listener.listener.local_addr().expect("Failed to g")
                )
            }
            MultiListener::InsecureOnly(insecure_listener) => {
                log::info!(
                    "Server started on tp://{}",
                    insecure_listener
                        .listener
                        .local_addr()
                        .expect("Failed to g")
                )
            }
            MultiListener::Multi(insecure_listener, secure_listener) => {
                log::info!(
                    "Listening to tp://{} and tps://{}",
                    insecure_listener
                        .listener
                        .local_addr()
                        .expect("Failed to g"),
                    secure_listener.listener.local_addr().expect("Failed to g")
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
    restore_filepath: Option<PathBuf>,
) {
    let (signal, _) = broadcast::channel(1);
    let (terminate_tx, terminate_rx) = mpsc::channel(1);
    let db = match CoreDB::new(bgsave_cfg, snapshot_cfg, restore_filepath) {
        Ok(d) => d,
        Err(e) => {
            log::error!("ERROR: {}", e);
            process::exit(0x100);
        }
    };
    match fs::create_dir_all(&*DIR_REMOTE_SNAPSHOT) {
        Ok(_) => (),
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => (),
            _ => {
                log::error!("Failed to create snapshot directories: '{}'", e);
                process::exit(0x100);
            }
        },
    }
    let climit = Arc::new(Semaphore::new(50000));
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
    if let Ok(_) = db.flush_db() {
        log::info!("Successfully saved data to disk");
        ()
    } else {
        log::error!("Failed to flush data to disk");
        loop {
            // Keep looping until we successfully write the in-memory table to disk
            log::warn!("Press enter to try again...");
            io::stdout().flush().unwrap();
            io::stdin().read(&mut [0]).unwrap();
            if let Ok(_) = db.flush_db() {
                log::info!("Successfully saved data to disk");
                break;
            } else {
                continue;
            }
        }
    }
    terminal::write_info("Goodbye :)\n").unwrap();
}

/// This is a **test only** function
/// This takes a `CoreDB` object so that keys can be modified externally by
/// the testing suite. This will **not save any data to disk**!
/// > **This is not for release builds in any way!**
#[cfg(test)]
pub async fn test_run(listener: TcpListener, db: CoreDB, sig: impl Future) {
    let (signal, _) = broadcast::channel(1);
    let (terminate_tx, terminate_rx) = mpsc::channel(1);
    let mut server = Listener {
        listener,
        db,
        climit: Arc::new(Semaphore::new(50000)),
        signal,
        terminate_tx,
        terminate_rx,
    };
    tokio::select! {
        _ = server.run() => {}
        _ = sig => {}
    }
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
