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

use crate::config::BGSave;
use crate::protocol::{Connection, QueryResult::*};
use crate::CoreDB;
use libtdb::util::terminal;
use libtdb::TResult;
use std::future::Future;
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
struct CHandler {
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
            time::delay_for(Duration::from_secs(backoff)).await;
            // We're using exponential backoff
            backoff *= 2;
        }
    }
    /// Run the server
    pub async fn run(&mut self) -> TResult<()> {
        loop {
            // Take the permit first, but we won't use it right now
            // that's why we will forget it
            self.climit.acquire().await.forget();
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
                    eprintln!("Error: {}", e);
                }
            });
        }
    }
}

impl CHandler {
    /// Process the incoming connection
    async fn run(&mut self) -> TResult<()> {
        while !self.terminator.is_termination_signal() {
            let try_df = tokio::select! {
                tdf = self.con.read_query() => tdf,
                _ = self.terminator.receive_signal() => {
                    return Ok(());
                }
            };
            match try_df {
                Ok(Q(s)) => self.db.execute_query(s, &mut self.con).await?,
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

/// Start the server waiting for incoming connections or a CTRL+C signal
pub async fn run(listener: TcpListener, bgsave_cfg: BGSave, sig: impl Future) {
    let (signal, _) = broadcast::channel(1);
    let (terminate_tx, terminate_rx) = mpsc::channel(1);
    let db = match CoreDB::new(bgsave_cfg) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("ERROR: {}", e);
            process::exit(0x100);
        }
    };
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
        _ = sig => {
            log::info!("Signalling all workers to shut down");
        }
    }
    let Listener {
        mut terminate_rx,
        terminate_tx,
        signal,
        db,
        ..
    } = server;
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
    drop(signal);
    drop(terminate_tx);
    let _ = terminate_rx.recv().await;
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
