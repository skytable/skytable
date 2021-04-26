/*
 * Created on Thu Jul 30 2020
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

//! # The `protocol` module
//!
//! This module provides low-level interfaces to read data from a socket, when control
//! is handed over to it by `dbnet`, and high-level interfaces for parsing an incoming
//! query into an _executable query_ via the `deserializer` module.

mod deserializer;
pub mod responses;
use crate::dbnet::Terminator;
use crate::protocol::con::ConnectionHandler;
use crate::CoreDB;
use bytes::BytesMut;
pub use deserializer::ActionGroup;
pub use deserializer::ParseResult;
pub use deserializer::Query;
use libsky::TResult;
use libsky::BUF_CAP;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
pub mod con;
pub mod tls;

/// A TCP connection wrapper
pub struct Connection {
    /// The connection to the remote socket, wrapped in a buffer to speed
    /// up writing
    stream: BufWriter<TcpStream>,
    /// The in-memory read buffer. The size is given by `BUF_CAP`
    buffer: BytesMut,
}

/// The outcome of running `Connection`'s `try_query` function
pub enum QueryResult {
    /// A parsed `Query` object
    Q(Query),
    /// An error response
    E(Vec<u8>),
    /// A closed connection
    Empty,
}

impl Connection {
    /// Initiailize a new `Connection` instance
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUF_CAP),
        }
    }
}

// We'll use the idea of gracefully shutting down from tokio

/// A listener
pub struct Listener {
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
            let mut chandle = ConnectionHandler::new(
                self.db.clone(),
                Connection::new(stream),
                self.climit.clone(),
                Terminator::new(self.signal.subscribe()),
                self.terminate_tx.clone(),
            );
            tokio::spawn(async move {
                if let Err(e) = chandle.run().await {
                    log::error!("Error: {}", e);
                }
            });
        }
    }
}
