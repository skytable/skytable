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
use crate::dbnet::Con;
use crate::dbnet::Terminator;
use crate::protocol::QueryResult::*;
use crate::resp::Writable;
use crate::CoreDB;
use bytes::{Buf, BytesMut};
pub use deserializer::ActionGroup;
pub use deserializer::ParseResult;
pub use deserializer::Query;
use libsky::TResult;
use libsky::BUF_CAP;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
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
    /// Read a query from the remote end
    ///
    /// This function asynchronously waits until all the data required
    /// for parsing the query is available
    pub async fn read_query(&mut self) -> Result<QueryResult, String> {
        self.read_again().await?;
        loop {
            match self.try_query() {
                Ok(ParseResult::Query(query, forward)) => {
                    self.buffer.advance(forward);
                    return Ok(QueryResult::Q(query));
                }
                Ok(ParseResult::BadPacket) => {
                    self.buffer.clear();
                    return Ok(QueryResult::E(responses::fresp::R_PACKET_ERR.to_owned()));
                }
                Err(_) => {
                    return Ok(QueryResult::Empty);
                }
                _ => (),
            }
            self.read_again().await?;
        }
    }
    /// Try to parse a query from the buffered data
    fn try_query(&mut self) -> Result<ParseResult, ()> {
        if self.buffer.is_empty() {
            return Err(());
        }
        Ok(deserializer::parse(&self.buffer))
    }
    /// Try to fill the buffer again
    async fn read_again(&mut self) -> Result<(), String> {
        match self.stream.read_buf(&mut self.buffer).await {
            Ok(0) => {
                // If 0 bytes were received, then the remote end closed
                // the connection
                if self.buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err(format!(
                        "Connection reset while reading from {}",
                        if let Ok(p) = self.get_peer() {
                            p.to_string()
                        } else {
                            "peer".to_owned()
                        }
                    )
                    .into());
                }
            }
            Ok(_) => Ok(()),
            Err(e) => return Err(format!("{}", e)),
        }
    }
    /// Get the peer address
    fn get_peer(&self) -> IoResult<SocketAddr> {
        self.stream.get_ref().peer_addr()
    }
    /// Write a response to the stream
    pub async fn write_response(&mut self, streamer: impl Writable) -> TResult<()> {
        streamer.write(&mut self.stream).await?;
        Ok(())
    }
    pub async fn flush_stream(&mut self) -> TResult<()> {
        self.stream.flush().await?;
        Ok(())
    }
    /// Wraps around the `write_response` used to differentiate between a
    /// success response and an error response
    pub async fn close_conn_with_error(&mut self, resp: Vec<u8>) -> TResult<()> {
        self.write_response(resp).await?;
        self.stream.flush().await?;
        Ok(())
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
