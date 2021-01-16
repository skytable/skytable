/*
 * Created on Fri Dec 18 2020
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

use super::deserializer;
use super::responses;
use super::IoResult;
use super::ParseResult;
use super::QueryResult;
use crate::dbnet::Con;
use crate::dbnet::Terminator;
use crate::resp::Writable;
use crate::CoreDB;
use bytes::Buf;
use bytes::BytesMut;
use libtdb::TResult;
use libtdb::BUF_CAP;
use openssl::ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::BufWriter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, Duration};
use tokio_openssl::SslStream;

pub struct SslListener {
    /// An atomic reference to the coretable
    pub db: CoreDB,
    /// The incoming connection listener (binding)
    pub listener: TcpListener,
    /// The maximum number of connections
    climit: Arc<Semaphore>,
    /// The shutdown broadcaster
    pub signal: broadcast::Sender<()>,
    // When all `Sender`s are dropped - the `Receiver` gets a `None` value
    // We send a clone of `terminate_tx` to each `CHandler`
    pub terminate_tx: mpsc::Sender<()>,
    pub terminate_rx: mpsc::Receiver<()>,
    acceptor: SslAcceptor,
}

impl SslListener {
    pub fn new_pem_based_ssl_connection(
        key_file: String,
        chain_file: String,
        db: CoreDB,
        listener: TcpListener,
        climit: Arc<Semaphore>,
        signal: broadcast::Sender<()>,
        terminate_tx: mpsc::Sender<()>,
        terminate_rx: mpsc::Receiver<()>,
    ) -> TResult<Self> {
        log::debug!("New SSL/TLS connection registered");
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        acceptor.set_private_key_file(key_file, SslFiletype::PEM)?;
        acceptor.set_certificate_chain_file(chain_file)?;
        let acceptor = acceptor.build();
        Ok(SslListener {
            db,
            listener,
            climit,
            signal,
            terminate_tx,
            terminate_rx,
            acceptor,
        })
    }
    async fn accept(&mut self) -> TResult<SslStream<TcpStream>> {
        log::debug!("Trying to accept a SSL connection");
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                // We don't need the bindaddr
                // We get the encrypted stream which we need to decrypt
                // by using the acceptor
                Ok((stream, _)) => {
                    log::debug!("Accepted an SSL/TLS connection");
                    let ssl = Ssl::new(self.acceptor.context())?;
                    let mut stream = SslStream::new(ssl, stream)?;
                    Pin::new(&mut stream).accept().await?;
                    log::debug!("Connected to secure socket over TCP");
                    return Ok(stream);
                }
                Err(e) => {
                    log::debug!("Failed to establish a secure connection");
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
    pub async fn run(&mut self) -> TResult<()> {
        log::debug!("Started secure server");
        loop {
            // Take the permit first, but we won't use it right now
            // that's why we will forget it
            self.climit.acquire().await.unwrap().forget();
            let stream = self.accept().await?;
            let mut sslhandle = SslConnectionHandler {
                db: self.db.clone(),
                con: SslConnection::new(stream),
                climit: self.climit.clone(),
                terminator: Terminator::new(self.signal.subscribe()),
                _term_sig_tx: self.terminate_tx.clone(),
            };
            tokio::spawn(async move {
                log::debug!("Spawned listener task");
                if let Err(e) = sslhandle.run().await {
                    log::error!("Error: {}", e);
                }
            });
        }
    }
}

pub struct SslConnectionHandler {
    db: CoreDB,
    con: SslConnection,
    climit: Arc<Semaphore>,
    terminator: Terminator,
    _term_sig_tx: mpsc::Sender<()>,
}

impl SslConnectionHandler {
    pub async fn run(&mut self) -> TResult<()> {
        log::debug!("SslConnectionHanler initialized to handle a remote client");
        while !self.terminator.is_termination_signal() {
            let try_df = tokio::select! {
                tdf = self.con.read_query() => tdf,
                _ = self.terminator.receive_signal() => {
                    return Ok(());
                }
            };
            match try_df {
                Ok(QueryResult::Q(s)) => {
                    self.db
                        .execute_query(s, &mut Con::init_secure(&mut self.con))
                        .await?
                }
                Ok(QueryResult::E(r)) => {
                    log::debug!("Failed to read query!");
                    self.con.close_conn_with_error(r).await?
                }
                Ok(QueryResult::Empty) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}
impl Drop for SslConnectionHandler {
    fn drop(&mut self) {
        // Make sure that the permit is returned to the semaphore
        // in the case that there is a panic inside
        self.climit.add_permits(1);
    }
}

pub struct SslConnection {
    stream: BufWriter<SslStream<TcpStream>>,
    buffer: BytesMut,
}

impl SslConnection {
    pub fn new(stream: SslStream<TcpStream>) -> Self {
        SslConnection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUF_CAP),
        }
    }
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
    fn get_peer(&self) -> IoResult<SocketAddr> {
        self.stream.get_ref().get_ref().peer_addr()
    }
    /// Try to parse a query from the buffered data
    fn try_query(&mut self) -> Result<ParseResult, ()> {
        if self.buffer.is_empty() {
            return Err(());
        }
        Ok(deserializer::parse(&self.buffer))
    }
    pub async fn read_query(&mut self) -> Result<QueryResult, String> {
        self.read_again().await?;
        loop {
            match self.try_query() {
                Ok(ParseResult::Query(query, forward)) => {
                    self.buffer.advance(forward);
                    return Ok(QueryResult::Q(query));
                }
                Ok(ParseResult::BadPacket(bp)) => {
                    self.buffer.advance(bp);
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
