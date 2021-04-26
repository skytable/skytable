/*
 * Created on Fri Dec 18 2020
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

use crate::dbnet::Terminator;
use crate::protocol::ConnectionHandler;
use crate::CoreDB;
use bytes::BytesMut;
use libsky::TResult;
use libsky::BUF_CAP;
use openssl::ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::BufWriter;
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
            let mut sslhandle = ConnectionHandler::new(
                self.db.clone(),
                SslConnection::new(stream),
                self.climit.clone(),
                Terminator::new(self.signal.subscribe()),
                self.terminate_tx.clone(),
            );
            tokio::spawn(async move {
                log::debug!("Spawned listener task");
                if let Err(e) = sslhandle.run().await {
                    log::error!("Error: {}", e);
                }
            });
        }
    }
}

pub struct SslConnection {
    pub stream: BufWriter<SslStream<TcpStream>>,
    pub buffer: BytesMut,
}

impl SslConnection {
    pub fn new(stream: SslStream<TcpStream>) -> Self {
        SslConnection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUF_CAP),
        }
    }
}
