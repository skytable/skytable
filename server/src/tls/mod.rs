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

#[allow(dead_code)] // TODO: Don't keep clippy quiet!
// use crate::dbnet::CHandler;
// use crate::dbnet::Terminator;
// use crate::protocol::Connection;
use crate::CoreDB;
use libtdb::TResult;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
// use std::net::ToSocketAddrs;
use std::sync::Arc;
// use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, Duration};
use tokio_openssl_vendored::SslStream;
pub struct SslListener {
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
    acceptor: Arc<SslAcceptor>,
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
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        acceptor.set_private_key_file(key_file, SslFiletype::PEM)?;
        acceptor.set_certificate_chain_file(chain_file)?;
        let acceptor = Arc::new(acceptor.build());
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
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                // We don't need the bindaddr
                // We get the encrypted stream which we need to decrypt
                // by using the acceptor
                Ok((encrypted_stream, _)) => {
                    let decrypted_stream =
                        tokio_openssl_vendored::accept(&self.acceptor, encrypted_stream).await?;
                    return Ok(decrypted_stream);
                }
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
}
