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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use super::connection::ConnectionHandler;
use crate::dbnet::tcp::BufferedSocketStream;
use crate::dbnet::tcp::Connection;
use crate::dbnet::BaseListener;
use crate::dbnet::Terminator;
use libsky::TResult;
use openssl::ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod};
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::time::{self, Duration};
use tokio_openssl::SslStream;

impl BufferedSocketStream for SslStream<TcpStream> {}

pub struct SslListener {
    pub base: BaseListener,
    acceptor: SslAcceptor,
}

impl SslListener {
    pub fn new_pem_based_ssl_connection(
        key_file: String,
        chain_file: String,
        base: BaseListener,
    ) -> TResult<Self> {
        log::debug!("New SSL/TLS connection registered");
        let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        acceptor.set_private_key_file(key_file, SslFiletype::PEM)?;
        acceptor.set_certificate_chain_file(chain_file)?;
        let acceptor = acceptor.build();
        Ok(SslListener { base, acceptor })
    }
    async fn accept(&mut self) -> TResult<SslStream<TcpStream>> {
        log::debug!("Trying to accept a SSL connection");
        let mut backoff = 1;
        loop {
            match self.base.listener.accept().await {
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
            self.base.climit.acquire().await.unwrap().forget();
            /*
             SECURITY: Ignore any errors that may arise in the accept
             loop. If we apply the try operator here, we will immediately
             terminate the run loop causing the entire server to go down.
             Also, do not log any errors because many connection errors
             can arise and it will flood the log and might also result
             in a crash
            */
            let stream = skip_loop_err!(self.accept().await);
            let mut sslhandle = ConnectionHandler::new(
                self.base.db.clone(),
                Connection::new(stream),
                self.base.climit.clone(),
                Terminator::new(self.base.signal.subscribe()),
                self.base.terminate_tx.clone(),
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
