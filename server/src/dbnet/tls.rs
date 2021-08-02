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
use openssl::pkey::PKey;
use openssl::rsa::Rsa;
use openssl::ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod};
use std::fs;
use std::io::Error as IoError;
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
        tls_passfile: Option<String>,
    ) -> TResult<Self> {
        let mut acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        // cert is the same for both
        acceptor_builder.set_certificate_chain_file(chain_file)?;
        if let Some(tls_passfile) = tls_passfile {
            // first read in the private key
            let tls_private_key = fs::read(key_file).map_err(|e: IoError| {
                format!("Failed to read TLS private key file with error: {}", e)
            })?;
            // read the passphrase because the passphrase file stream was provided
            let tls_keyfile_stream = fs::read(tls_passfile).map_err(|e: IoError| {
                format!(
                    "Failed to read TLS private key passphrase file with error: {}",
                    e
                )
            })?;
            // decrypt the private key
            let pkey = Rsa::private_key_from_pem_passphrase(&tls_private_key, &tls_keyfile_stream)?;
            let pkey = PKey::from_rsa(pkey)?;
            // set the private key for the acceptor
            acceptor_builder.set_private_key(&pkey)?;
        } else {
            // no passphrase, needs interactive
            acceptor_builder.set_private_key_file(key_file, SslFiletype::PEM)?;
        }
        Ok(SslListener {
            base,
            acceptor: acceptor_builder.build(),
        })
    }
    async fn accept(&mut self) -> TResult<SslStream<TcpStream>> {
        let mut backoff = 1;
        loop {
            match self.base.listener.accept().await {
                // We don't need the bindaddr
                // We get the encrypted stream which we need to decrypt
                // by using the acceptor
                Ok((stream, _)) => {
                    let ssl = Ssl::new(self.acceptor.context())?;
                    let mut stream = SslStream::new(ssl, stream)?;
                    Pin::new(&mut stream).accept().await?;
                    return Ok(stream);
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
    pub async fn run(&mut self) -> TResult<()> {
        loop {
            // Take the permit first, but we won't use it right now
            // that's why we will forget it
            self.base.climit.acquire().await.unwrap().forget();
            let stream = self.accept().await?;
            let mut sslhandle = ConnectionHandler::new(
                self.base.db.clone(),
                Connection::new(stream),
                self.base.climit.clone(),
                Terminator::new(self.base.signal.subscribe()),
                self.base.terminate_tx.clone(),
            );
            tokio::spawn(async move {
                if let Err(e) = sslhandle.run().await {
                    log::error!("Error: {}", e);
                }
            });
        }
    }
}
