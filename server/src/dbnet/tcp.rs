/*
 * Created on Mon Apr 26 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use crate::dbnet::connection::ConnectionHandler;
use crate::dbnet::BaseListener;
use crate::dbnet::Terminator;
use crate::protocol;
use bytes::BytesMut;
use libsky::TResult;
use libsky::BUF_CAP;
pub use protocol::ParseResult;
pub use protocol::Query;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::time;

pub trait BufferedSocketStream: AsyncWrite {}

impl BufferedSocketStream for TcpStream {}

/// A TCP/SSL connection wrapper
pub struct Connection<T>
where
    T: BufferedSocketStream,
{
    /// The connection to the remote socket, wrapped in a buffer to speed
    /// up writing
    pub stream: BufWriter<T>,
    /// The in-memory read buffer. The size is given by `BUF_CAP`
    pub buffer: BytesMut,
}

impl<T> Connection<T>
where
    T: BufferedSocketStream,
{
    /// Initiailize a new `Connection` instance
    pub fn new(stream: T) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUF_CAP),
        }
    }
}

// We'll use the idea of gracefully shutting down from tokio

/// A listener
pub struct Listener {
    pub base: BaseListener,
}

impl Listener {
    /// Accept an incoming connection
    async fn accept(&mut self) -> TResult<TcpStream> {
        // We will steal the idea of Ethernet's backoff for connection errors
        let mut backoff = 1;
        loop {
            match self.base.listener.accept().await {
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
            self.base.climit.acquire().await.unwrap().forget();
            let stream = self.accept().await?;
            let mut chandle = ConnectionHandler::new(
                self.base.db.clone(),
                Connection::new(stream),
                self.base.climit.clone(),
                Terminator::new(self.base.signal.subscribe()),
                self.base.terminate_tx.clone(),
            );
            tokio::spawn(async move {
                if let Err(e) = chandle.run().await {
                    log::error!("Error: {}", e);
                }
            });
        }
    }
}
