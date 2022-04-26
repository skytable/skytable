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

use crate::dbnet::connection::ProtocolSpec;
use crate::protocol::Skyhash2;
use crate::{
    dbnet::{
        connection::{ConnectionHandler, ExecutorFn},
        BaseListener, Terminator,
    },
    protocol, IoResult,
};
use bytes::BytesMut;
use libsky::BUF_CAP;
pub use protocol::{ParseResult, Query};
use std::{cell::Cell, time::Duration};
use tokio::{
    io::{AsyncWrite, BufWriter},
    net::TcpStream,
    time,
};

pub trait BufferedSocketStream: AsyncWrite {}

impl BufferedSocketStream for TcpStream {}

type TcpExecutorFn<P> = ExecutorFn<P, Connection<TcpStream>, TcpStream>;

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

pub struct TcpBackoff {
    current: Cell<u8>,
}

impl TcpBackoff {
    const MAX_BACKOFF: u8 = 64;
    pub const fn new() -> Self {
        Self {
            current: Cell::new(1),
        }
    }
    pub async fn spin(&self) {
        // we can guarantee that this won't wrap around beyond u8::MAX because we always
        // check if we `should_disconnect` before sleeping and spinning
        time::sleep(Duration::from_secs(self.current.get() as u64)).await;
        self.current.set(self.current.get() << 1);
    }
    pub fn should_disconnect(&self) -> bool {
        self.current.get() > Self::MAX_BACKOFF
    }
}

pub type Listener = RawListener<Skyhash2>;

/// A listener
pub struct RawListener<P> {
    pub base: BaseListener,
    executor_fn: TcpExecutorFn<P>,
}

impl<P: ProtocolSpec + 'static> RawListener<P> {
    pub fn new(base: BaseListener) -> Self {
        Self {
            executor_fn: if base.auth.is_enabled() {
                ConnectionHandler::execute_unauth
            } else {
                ConnectionHandler::execute_auth
            },
            base,
        }
    }
    /// Accept an incoming connection
    async fn accept(&mut self) -> IoResult<TcpStream> {
        let backoff = TcpBackoff::new();
        loop {
            match self.base.listener.accept().await {
                // We don't need the bindaddr
                Ok((stream, _)) => return Ok(stream),
                Err(e) => {
                    if backoff.should_disconnect() {
                        // Too many retries, goodbye user
                        return Err(e);
                    }
                }
            }
            // spin to wait for the backoff duration
            backoff.spin().await;
        }
    }
    /// Run the server
    pub async fn run(&mut self) -> IoResult<()> {
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
            let mut chandle = ConnectionHandler::new(
                self.base.db.clone(),
                Connection::new(stream),
                self.base.auth.clone(),
                self.executor_fn,
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
