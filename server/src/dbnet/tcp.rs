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

pub use protocol::{ParseResult, Query};
use {
    super::NetBackoff,
    crate::{
        dbnet::{listener::BaseListener, BufferedSocketStream, Connection, ConnectionHandler},
        protocol::{self, interface::ProtocolSpec, Skyhash1, Skyhash2},
        IoResult,
    },
    std::marker::PhantomData,
    tokio::net::TcpStream,
};

impl BufferedSocketStream for TcpStream {}

pub type Listener = RawListener<Skyhash2>;
pub type ListenerV1 = RawListener<Skyhash1>;

/// A listener
pub struct RawListener<P> {
    pub base: BaseListener,
    _marker: PhantomData<P>,
}

impl<P: ProtocolSpec + 'static> RawListener<P> {
    pub fn new(base: BaseListener) -> Self {
        Self {
            base,
            _marker: PhantomData,
        }
    }
    /// Accept an incoming connection
    async fn accept(&mut self) -> IoResult<TcpStream> {
        let backoff = NetBackoff::new();
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
            let mut chandle = ConnectionHandler::<TcpStream, P>::new(
                self.base.db.clone(),
                Connection::new(stream),
                self.base.auth.clone(),
                self.base.climit.clone(),
                self.base.signal.subscribe(),
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
