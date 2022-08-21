/*
 * Created on Sun Aug 21 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use {
    self::connection::Connection,
    crate::{
        actions::{ActionError, ActionResult},
        auth::AuthProvider,
        corestore::Corestore,
        protocol::{interface::ProtocolSpec, Query},
        util::compiler,
        IoResult,
    },
    bytes::Buf,
    std::{cell::Cell, sync::Arc, time::Duration},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{
            broadcast::{self},
            mpsc::{self},
            Semaphore,
        },
        time,
    },
};

pub type QueryWithAdvance = (Query, usize);
pub const MAXIMUM_CONNECTION_LIMIT: usize = 50000;
use crate::queryengine;

pub use self::listener::connect;

mod connection;
#[macro_use]
mod macros;
mod listener;
pub mod prelude;
mod tcp;
mod tls;

/// This is a "marker trait" that ensures that no silly types are
/// passed into the [`Connection`] type
pub trait BufferedSocketStream: AsyncWriteExt + AsyncReadExt + Unpin {}

/// Result of [`Connection::read_query`]
enum QueryResult {
    /// A [`Query`] read to be run
    Q(QueryWithAdvance),
    /// Simply proceed to the next run loop iter
    NextLoop,
    /// The client disconnected
    Disconnected,
}

/// A backoff implementation that is meant to be used in connection loops
pub(self) struct NetBackoff {
    c: Cell<u8>,
}

impl NetBackoff {
    /// The maximum backoff duration
    const MAX_BACKOFF: u8 = 64;
    /// Create a new [`NetBackoff`] instance
    pub const fn new() -> Self {
        Self { c: Cell::new(1) }
    }
    /// Wait for the current backoff duration
    pub async fn spin(&self) {
        time::sleep(Duration::from_secs(self.c.get() as _)).await;
        self.c.set(self.c.get() << 1);
    }
    /// Should we disconnect the stream?
    pub fn should_disconnect(&self) -> bool {
        self.c.get() > Self::MAX_BACKOFF
    }
}

pub struct AuthProviderHandle {
    /// the source authentication provider
    provider: AuthProvider,
    /// authenticated
    auth_good: bool,
}

impl AuthProviderHandle {
    pub fn new(provider: AuthProvider) -> Self {
        let auth_good = !provider.is_enabled();
        Self {
            provider,
            auth_good,
        }
    }
    /// This returns `true` if:
    /// 1. Authn is disabled
    /// 2. The connection is authenticated
    pub const fn authenticated(&self) -> bool {
        self.auth_good
    }
    pub fn set_auth(&mut self) {
        self.auth_good = true;
    }
    pub fn set_unauth(&mut self) {
        self.auth_good = false;
    }
    pub fn provider_mut(&mut self) -> &mut AuthProvider {
        &mut self.provider
    }
    pub fn provider(&self) -> &AuthProvider {
        &self.provider
    }
}

/// A generic connection handler. You have two choices:
/// 1. Choose the connection kind
/// 2. Choose the protocol implementation
pub struct ConnectionHandler<C, P> {
    /// an atomic reference to the shared in-memory engine
    db: Corestore,
    /// the connection
    con: Connection<C, P>,
    /// the semaphore used to impose limits on number of connections
    climit: Arc<Semaphore>,
    /// the authentication handle
    auth: AuthProviderHandle,
    /// check for termination signals
    termination_signal: broadcast::Receiver<()>,
    /// the sender that we drop when we're done with handling a connection (used for gracefule exit)
    _term_sig_tx: mpsc::Sender<()>,
}

impl<C, P> ConnectionHandler<C, P>
where
    C: BufferedSocketStream,
    P: ProtocolSpec,
{
    /// Create a new connection handler
    pub fn new(
        db: Corestore,
        con: Connection<C, P>,
        auth_data: AuthProvider,
        climit: Arc<Semaphore>,
        termination_signal: broadcast::Receiver<()>,
        _term_sig_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            db,
            con,
            climit,
            auth: AuthProviderHandle::new(auth_data),
            termination_signal,
            _term_sig_tx,
        }
    }
    pub async fn run(&mut self) -> IoResult<()> {
        loop {
            let packet = tokio::select! {
                pkt = self.con.read_query() => pkt,
                _ = self.termination_signal.recv() => {
                    return Ok(());
                }
            };
            match packet {
                Ok(QueryResult::Q((query, advance))) => {
                    // the mutable reference to self ensures that the buffer is not modified
                    // hence ensuring that the pointers will remain valid
                    #[cfg(debug_assertions)]
                    let len_at_start = self.con.buffer.len();
                    #[cfg(debug_assertions)]
                    let sptr_at_start = self.con.buffer.as_ptr() as usize;
                    #[cfg(debug_assertions)]
                    let eptr_at_start = sptr_at_start + len_at_start;
                    {
                        // The actual execution (the assertions are just debug build sanity checks)
                        match self.execute_query(query).await {
                            Ok(()) => {}
                            Err(ActionError::ActionError(e)) => self.con.write_error(e).await?,
                            Err(ActionError::IoError(e)) => return Err(e),
                        }
                    }
                    {
                        // do these assertions to ensure memory safety (this is just for sanity sake)
                        #[cfg(debug_assertions)]
                        // len should be unchanged. no functions should **ever** touch the buffer
                        debug_assert_eq!(self.con.buffer.len(), len_at_start);
                        #[cfg(debug_assertions)]
                        // start of allocation should be unchanged
                        debug_assert_eq!(self.con.buffer.as_ptr() as usize, sptr_at_start);
                        #[cfg(debug_assertions)]
                        // end of allocation should be unchanged. else we're entirely violating
                        // memory safety guarantees
                        debug_assert_eq!(
                            unsafe {
                                // UNSAFE(@ohsayan): THis is always okay
                                self.con.buffer.as_ptr().add(len_at_start)
                            } as usize,
                            eptr_at_start
                        );
                        // this is only when we clear the buffer. since execute_query is not called
                        // at this point, it's totally fine (so invalidating ptrs is totally cool)
                        self.con.buffer.advance(advance);
                    }
                }
                Ok(QueryResult::Disconnected) => return Ok(()),
                Ok(QueryResult::NextLoop) => {}
                Err(e) => return Err(e),
            }
        }
    }
    async fn execute_query(&mut self, query: Query) -> ActionResult<()> {
        let Self { db, con, auth, .. } = self;
        match query {
            Query::Simple(q) => {
                con.write_simple_query_header().await?;
                if compiler::likely(auth.authenticated()) {
                    queryengine::execute_simple(db, con, auth, q).await?;
                } else {
                    queryengine::execute_simple_noauth(db, con, auth, q).await?;
                }
            }
            Query::Pipelined(p) => {
                if compiler::likely(auth.authenticated()) {
                    con.write_pipelined_query_header(p.len()).await?;
                    queryengine::execute_pipeline(db, con, auth, p).await?;
                } else {
                    con.write_simple_query_header().await?;
                    con.write_error(P::AUTH_CODE_BAD_CREDENTIALS).await?;
                }
            }
        }
        con.stream.flush().await?;
        Ok(())
    }
}

impl<C, T> Drop for ConnectionHandler<C, T> {
    fn drop(&mut self) {
        // Make sure that the permit is returned to the semaphore
        // in the case that there is a panic inside
        self.climit.add_permits(1);
    }
}
