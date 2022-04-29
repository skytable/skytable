/*
 * Created on Sun Apr 25 2021
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

//! # Generic connection traits
//! The `con` module defines the generic connection traits `RawConnection` and `ProtocolRead`.
//! These two traits can be used to interface with sockets that are used for communication through the Skyhash
//! protocol.
//!
//! The `RawConnection` trait provides a basic set of methods that are required by prospective connection
//! objects to be eligible for higher level protocol interactions (such as interactions with high-level query objects).
//! Once a type implements this trait, it automatically gets a free `ProtocolRead` implementation. This immediately
//! enables this connection object/type to use methods like read_query enabling it to read and interact with queries and write
//! respones in compliance with the Skyhash protocol.

use crate::{
    actions::{ActionError, ActionResult},
    auth::{self, AuthProvider},
    corestore::Corestore,
    dbnet::{
        connection::prelude::FutureResult,
        tcp::{BufferedSocketStream, Connection},
        Terminator,
    },
    protocol::{
        interface::{ProtocolRead, ProtocolSpec},
        responses, Query,
    },
    queryengine, IoResult,
};
use bytes::{Buf, BytesMut};
use std::{marker::PhantomData, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::{mpsc, Semaphore},
};

pub type QueryWithAdvance = (Query, usize);

pub enum QueryResult {
    Q(QueryWithAdvance),
    E(&'static [u8]),
    Wrongtype,
    Disconnected,
}

pub struct AuthProviderHandle<'a, P: ProtocolSpec, T, Strm> {
    provider: &'a mut AuthProvider,
    executor: &'a mut ExecutorFn<P, T, Strm>,
    _phantom: PhantomData<(T, Strm)>,
}

impl<'a, P, T, Strm> AuthProviderHandle<'a, P, T, Strm>
where
    T: ClientConnection<P, Strm>,
    Strm: Stream,
    P: ProtocolSpec,
{
    pub fn new(provider: &'a mut AuthProvider, executor: &'a mut ExecutorFn<P, T, Strm>) -> Self {
        Self {
            provider,
            executor,
            _phantom: PhantomData,
        }
    }
    pub fn provider_mut(&mut self) -> &mut AuthProvider {
        self.provider
    }
    pub fn provider(&self) -> &AuthProvider {
        self.provider
    }
    pub fn swap_executor_to_anonymous(&mut self) {
        *self.executor = ConnectionHandler::execute_unauth;
    }
    pub fn swap_executor_to_authenticated(&mut self) {
        *self.executor = ConnectionHandler::execute_auth;
    }
}

pub mod prelude {
    //! A 'prelude' for callers that would like to use the `RawConnection` and `ProtocolRead` traits
    //!
    //! This module is hollow itself, it only re-exports from `dbnet::con` and `tokio::io`
    pub use super::{AuthProviderHandle, ClientConnection, Stream};
    pub use crate::{
        actions::{ensure_boolean_or_aerr, ensure_cond_or_err, ensure_length},
        aerr, conwrite,
        corestore::{
            table::{KVEBlob, KVEList},
            Corestore,
        },
        get_tbl, handle_entity, is_lowbit_set,
        protocol::{
            interface::{ProtocolRead, ProtocolSpec},
            responses::{self, groups},
        },
        queryengine::ActionIter,
        registry,
        resp::StringWrapper,
        util::{self, FutureResult, UnwrapActionError, Unwrappable},
    };
    pub use tokio::io::{AsyncReadExt, AsyncWriteExt};
}

/// # The `RawConnection` trait
///
/// The `RawConnection` trait has low-level methods that can be used to interface with raw sockets. Any type
/// that successfully implements this trait will get an implementation for `ProtocolRead` which augments and
/// builds on these fundamental methods to provide high-level interfacing with queries.
///
/// ## Example of a `RawConnection` object
/// Ideally a `RawConnection` object should look like (the generic parameter just exists for doc-tests, just think that
/// there is a type `Strm`):
/// ```no_run
/// struct Connection<Strm> {
///     pub buffer: bytes::BytesMut,
///     pub stream: Strm,
/// }
/// ```
///
/// `Strm` should be a stream, i.e something like an SSL connection/TCP connection.
pub trait RawConnection<P: ProtocolSpec, Strm>: Send + Sync {
    /// Returns an **immutable** reference to the underlying read buffer
    fn get_buffer(&self) -> &BytesMut;
    /// Returns an **immutable** reference to the underlying stream
    fn get_stream(&self) -> &BufWriter<Strm>;
    /// Returns a **mutable** reference to the underlying read buffer
    fn get_mut_buffer(&mut self) -> &mut BytesMut;
    /// Returns a **mutable** reference to the underlying stream
    fn get_mut_stream(&mut self) -> &mut BufWriter<Strm>;
    /// Returns a **mutable** reference to (buffer, stream)
    ///
    /// This is to avoid double mutable reference errors
    fn get_mut_both(&mut self) -> (&mut BytesMut, &mut BufWriter<Strm>);
    /// Advance the read buffer by `forward_by` positions
    fn advance_buffer(&mut self, forward_by: usize) {
        self.get_mut_buffer().advance(forward_by)
    }
    /// Clear the internal buffer completely
    fn clear_buffer(&mut self) {
        self.get_mut_buffer().clear()
    }
}

impl<T, P> RawConnection<P, T> for Connection<T>
where
    T: BufferedSocketStream + Sync + Send,
    P: ProtocolSpec,
{
    fn get_buffer(&self) -> &BytesMut {
        &self.buffer
    }
    fn get_stream(&self) -> &BufWriter<T> {
        &self.stream
    }
    fn get_mut_buffer(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
    fn get_mut_stream(&mut self) -> &mut BufWriter<T> {
        &mut self.stream
    }
    fn get_mut_both(&mut self) -> (&mut BytesMut, &mut BufWriter<T>) {
        (&mut self.buffer, &mut self.stream)
    }
}

pub(super) type ExecutorFn<P, T, Strm> =
    for<'s> fn(&'s mut ConnectionHandler<P, T, Strm>, Query) -> FutureResult<'s, ActionResult<()>>;

/// # A generic connection handler
///
/// A [`ConnectionHandler`] object is a generic connection handler for any object that implements the [`RawConnection`] trait (or
/// the [`ProtocolRead`] trait). This function will accept such a type `T`, possibly a listener object and then use it to read
/// a query, parse it and return an appropriate response through [`corestore::Corestore::execute_query`]
pub struct ConnectionHandler<P, T, Strm> {
    db: Corestore,
    con: T,
    climit: Arc<Semaphore>,
    auth: AuthProvider,
    executor: ExecutorFn<P, T, Strm>,
    terminator: Terminator,
    _term_sig_tx: mpsc::Sender<()>,
    _marker: PhantomData<Strm>,
}

impl<P, T, Strm> ConnectionHandler<P, T, Strm>
where
    T: ProtocolRead<P, Strm> + Send + Sync,
    Strm: Stream,
    P: ProtocolSpec,
{
    pub fn new(
        db: Corestore,
        con: T,
        auth: AuthProvider,
        executor: ExecutorFn<P, T, Strm>,
        climit: Arc<Semaphore>,
        terminator: Terminator,
        _term_sig_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            db,
            con,
            auth,
            climit,
            executor,
            terminator,
            _term_sig_tx,
            _marker: PhantomData,
        }
    }
    pub async fn run(&mut self) -> IoResult<()> {
        while !self.terminator.is_termination_signal() {
            let try_df = tokio::select! {
                tdf = self.con.read_query() => tdf,
                _ = self.terminator.receive_signal() => {
                    return Ok(());
                }
            };
            match try_df {
                Ok(QueryResult::Q((query, advance_by))) => {
                    // the mutable reference to self ensures that the buffer is not modified
                    // hence ensuring that the pointers will remain valid
                    #[cfg(debug_assertions)]
                    let len_at_start = self.con.get_buffer().len();
                    #[cfg(debug_assertions)]
                    let sptr_at_start = self.con.get_buffer().as_ptr() as usize;
                    #[cfg(debug_assertions)]
                    let eptr_at_start = sptr_at_start + len_at_start;
                    {
                        match self.execute_query(query).await {
                            Ok(()) => {}
                            Err(ActionError::ActionError(e)) => {
                                self.con.close_conn_with_error(e).await?;
                            }
                            Err(ActionError::IoError(e)) => {
                                return Err(e);
                            }
                        }
                    }
                    {
                        // do these assertions to ensure memory safety (this is just for sanity sake)
                        #[cfg(debug_assertions)]
                        // len should be unchanged. no functions should **ever** touch the buffer
                        debug_assert_eq!(self.con.get_buffer().len(), len_at_start);
                        #[cfg(debug_assertions)]
                        // start of allocation should be unchanged
                        debug_assert_eq!(self.con.get_buffer().as_ptr() as usize, sptr_at_start);
                        #[cfg(debug_assertions)]
                        // end of allocation should be unchanged. else we're entirely violating
                        // memory safety guarantees
                        debug_assert_eq!(
                            unsafe {
                                // UNSAFE(@ohsayan): THis is always okay
                                self.con.get_buffer().as_ptr().add(len_at_start)
                            } as usize,
                            eptr_at_start
                        );
                        // this is only when we clear the buffer. since execute_query is not called
                        // at this point, it's totally fine (so invalidating ptrs is totally cool)
                        self.con.advance_buffer(advance_by);
                    }
                }
                Ok(QueryResult::E(r)) => self.con.close_conn_with_error(r).await?,
                Ok(QueryResult::Wrongtype) => {
                    self.con
                        .close_conn_with_error(responses::groups::WRONGTYPE_ERR.to_owned())
                        .await?
                }
                Ok(QueryResult::Disconnected) => return Ok(()),
                #[cfg(windows)]
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionReset => return Ok(()),
                    _ => return Err(e),
                },
                #[cfg(not(windows))]
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Execute queries for an unauthenticated user
    pub(super) fn execute_unauth(&mut self, query: Query) -> FutureResult<'_, ActionResult<()>> {
        Box::pin(async move {
            let con = &mut self.con;
            let db = &mut self.db;
            let mut auth_provider = AuthProviderHandle::new(&mut self.auth, &mut self.executor);
            match query {
                Query::Simple(sq) => {
                    con.write_simple_query_header().await?;
                    queryengine::execute_simple_noauth(db, con, &mut auth_provider, sq).await?;
                }
                Query::Pipelined(_) => {
                    con.write_simple_query_header().await?;
                    con.write_response(auth::errors::AUTH_CODE_BAD_CREDENTIALS)
                        .await?;
                }
            }
            Ok(())
        })
    }

    /// Execute queries for an authenticated user
    pub(super) fn execute_auth(&mut self, query: Query) -> FutureResult<'_, ActionResult<()>> {
        Box::pin(async move {
            let con = &mut self.con;
            let db = &mut self.db;
            let mut auth_provider = AuthProviderHandle::new(&mut self.auth, &mut self.executor);
            match query {
                Query::Simple(q) => {
                    con.write_simple_query_header().await?;
                    queryengine::execute_simple(db, con, &mut auth_provider, q).await?;
                }
                Query::Pipelined(pipeline) => {
                    con.write_pipeline_query_header(pipeline.len()).await?;
                    queryengine::execute_pipeline(db, con, &mut auth_provider, pipeline).await?;
                }
            }
            Ok(())
        })
    }

    /// Execute a query that has already been validated by `Connection::read_query`
    async fn execute_query(&mut self, query: Query) -> ActionResult<()> {
        (self.executor)(self, query).await?;
        self.con.flush_stream().await?;
        Ok(())
    }
}

impl<P, T, Strm> Drop for ConnectionHandler<P, T, Strm> {
    fn drop(&mut self) {
        // Make sure that the permit is returned to the semaphore
        // in the case that there is a panic inside
        self.climit.add_permits(1);
    }
}

/// A simple _shorthand trait_ for the insanely long definition of the TCP-based stream generic type
pub trait Stream: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync {}
impl<T> Stream for T where T: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync {}

/// A simple _shorthand trait_ for the insanely long definition of the connection generic type
pub trait ClientConnection<P: ProtocolSpec, Strm: Stream>:
    ProtocolRead<P, Strm> + Send + Sync
{
}
impl<P, T, Strm> ClientConnection<P, Strm> for T
where
    T: ProtocolRead<P, Strm> + Send + Sync,
    Strm: Stream,
    P: ProtocolSpec,
{
}
