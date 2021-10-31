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
//! The `con` module defines the generic connection traits `ProtocolConnection` and `ProtocolConnectionExt`.
//! These two traits can be used to interface with sockets that are used for communication through the Skyhash
//! protocol.
//!
//! The `ProtocolConnection` trait provides a basic set of methods that are required by prospective connection
//! objects to be eligible for higher level protocol interactions (such as interactions with high-level query objects).
//! Once a type implements this trait, it automatically gets a free `ProtocolConnectionExt` implementation. This immediately
//! enables this connection object/type to use methods like read_query enabling it to read and interact with queries and write
//! respones in compliance with the Skyhash protocol.

use super::tcp::Connection;
use crate::corestore::buffers::Integer64;
use crate::corestore::Corestore;
use crate::dbnet::tcp::BufferedSocketStream;
use crate::dbnet::Terminator;
use crate::protocol;
use crate::protocol::responses;
use crate::protocol::ParseError;
use crate::protocol::Query;
use crate::resp::Writable;
use crate::IoResult;
use bytes::Buf;
use bytes::BytesMut;
use libsky::TResult;
use std::future::Future;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;

pub const SIMPLE_QUERY_HEADER: [u8; 3] = [b'*', b'1', b'\n'];
type QueryWithAdvance = (Query, usize);

pub enum QueryResult {
    Q(QueryWithAdvance),
    E(&'static [u8]),
    Empty,
    Wrongtype,
}

pub mod prelude {
    //! A 'prelude' for callers that would like to use the `ProtocolConnection` and `ProtocolConnectionExt` traits
    //!
    //! This module is hollow itself, it only re-exports from `dbnet::con` and `tokio::io`
    pub use super::ProtocolConnectionExt;
    pub use crate::aerr;
    pub use crate::conwrite;
    pub use crate::corestore::Corestore;
    pub use crate::default_keyspace;
    pub use crate::err_if_len_is;
    pub use crate::get_tbl;
    pub use crate::handle_entity;
    pub use crate::is_lowbit_set;
    pub use crate::kve;
    pub use crate::protocol::responses;
    pub use crate::protocol::responses::groups;
    pub use crate::queryengine::ActionIter;
    pub use crate::registry;
    pub use crate::util::Unwrappable;
    pub use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[macro_export]
    macro_rules! kve {
        ($con:expr, $store:expr) => {
            match $store.get_kvstore() {
                Ok(store) => store,
                _ => {
                    // wrong model
                    return $con
                        .write_response(crate::protocol::responses::groups::WRONG_MODEL)
                        .await;
                }
            }
        };
    }
    #[macro_export]
    macro_rules! default_keyspace {
        ($store:expr, $con:expr) => {
            match $store.get_keyspace() {
                Ok(ks) => ks,
                Err(_) => {
                    return $con
                        .write_response(crate::protocol::responses::groups::DEFAULT_UNSET)
                        .await;
                }
            }
        };
    }
    #[macro_export]
    macro_rules! conwrite {
        ($con:expr, $what:expr) => {
            $con.write_response($what).await
        };
    }
    #[macro_export]
    macro_rules! aerr {
        ($con:expr, aerr) => {
            return conwrite!($con, crate::protocol::responses::groups::ACTION_ERR)
        };
    }
    #[macro_export]
    macro_rules! get_tbl {
        ($entity:expr, $store:expr, $con:expr) => {{
            use crate::corestore::memstore::DdlError;
            match $store.get_table($entity) {
                Ok(tbl) => tbl,
                Err(DdlError::DefaultNotFound) => {
                    return conwrite!($con, crate::protocol::responses::groups::DEFAULT_UNSET);
                }
                Err(DdlError::ObjectNotFound) => {
                    return conwrite!(
                        $con,
                        crate::protocol::responses::groups::CONTAINER_NOT_FOUND
                    );
                }
                Err(_) => unsafe { impossible!() },
            }
        }};
        ($store:expr, $con:expr) => {{
            match $store.get_ctable() {
                Some(tbl) => tbl,
                None => return conwrite!($con, crate::protocol::responses::groups::DEFAULT_UNSET),
            }
        }};
    }
    #[macro_export]
    macro_rules! handle_entity {
        ($con:expr, $ident:expr) => {{
            match crate::queryengine::parser::get_query_entity(&$ident) {
                Ok(e) => e,
                Err(e) => return conwrite!($con, e),
            }
        }};
    }
}

/// # The `ProtocolConnectionExt` trait
///
/// The `ProtocolConnectionExt` trait has default implementations and doesn't ever require explicit definitions, unless
/// there's some black magic that you want to do. All [`ProtocolConnection`] objects will get a free implementation for this trait.
/// Hence implementing [`ProtocolConnection`] alone is enough for you to get high-level methods to interface with the protocol.
///
/// ## DO NOT
/// The fact that this is a trait enables great flexibility in terms of visibility, but **DO NOT EVER CALL any function other than
/// `read_query`, `close_conn_with_error` or `write_response`**. If you mess with functions like `read_again`, you're likely to pull yourself into some
/// good trouble.
pub trait ProtocolConnectionExt<Strm>: ProtocolConnection<Strm> + Send
where
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    /// Try to fill the buffer again
    fn read_again<'r, 's>(&'r mut self) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let ret: IoResult<()> = {
                let (buffer, stream) = mv_self.get_mut_both();
                match stream.read_buf(buffer).await {
                    Ok(0) => {
                        if buffer.is_empty() {
                            return Ok(());
                        } else {
                            return Err(IoError::from(ErrorKind::ConnectionReset));
                        }
                    }
                    Ok(_) => Ok(()),
                    Err(e) => return Err(e),
                }
            };
            ret
        })
    }
    /// Try to parse a query from the buffered data
    fn try_query(&self) -> Result<QueryWithAdvance, ParseError> {
        if self.get_buffer().is_empty() {
            return Err(ParseError::Empty);
        }
        protocol::Parser::new(self.get_buffer()).parse()
    }
    /// Read a query from the remote end
    ///
    /// This function asynchronously waits until all the data required
    /// for parsing the query is available
    fn read_query<'r, 's>(
        &'r mut self,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, IoError>> + Send + 's>>
    where
        'r: 's,
        Self: Send + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let _: Result<QueryResult, IoError> = {
                loop {
                    mv_self.read_again().await?;
                    match mv_self.try_query() {
                        Ok(query_with_advance) => {
                            return Ok(QueryResult::Q(query_with_advance));
                        }
                        Err(ParseError::Empty) => return Ok(QueryResult::Empty),
                        Err(ParseError::NotEnough) => (),
                        Err(ParseError::DatatypeParseFailure) => return Ok(QueryResult::Wrongtype),
                        Err(ParseError::UnexpectedByte) | Err(ParseError::BadPacket) => {
                            return Ok(QueryResult::E(responses::full_responses::R_PACKET_ERR));
                        }
                        Err(ParseError::UnknownDatatype) => {
                            return Ok(QueryResult::E(
                                responses::full_responses::R_UNKNOWN_DATA_TYPE,
                            ));
                        }
                    }
                }
            };
        })
    }
    /// Write a response to the stream
    fn write_response<'r, 's>(
        &'r mut self,
        streamer: impl Writable + 's + Send + Sync,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Sync + Send + 's>>
    where
        'r: 's,
        Self: Send + 's,
        Self: Sync,
    {
        Box::pin(async move {
            let mv_self = self;
            let streamer = streamer;
            let ret: IoResult<()> = {
                streamer.write(&mut mv_self.get_mut_stream()).await?;
                Ok(())
            };
            ret
        })
    }
    /// Write the simple query header `*1\n` to the stream
    fn write_simple_query_header<'r, 's>(
        &'r mut self,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + Sync + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let ret: IoResult<()> = {
                mv_self.write_response(SIMPLE_QUERY_HEADER).await?;
                Ok(())
            };
            ret
        })
    }
    /// Write the length of the pipeline query (*)
    fn write_pipeline_query_header<'r, 's>(
        &'r mut self,
        len: usize,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + Sync + 's,
    {
        Box::pin(async move {
            let slf = self;
            slf.write_response([b'*']).await?;
            slf.get_mut_stream()
                .write_all(&Integer64::init(len as u64))
                .await?;
            slf.write_response([b'\n']).await?;
            Ok(())
        })
    }
    /// Write the flat array length (`_<size>\n`)
    fn write_flat_array_length<'r, 's>(
        &'r mut self,
        len: usize,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + Sync + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let ret: IoResult<()> = {
                mv_self.write_response([b'_']).await?;
                mv_self.write_response(len.to_string().into_bytes()).await?;
                mv_self.write_response([b'\n']).await?;
                Ok(())
            };
            ret
        })
    }
    /// Write the array length (`&<size>\n`)
    fn write_array_length<'r, 's>(
        &'r mut self,
        len: usize,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + Sync + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let ret: IoResult<()> = {
                mv_self.write_response([b'&']).await?;
                mv_self.write_response(len.to_string().into_bytes()).await?;
                mv_self.write_response([b'\n']).await?;
                Ok(())
            };
            ret
        })
    }
    /// Wraps around the `write_response` used to differentiate between a
    /// success response and an error response
    fn close_conn_with_error<'r, 's>(
        &'r mut self,
        resp: impl Writable + 's + Send + Sync,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + Sync + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let ret: IoResult<()> = {
                mv_self.write_response(resp).await?;
                mv_self.flush_stream().await?;
                Ok(())
            };
            ret
        })
    }
    fn flush_stream<'r, 's>(&'r mut self) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + 's,
    {
        Box::pin(async move {
            let mv_self = self;
            let ret: IoResult<()> = {
                mv_self.get_mut_stream().flush().await?;
                Ok(())
            };
            ret
        })
    }
    unsafe fn raw_stream(&mut self) -> &mut BufWriter<Strm> {
        self.get_mut_stream()
    }
}

/// # The `ProtocolConnection` trait
///
/// The `ProtocolConnection` trait has low-level methods that can be used to interface with raw sockets. Any type
/// that successfully implements this trait will get an implementation for `ProtocolConnectionExt` which augments and
/// builds on these fundamental methods to provide high-level interfacing with queries.
///
/// ## Example of a `ProtocolConnection` object
/// Ideally a `ProtocolConnection` object should look like (the generic parameter just exists for doc-tests, just think that
/// there is a type `Strm`):
/// ```no_run
/// struct Connection<Strm> {
///     pub buffer: bytes::BytesMut,
///     pub stream: Strm,
/// }
/// ```
///
/// `Strm` should be a stream, i.e something like an SSL connection/TCP connection.
pub trait ProtocolConnection<Strm> {
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

// Give ProtocolConnection implementors a free ProtocolConnectionExt impl

impl<Strm, T> ProtocolConnectionExt<Strm> for T
where
    T: ProtocolConnection<Strm> + Send,
    Strm: Sync + Send + Unpin + AsyncWriteExt + AsyncReadExt,
{
}

impl<T> ProtocolConnection<T> for Connection<T>
where
    T: BufferedSocketStream,
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

/// # A generic connection handler
///
/// A [`ConnectionHandler`] object is a generic connection handler for any object that implements the [`ProtocolConnection`] trait (or
/// the [`ProtocolConnectionExt`] trait). This function will accept such a type `T`, possibly a listener object and then use it to read
/// a query, parse it and return an appropriate response through [`corestore::Corestore::execute_query`]
pub struct ConnectionHandler<T, Strm>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: Sync + Send + Unpin + AsyncWriteExt + AsyncReadExt,
    Self: Send,
{
    db: Corestore,
    con: T,
    climit: Arc<Semaphore>,
    terminator: Terminator,
    _term_sig_tx: mpsc::Sender<()>,
    _marker: PhantomData<Strm>,
}

impl<T, Strm> ConnectionHandler<T, Strm>
where
    T: ProtocolConnectionExt<Strm> + Send + Sync,
    Strm: Sync + Send + Unpin + AsyncWriteExt + AsyncReadExt,
{
    pub fn new(
        db: Corestore,
        con: T,
        climit: Arc<Semaphore>,
        terminator: Terminator,
        _term_sig_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            db,
            con,
            climit,
            terminator,
            _term_sig_tx,
            _marker: PhantomData,
        }
    }
    pub async fn run(&mut self) -> TResult<()> {
        while !self.terminator.is_termination_signal() {
            let try_df = tokio::select! {
                tdf = self.con.read_query() => tdf,
                _ = self.terminator.receive_signal() => {
                    return Ok(());
                }
            };
            match try_df {
                Ok(QueryResult::Q((query, advance_by))) => {
                    self.db.execute_query(query, &mut self.con).await?;
                    self.con.advance_buffer(advance_by);
                }
                Ok(QueryResult::E(r)) => self.con.close_conn_with_error(r).await?,
                Ok(QueryResult::Wrongtype) => {
                    self.con
                        .close_conn_with_error(responses::groups::WRONGTYPE_ERR.to_owned())
                        .await?
                }
                Ok(QueryResult::Empty) => return Ok(()),
                #[cfg(windows)]
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionReset => return Ok(()),
                    _ => return Err(e.into()),
                },
                #[cfg(not(windows))]
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

impl<T, Strm> Drop for ConnectionHandler<T, Strm>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: Sync + Send + Unpin + AsyncWriteExt + AsyncReadExt,
{
    fn drop(&mut self) {
        // Make sure that the permit is returned to the semaphore
        // in the case that there is a panic inside
        self.climit.add_permits(1);
    }
}
