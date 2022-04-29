/*
 * Created on Tue Apr 26 2022
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

use super::{responses, ParseError};
use crate::{
    corestore::buffers::Integer64,
    dbnet::connection::{QueryResult, QueryWithAdvance, RawConnection, Stream},
    resp::Writable,
    util::FutureResult,
    IoResult,
};
use std::io::{Error as IoError, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

pub const SIMPLE_QUERY_HEADER: [u8; 1] = [b'*'];

pub trait ProtocolCharset {
    const TSYMBOL_STRING: u8;
    const TSYMBOL_BINARY: u8;
    const TSYMBOL_FLOAT: u8;
    const TSYMBOL_INT64: u8;
    const TSYMBOL_TYPED_ARRAY: u8;
    const TSYMBOL_TYPED_NON_NULL_ARRAY: u8;
    const TSYMBOL_ARRAY: u8;
    const TSYMBOL_FLAT_ARRAY: u8;
    const LF: u8 = b'\n';
    const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8];
}

/// The [`ProtocolSpec`] trait implementation enables extremely easy switching between
/// protocols by being generic for the same base connection types
pub trait ProtocolSpec: Send + Sync + Sized + ProtocolCharset {
    fn parse(buf: &[u8]) -> Result<QueryWithAdvance, ParseError>;
}

/// # The `ProtocolRead` trait
///
/// The `ProtocolRead` trait enables read operations using the protocol for a given stream `Strm` and protocol
/// `P`. Both the stream and protocol must implement the appropriate traits for you to be able to use these
/// traits
///
/// ## DO NOT
/// The fact that this is a trait enables great flexibility in terms of visibility, but **DO NOT EVER CALL any
/// function other than `read_query`, `close_conn_with_error` or `write_response`**. If you mess with functions
/// like `read_again`, you're likely to pull yourself into some good trouble.
#[async_trait::async_trait]
pub trait ProtocolRead<P, Strm>: RawConnection<P, Strm>
where
    Strm: Stream,
    P: ProtocolSpec,
{
    /// Try to parse a query from the buffered data
    fn try_query(&self) -> Result<QueryWithAdvance, ParseError> {
        P::parse(self.get_buffer())
    }
    /// Read a query from the remote end
    ///
    /// This function asynchronously waits until all the data required
    /// for parsing the query is available
    fn read_query<'s, 'r: 's>(&'r mut self) -> FutureResult<'s, Result<QueryResult, IoError>> {
        Box::pin(async move {
            let mv_self = self;
            loop {
                let (buffer, stream) = mv_self.get_mut_both();
                match stream.read_buf(buffer).await {
                    Ok(0) => {
                        if buffer.is_empty() {
                            return Ok(QueryResult::Disconnected);
                        } else {
                            return Err(IoError::from(ErrorKind::ConnectionReset));
                        }
                    }
                    Ok(_) => {}
                    Err(e) => return Err(e),
                }
                match mv_self.try_query() {
                    Ok(query_with_advance) => {
                        return Ok(QueryResult::Q(query_with_advance));
                    }
                    Err(ParseError::NotEnough) => (),
                    Err(ParseError::DatatypeParseFailure) => return Ok(QueryResult::Wrongtype),
                    Err(ParseError::UnexpectedByte | ParseError::BadPacket) => {
                        return Ok(QueryResult::E(responses::full_responses::R_PACKET_ERR));
                    }
                }
            }
        })
    }
    /// Write a response to the stream
    fn write_response<'s, 'r: 's>(
        &'r mut self,
        streamer: impl Writable + 's + Send + Sync,
    ) -> FutureResult<'s, IoResult<()>> {
        Box::pin(async move {
            let mv_self = self;
            let streamer = streamer;
            let ret: IoResult<()> = {
                streamer.write(mv_self.get_mut_stream()).await?;
                Ok(())
            };
            ret
        })
    }
    /// Write the simple query header `*` to the stream
    fn write_simple_query_header<'s, 'r: 's>(&'r mut self) -> FutureResult<'s, IoResult<()>> {
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
    fn write_pipeline_query_header<'s, 'r: 's>(
        &'r mut self,
        len: usize,
    ) -> FutureResult<'s, IoResult<()>> {
        Box::pin(async move {
            let slf = self;
            slf.write_response([b'$']).await?;
            slf.get_mut_stream()
                .write_all(&Integer64::init(len as u64))
                .await?;
            slf.write_response([b'\n']).await?;
            Ok(())
        })
    }
    /// Write the flat array length (`_<size>\n`)
    fn write_flat_array_length<'s, 'r: 's>(
        &'r mut self,
        len: usize,
    ) -> FutureResult<'s, IoResult<()>> {
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
    fn write_array_length<'s, 'r: 's>(&'r mut self, len: usize) -> FutureResult<'s, IoResult<()>> {
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
    fn close_conn_with_error<'s, 'r: 's>(
        &'r mut self,
        resp: impl Writable + 's + Send + Sync,
    ) -> FutureResult<'s, IoResult<()>> {
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
    fn flush_stream<'s, 'r: 's>(&'r mut self) -> FutureResult<'s, IoResult<()>> {
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

impl<Strm, T, P> ProtocolRead<P, Strm> for T
where
    T: RawConnection<P, Strm> + Send + Sync,
    Strm: Stream,
    P: ProtocolSpec,
{
}

#[async_trait::async_trait]
pub trait ProtocolWrite<P, Strm>: RawConnection<P, Strm>
where
    Strm: Stream,
    P: ProtocolSpec,
{
    fn _get_raw_stream(&mut self) -> &mut BufWriter<Strm> {
        self.get_mut_stream()
    }

    // monoelements
    /// serialize and write an `&str` to the stream
    async fn write_string(&mut self, string: &str) -> IoResult<()>;
    /// serialize and write an `&[u8]` to the stream
    async fn write_binary(&mut self, binary: &[u8]) -> IoResult<()>;
    /// serialize and write an `usize` to the stream
    async fn write_usize(&mut self, size: usize) -> IoResult<()>;
    /// serialize and write an `f32` to the stream
    async fn write_float(&mut self, float: f32) -> IoResult<()>;

    // typed array
    async fn write_typed_array_header(&mut self, len: usize, tsymbol: u8) -> IoResult<()> {
        // <typed array tsymbol><element type symbol><len>\n
        self.get_mut_stream()
            .write_all(&[P::TSYMBOL_TYPED_ARRAY, tsymbol])
            .await?;
        self.get_mut_stream()
            .write_all(&Integer64::from(len))
            .await?;
        self.get_mut_stream().write_all(&[P::LF]).await?;
        Ok(())
    }
    async fn write_typed_array_element_null(&mut self) -> IoResult<()> {
        self.get_mut_stream()
            .write_all(P::TYPE_TYPED_ARRAY_ELEMENT_NULL)
            .await
    }
    async fn write_typed_array_element(&mut self, element: &[u8]) -> IoResult<()>;

    // typed non-null array
    async fn write_typed_non_null_array_header(&mut self, len: usize, tsymbol: u8) -> IoResult<()> {
        // <typed array tsymbol><element type symbol><len>\n
        self.get_mut_stream()
            .write_all(&[P::TSYMBOL_TYPED_NON_NULL_ARRAY, tsymbol])
            .await?;
        self.get_mut_stream()
            .write_all(&Integer64::from(len))
            .await?;
        self.get_mut_stream().write_all(&[P::LF]).await?;
        Ok(())
    }
    async fn write_typed_non_null_array_element(&mut self, element: &[u8]) -> IoResult<()> {
        self.write_typed_array_element(element).await
    }
}
