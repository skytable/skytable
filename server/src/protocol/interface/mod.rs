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
    util::FutureResult,
    IoResult,
};
use std::io::{Error as IoError, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

pub trait ProtocolSpec {
    const TSYMBOL_STRING: u8;
    const TSYMBOL_BINARY: u8;
    const TSYMBOL_FLOAT: u8;
    const TSYMBOL_INT64: u8;
    const TSYMBOL_TYPED_ARRAY: u8;
    const TSYMBOL_TYPED_NON_NULL_ARRAY: u8;
    const TSYMBOL_ARRAY: u8;
    const TSYMBOL_FLAT_ARRAY: u8;
    const LF: u8 = b'\n';
    const SIMPLE_QUERY_HEADER: &'static [u8];
    const PIPELINED_QUERY_FIRST_BYTE: u8;
    const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8];
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
pub trait ProtocolRead<P, Strm>: RawConnection<P, Strm>
where
    Strm: Stream,
    P: ProtocolSpec,
{
    /// Try to parse a query from the buffered data
    fn try_query(&self) -> Result<QueryWithAdvance, ParseError>;
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
}

pub trait ProtocolWrite<P, Strm>: RawConnection<P, Strm>
where
    Strm: Stream,
    P: ProtocolSpec,
{
    // utility
    fn _get_raw_stream(&mut self) -> &mut BufWriter<Strm> {
        self.get_mut_stream()
    }
    fn _flush_stream<'life0, 'ret_life>(&'life0 mut self) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move { self.get_mut_stream().flush().await })
    }
    fn _write_raw<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        data: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move { self.get_mut_stream().write_all(data).await })
    }
    fn _write_raw_flushed<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        data: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            self._write_raw(data).await?;
            self._flush_stream().await
        })
    }
    fn close_conn_with_error<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        resp: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move { self._write_raw_flushed(resp).await })
    }

    // metaframe
    fn write_simple_query_header<'life0, 'ret_life>(
        &'life0 mut self,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            self.get_mut_stream()
                .write_all(P::SIMPLE_QUERY_HEADER)
                .await
        })
    }
    fn write_pipelined_query_header<'life0, 'ret_life>(
        &'life0 mut self,
        qcount: usize,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            self.get_mut_stream()
                .write_all(&[P::PIPELINED_QUERY_FIRST_BYTE])
                .await?;
            self.get_mut_stream()
                .write_all(&Integer64::from(qcount))
                .await?;
            self.get_mut_stream().write_all(&[P::LF]).await
        })
    }

    // monoelement
    fn write_mono_length_prefixed_with_tsymbol<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        data: &'life1 [u8],
        tsymbol: u8,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // <tsymbol><length><lf>
            stream.write_all(&[tsymbol]).await?;
            stream.write_all(&Integer64::from(data.len())).await?;
            stream.write_all(&[P::LF]).await?;
            stream.write_all(data).await
        })
    }
    /// serialize and write an `&str` to the stream
    fn write_string<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        string: &'life1 str,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: 'ret_life;
    /// serialize and write an `&[u8]` to the stream
    fn write_binary<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        binary: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: 'ret_life;
    /// serialize and write an `usize` to the stream
    fn write_usize<'life0, 'ret_life>(
        &'life0 mut self,
        size: usize,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: 'ret_life;
    /// serialize and write an `u64` to the stream
    fn write_int64<'life0, 'ret_life>(
        &'life0 mut self,
        int: u64,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: 'ret_life;
    /// serialize and write an `f32` to the stream
    fn write_float<'life0, 'ret_life>(
        &'life0 mut self,
        float: f32,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: 'ret_life;

    // typed array
    fn write_typed_array_header<'life0, 'ret_life>(
        &'life0 mut self,
        len: usize,
        tsymbol: u8,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            self.get_mut_stream()
                .write_all(&[P::TSYMBOL_TYPED_ARRAY, tsymbol])
                .await?;
            self.get_mut_stream()
                .write_all(&Integer64::from(len))
                .await?;
            self.get_mut_stream().write_all(&[P::LF]).await?;
            Ok(())
        })
    }
    fn write_typed_array_element_null<'life0, 'ret_life>(
        &'life0 mut self,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            self.get_mut_stream()
                .write_all(P::TYPE_TYPED_ARRAY_ELEMENT_NULL)
                .await
        })
    }
    fn write_typed_array_element<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        element: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: 'ret_life;

    // typed non-null array
    fn write_typed_non_null_array_header<'life0, 'ret_life>(
        &'life0 mut self,
        len: usize,
        tsymbol: u8,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move {
            self.get_mut_stream()
                .write_all(&[P::TSYMBOL_TYPED_NON_NULL_ARRAY, tsymbol])
                .await?;
            self.get_mut_stream()
                .write_all(&Integer64::from(len))
                .await?;
            self.get_mut_stream().write_all(&[P::LF]).await?;
            Ok(())
        })
    }
    fn write_typed_non_null_array_element<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        element: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: Send + 'ret_life,
    {
        Box::pin(async move { self.write_typed_array_element(element).await })
    }
}
