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
    super::{BufferedSocketStream, QueryResult},
    crate::{
        corestore::buffers::Integer64,
        protocol::{interface::ProtocolSpec, ParseError},
        IoResult,
    },
    bytes::BytesMut,
    std::{
        io::{Error as IoError, ErrorKind},
        marker::PhantomData,
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};

const BUF_WRITE_CAP: usize = 8192;
const BUF_READ_CAP: usize = 8192;

/// A generic connection type
///
/// The generic connection type allows you to choose:
/// 1. A stream (TCP, TLS(TCP), UDS, ...)
/// 2. A protocol (one that implements [`ProtocolSpec`])
pub struct Connection<T, P> {
    pub(super) stream: BufWriter<T>,
    pub(super) buffer: BytesMut,
    _marker: PhantomData<P>,
}

impl<T: BufferedSocketStream, P: ProtocolSpec> Connection<T, P> {
    pub fn new(stream: T) -> Self {
        Connection {
            stream: BufWriter::with_capacity(BUF_WRITE_CAP, stream),
            buffer: BytesMut::with_capacity(BUF_READ_CAP),
            _marker: PhantomData,
        }
    }
}

// protocol read
impl<T: BufferedSocketStream, P: ProtocolSpec> Connection<T, P> {
    /// Attempt to read a query
    pub(super) async fn read_query(&mut self) -> IoResult<QueryResult> {
        loop {
            match self.stream.read_buf(&mut self.buffer).await {
                Ok(0) => {
                    if self.buffer.is_empty() {
                        // buffer is empty, and the remote pulled off (simple disconnection)
                        return Ok(QueryResult::Disconnected);
                    } else {
                        // wrote something, and then died. nope, that's an error
                        return Err(IoError::from(ErrorKind::ConnectionReset));
                    }
                }
                Ok(_) => {}
                Err(e) => return Err(e),
            }
            // see if we have buffered enough data to run anything
            match P::decode_packet(self.buffer.as_ref()) {
                Ok(query_with_advance) => return Ok(QueryResult::Q(query_with_advance)),
                Err(ParseError::NotEnough) => {}
                Err(e) => {
                    self.write_error(P::SKYHASH_PARSE_ERROR_LUT[e as usize - 1])
                        .await?;
                    return Ok(QueryResult::NextLoop);
                }
            }
        }
    }
}

// protocol write (metaframe)
impl<T: BufferedSocketStream, P: ProtocolSpec> Connection<T, P> {
    /// Write a simple query header to the stream
    pub(super) async fn write_simple_query_header(&mut self) -> IoResult<()> {
        self.stream.write_all(P::SIMPLE_QUERY_HEADER).await
    }

    /// Write the pipeline query header
    pub(super) async fn write_pipelined_query_header(&mut self, count: usize) -> IoResult<()> {
        // write pipeline first byte
        self.stream.write_u8(P::PIPELINED_QUERY_FIRST_BYTE).await?;
        // write pipeline query count
        self.stream.write_all(&Integer64::from(count)).await?;
        // write the LF
        self.stream.write_u8(P::LF).await
    }
}

// protocol write (helpers)
impl<T: BufferedSocketStream, P: ProtocolSpec> Connection<T, P> {
    /// Write an error to the stream (just used to differentiate between "normal" and "errored" writes)
    pub(super) async fn write_error(&mut self, error: &[u8]) -> IoResult<()> {
        self.stream.write_all(error).await?;
        self.stream.flush().await
    }
    /// Write something "raw" to the stream (intentional underscore to avoid misuse)
    pub async fn _write_raw(&mut self, raw: &[u8]) -> IoResult<()> {
        self.stream.write_all(raw).await
    }
}

// protocol write (dataframe)
impl<T: BufferedSocketStream, P: ProtocolSpec> Connection<T, P> {
    // monoelements
    /// Encode and write a length-prefixed monoelement
    pub async fn write_mono_length_prefixed_with_tsymbol(
        &mut self,
        data: &[u8],
        tsymbol: u8,
    ) -> IoResult<()> {
        // first write the tsymbol
        self.stream.write_u8(tsymbol).await?;
        // now write length
        self.stream.write_all(&Integer64::from(data.len())).await?;
        // now write LF
        self.stream.write_u8(P::LF).await?;
        // now write the actual body
        self.stream.write_all(data).await?;
        if P::NEEDS_TERMINAL_LF {
            self.stream.write_u8(P::LF).await
        } else {
            Ok(())
        }
    }
    /// Encode and write a mon element (**without** length-prefixing)
    pub async fn write_mono_with_tsymbol(&mut self, data: &[u8], tsymbol: u8) -> IoResult<()> {
        // first write the tsymbol
        self.stream.write_u8(tsymbol).await?;
        // now write the actual body
        self.stream.write_all(data).await?;
        self.stream.write_u8(P::LF).await
    }
    /// Encode and write an unicode string
    pub async fn write_string(&mut self, string: &str) -> IoResult<()> {
        self.write_mono_length_prefixed_with_tsymbol(string.as_bytes(), P::TSYMBOL_STRING)
            .await
    }
    /// Encode and write a blob
    #[allow(unused)]
    pub async fn write_binary(&mut self, binary: &[u8]) -> IoResult<()> {
        self.write_mono_length_prefixed_with_tsymbol(binary, P::TSYMBOL_BINARY)
            .await
    }
    /// Encode and write an `usize`
    pub async fn write_usize(&mut self, size: usize) -> IoResult<()> {
        self.write_mono_with_tsymbol(&Integer64::from(size), P::TSYMBOL_INT64)
            .await
    }
    /// Encode and write an `u64`
    pub async fn write_int64(&mut self, int: u64) -> IoResult<()> {
        self.write_mono_with_tsymbol(&Integer64::from(int), P::TSYMBOL_INT64)
            .await
    }
    /// Encode and write an `f32`
    pub async fn write_float(&mut self, float: f32) -> IoResult<()> {
        self.write_mono_with_tsymbol(float.to_string().as_bytes(), P::TSYMBOL_FLOAT)
            .await
    }

    // typed array
    /// Write a typed array header (including type information and size)
    pub async fn write_typed_array_header(&mut self, len: usize, tsymbol: u8) -> IoResult<()> {
        self.stream
            .write_all(&[P::TSYMBOL_TYPED_ARRAY, tsymbol])
            .await?;
        self.stream.write_all(&Integer64::from(len)).await?;
        self.stream.write_u8(P::LF).await
    }
    /// Encode and write a null element for a typed array
    pub async fn write_typed_array_element_null(&mut self) -> IoResult<()> {
        self.stream
            .write_all(P::TYPE_TYPED_ARRAY_ELEMENT_NULL)
            .await
    }
    /// Encode and write a typed array element
    pub async fn write_typed_array_element(&mut self, element: &[u8]) -> IoResult<()> {
        self.stream
            .write_all(&Integer64::from(element.len()))
            .await?;
        self.stream.write_u8(P::LF).await?;
        self.stream.write_all(element).await?;
        if P::NEEDS_TERMINAL_LF {
            self.stream.write_u8(P::LF).await
        } else {
            Ok(())
        }
    }

    // typed non-null array
    /// write typed non-null array header
    pub async fn write_typed_non_null_array_header(
        &mut self,
        len: usize,
        tsymbol: u8,
    ) -> IoResult<()> {
        self.stream
            .write_all(&[P::TSYMBOL_TYPED_NON_NULL_ARRAY, tsymbol])
            .await?;
        self.stream.write_all(&Integer64::from(len)).await?;
        self.stream.write_all(&[P::LF]).await
    }
    /// Encode and write typed non-null array element
    pub async fn write_typed_non_null_array_element(&mut self, element: &[u8]) -> IoResult<()> {
        self.write_typed_array_element(element).await
    }
    /// Encode and write a typed non-null array
    pub async fn write_typed_non_null_array<A, B>(&mut self, body: B, tsymbol: u8) -> IoResult<()>
    where
        B: AsRef<[A]>,
        A: AsRef<[u8]>,
    {
        let body = body.as_ref();
        self.write_typed_non_null_array_header(body.len(), tsymbol)
            .await?;
        for element in body {
            self.write_typed_non_null_array_element(element.as_ref())
                .await?;
        }
        Ok(())
    }
}
