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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use super::deserializer;
use super::responses;
use crate::dbnet::Terminator;
use crate::protocol::tls::SslConnection;
use crate::protocol::Connection;
use crate::protocol::ParseResult;
use crate::protocol::QueryResult;
use crate::resp::Writable;
use crate::CoreDB;
use bytes::Buf;
use bytes::BytesMut;
use std::future::Future;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio_openssl::SslStream;

pub trait Con<Strm>: ConOps<Strm>
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
    fn try_query(&self) -> Result<ParseResult, ()> {
        if self.get_buffer().is_empty() {
            return Err(());
        }
        Ok(deserializer::parse(&self.get_buffer()))
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
                mv_self.read_again().await?;
                loop {
                    match mv_self.try_query() {
                        Ok(ParseResult::Query(query, forward)) => {
                            mv_self.advance_buffer(forward);
                            return Ok(QueryResult::Q(query));
                        }
                        Ok(ParseResult::BadPacket) => {
                            mv_self.clear_buffer();
                            return Ok(QueryResult::E(responses::fresp::R_PACKET_ERR.to_owned()));
                        }
                        Err(_) => {
                            return Ok(QueryResult::Empty);
                        }
                        _ => (),
                    }
                    mv_self.read_again().await?;
                }
            };
        })
    }
    /// Write a response to the stream
    fn write_response<'r, 's>(
        &'r mut self,
        streamer: impl Writable + 's + Send,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + 's,
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
    /// Wraps around the `write_response` used to differentiate between a
    /// success response and an error response
    fn close_conn_with_error<'r, 's>(
        &'r mut self,
        resp: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 's>>
    where
        'r: 's,
        Self: Send + 's,
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
}

pub trait ConOps<Strm> {
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

// Give ConOps implementors a free Con impl

impl<Strm, T> Con<Strm> for T
where
    T: ConOps<Strm>,
    Strm: Sync + Send + Unpin + AsyncWriteExt + AsyncReadExt,
{
}

impl ConOps<SslStream<TcpStream>> for SslConnection {
    fn get_buffer(&self) -> &BytesMut {
        &self.buffer
    }
    fn get_stream(&self) -> &BufWriter<SslStream<TcpStream>> {
        &self.stream
    }
    fn get_mut_buffer(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
    fn get_mut_stream(&mut self) -> &mut BufWriter<SslStream<TcpStream>> {
        &mut self.stream
    }
    fn get_mut_both(&mut self) -> (&mut BytesMut, &mut BufWriter<SslStream<TcpStream>>) {
        (&mut self.buffer, &mut self.stream)
    }
}

impl ConOps<TcpStream> for Connection {
    fn get_buffer(&self) -> &BytesMut {
        &self.buffer
    }
    fn get_stream(&self) -> &BufWriter<TcpStream> {
        &self.stream
    }
    fn get_mut_buffer(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
    fn get_mut_stream(&mut self) -> &mut BufWriter<TcpStream> {
        &mut self.stream
    }
    fn get_mut_both(&mut self) -> (&mut BytesMut, &mut BufWriter<TcpStream>) {
        (&mut self.buffer, &mut self.stream)
    }
}

pub struct ConnectionHandler<T, Strm>
where
    T: Con<Strm>,
    Strm: Sync + Send + Unpin + AsyncWriteExt + AsyncReadExt,
{
    db: CoreDB,
    con: T,
    climit: Arc<Semaphore>,
    terminator: Terminator,
    _term_sig_tx: mpsc::Sender<()>,
    _marker: PhantomData<Strm>,
}
