/*
 * Created on Thu Jul 30 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

//! # The `protocol` module
//!
//! This module provides low-level interfaces to read data from a socket, when control
//! is handed over to it by `dbnet`, and high-level interfaces for parsing an incoming
//! query into an _executable query_ via the `deserializer` module.

mod deserializer;
pub mod responses;
use crate::resp::Writable;
use bytes::{Buf, BytesMut};
pub use deserializer::ActionGroup;
pub use deserializer::ParseResult;
pub use deserializer::Query;
use libtdb::TResult;
use libtdb::BUF_CAP;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
pub mod tls;

/// A TCP connection wrapper
pub struct Connection {
    /// The connection to the remote socket, wrapped in a buffer to speed
    /// up writing
    stream: BufWriter<TcpStream>,
    /// The in-memory read buffer. The size is given by `BUF_CAP`
    buffer: BytesMut,
}

/// The outcome of running `Connection`'s `try_query` function
pub enum QueryResult {
    /// A parsed `Query` object
    Q(Query),
    /// An error response
    E(Vec<u8>),
    /// A closed connection
    Empty,
}

impl Connection {
    /// Initiailize a new `Connection` instance
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(BUF_CAP),
        }
    }
    /// Read a query from the remote end
    ///
    /// This function asynchronously waits until all the data required
    /// for parsing the query is available
    pub async fn read_query(&mut self) -> Result<QueryResult, String> {
        self.read_again().await?;
        loop {
            match self.try_query() {
                Ok(ParseResult::Query(query, forward)) => {
                    self.buffer.advance(forward);
                    return Ok(QueryResult::Q(query));
                }
                Ok(ParseResult::BadPacket(bp)) => {
                    self.buffer.advance(bp);
                    return Ok(QueryResult::E(responses::fresp::R_PACKET_ERR.to_owned()));
                }
                Err(_) => {
                    return Ok(QueryResult::Empty);
                }
                _ => (),
            }
            self.read_again().await?;
        }
    }
    /// Try to parse a query from the buffered data
    fn try_query(&mut self) -> Result<ParseResult, ()> {
        if self.buffer.is_empty() {
            return Err(());
        }
        Ok(deserializer::parse(&self.buffer))
    }
    /// Try to fill the buffer again
    async fn read_again(&mut self) -> Result<(), String> {
        match self.stream.read_buf(&mut self.buffer).await {
            Ok(0) => {
                // If 0 bytes were received, then the remote end closed
                // the connection
                if self.buffer.is_empty() {
                    return Ok(());
                } else {
                    return Err(format!(
                        "Connection reset while reading from {}",
                        if let Ok(p) = self.get_peer() {
                            p.to_string()
                        } else {
                            "peer".to_owned()
                        }
                    )
                    .into());
                }
            }
            Ok(_) => Ok(()),
            Err(e) => return Err(format!("{}", e)),
        }
    }
    /// Get the peer address
    fn get_peer(&self) -> IoResult<SocketAddr> {
        self.stream.get_ref().peer_addr()
    }
    /// Write a response to the stream
    pub async fn write_response(&mut self, streamer: impl Writable) -> TResult<()> {
        streamer.write(&mut self.stream).await?;
        Ok(())
    }
    pub async fn flush_stream(&mut self) -> TResult<()> {
        self.stream.flush().await?;
        Ok(())
    }
    /// Wraps around the `write_response` used to differentiate between a
    /// success response and an error response
    pub async fn close_conn_with_error(&mut self, resp: Vec<u8>) -> TResult<()> {
        self.write_response(resp).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
