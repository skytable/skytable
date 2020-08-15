/*
 * Created on Thu Jul 30 2020
 *
 * This file is a part of the source code for the Terrabase database
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

mod deserializer;
use bytes::{Buf, BytesMut};
use corelib::builders::response::IntoResponse;
use corelib::builders::response::Response;
use corelib::de::*;
use corelib::TResult;
pub use deserializer::{
    Query,
    QueryParseResult::{self, *},
};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

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
    E(Response),
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
                Ok(Parsed((query, forward))) => {
                    self.buffer.advance(forward);
                    return Ok(QueryResult::Q(query));
                }
                Ok(RespCode(r)) => return Ok(QueryResult::E(r.into_response())),
                Err(_) => return Ok(QueryResult::Empty),
                _ => (),
            }
            self.read_again().await?;
        }
    }
    /// Try to parse a query from the buffered data
    fn try_query(&mut self) -> Result<QueryParseResult, ()> {
        if self.buffer.is_empty() {
            return Err(());
        }
        let nav = Navigator::new(&mut self.buffer);
        Ok(Query::from_navigator(nav))
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
    pub async fn write_response(&mut self, (mline, mlayout, df): Response) -> TResult<()> {
        self.stream.write_all(&mline).await?;
        self.stream.write_all(&mlayout).await?;
        self.stream.write_all(&df).await?;
        // Flush the stream to make sure that the data was delivered
        self.stream.flush().await?;
        Ok(())
    }
    /// Wraps around the `write_response` used to differentiate between a
    /// success response and an error response
    pub async fn close_conn_with_error(&mut self, resp: Response) -> TResult<()> {
        self.write_response(resp).await
    }
}
