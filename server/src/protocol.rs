/*
 * Created on Sat Jul 18 2020
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

use bytes::{Buf, BytesMut};
use corelib::terrapipe::{extract_idents, ActionType};
use corelib::terrapipe::{RespBytes, RespCodes};
use std::error::Error;
use std::io::{BufRead, Cursor, Result as IoResult};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// The query dataframe
#[derive(Debug)]
pub struct QueryDataframe {
    /// The data part
    pub data: Vec<String>,
    /// The query action type
    pub actiontype: ActionType,
}

pub enum QueryParseResult {
    Query(QueryDataframe),
    RespCode(Vec<u8>),
    Incomplete,
}

use QueryParseResult::*;

/// A TCP connection wrapper
pub struct Connection {
    writer: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    /// Initiailize a new `Connection` instance
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            writer: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
        }
    }
    pub async fn read_query(&mut self) -> Result<QueryParseResult, String> {
        use QueryParseResult::*;
        loop {
            match self.writer.read_buf(&mut self.buffer).await {
                Ok(0) => {
                    if self.buffer.is_empty() {
                        return Err(format!("{:?} didn't send any data", self.get_peer()).into());
                    } else {
                        return Err(format!(
                            "Connection reset while reading from: {:?}",
                            self.get_peer()
                        )
                        .into());
                    }
                }
                Ok(_) => (),
                Err(e) => return Err(format!("{}", e)),
            }
            match self.parse_query().await? {
                Incomplete => (),
                x @ _ => return Ok(x),
            }
        }
    }
    async fn parse_query(&mut self) -> Result<QueryParseResult, String> {
        let mut metaline = Vec::with_capacity(46);
        let mut i = 0;
        let mut first_linefeed = None;
        while i < self.buffer.len() {
            if self.buffer[i] == b'\n' {
                first_linefeed = Some(i);
                break;
            }
            i = i + 1;
        }
        if first_linefeed.is_none() {
            return Ok(RespCode(RespCodes::InvalidMetaframe.into_response()));
        }
        metaline.extend_from_slice(&self.buffer[..first_linefeed.unwrap()]);
        let actiontype = match metaline.get(0) {
            Some(42) => ActionType::Simple,
            Some(36) => ActionType::Pipeline,
            _ => return Ok(RespCode(RespCodes::InvalidMetaframe.into_response())),
        };
        let sizes = match self.get_frame_sizes(&metaline[1..]) {
            Some(s) => s,
            None => return Ok(RespCode(RespCodes::InvalidMetaframe.into_response())),
        };
        eprintln!("Got frame sizes: {:?}", sizes);
        // Check if the thing is incomplete
        if self.buffer.remaining() < (sizes[0] + sizes[1]) {
            return Ok(Incomplete);
        }
        // We have read from 0 to metaline.len()+1
        // We need to read till (metaline.len())+1+metalayout.len()
        // Now parse the sizes
        let ss = match self
            .get_skip_sequence(&self.buffer[metaline.len() + 1..(metaline.len() + 1 + sizes[1])])
        {
            Some(s) => s,
            None => return Ok(RespCode(RespCodes::InvalidMetaframe.into_response())),
        };
        let data = self.read_dataframe(&self.buffer[(..)], ss);
        Ok(Query(QueryDataframe { data, actiontype }))
    }
    fn get_frame_sizes(&self, metaline: &[u8]) -> Option<Vec<usize>> {
        if let Some(s) = self.extract_sizes_splitoff(metaline, b'!', 2) {
            if s.len() == 2 {
                Some(s)
            } else {
                None
            }
        } else {
            None
        }
    }
    fn get_skip_sequence(&self, metalayout: &[u8]) -> Option<Vec<usize>> {
        let l = metalayout.len() / 2;
        self.extract_sizes_splitoff(metalayout, b'#', l)
    }
    fn read_dataframe(&self, dataframe: &[u8], skips: Vec<usize>) -> Vec<String> {
        extract_idents(dataframe.to_owned(), skips)
    }
    fn extract_sizes_splitoff(
        &self,
        buf: &[u8],
        splitoff: u8,
        sizehint: usize,
    ) -> Option<Vec<usize>> {
        let mut sizes = Vec::with_capacity(sizehint);
        let len = buf.len() - 1;
        let mut i = 0;
        while i < len {
            if buf[i] == splitoff {
                // This is a hash
                let mut res: usize = 0;
                // Move to the next element
                i = i + 1;
                while i < len {
                    if buf[i] != splitoff {
                        let num: usize = match buf[i].checked_sub(48) {
                            Some(s) => s.into(),
                            None => return None,
                        };
                        res = res * 10 + num;
                        i = i + 1;
                        continue;
                    } else {
                        break;
                    }
                }
                println!("{}", res);
                sizes.push(res.into());
                continue;
            } else {
                // Technically, we should never reach here, but if we do
                // clearly, it's an error by the client-side driver
                return None;
            }
        }
        Some(sizes)
    }
    fn get_peer(&self) -> IoResult<SocketAddr> {
        self.writer.get_ref().peer_addr()
    }
    pub async fn write_response(&mut self, resp: Vec<u8>) {
        if let Err(_) = self.writer.write_all(&resp).await {
            eprintln!("Error while writing to stream: {:?}", self.get_peer());
            return;
        }
        // Flush the stream to make sure that the data was delivered
        if let Err(_) = self.writer.flush().await {
            eprintln!("Error while flushing data to stream: {:?}", self.get_peer());
            return;
        }
    }
    /// Wraps around the `write_response` used to differentiate between a
    /// success response and an error response
    pub async fn close_conn_with_error(&mut self, bytes: Vec<u8>) {
        self.write_response(bytes).await
    }
}
