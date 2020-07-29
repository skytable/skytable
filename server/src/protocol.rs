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

use bytes::BytesMut;
use corelib::terrapipe::{extract_idents, ActionType};
use corelib::terrapipe::{RespBytes, RespCodes};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The query dataframe
#[derive(Debug)]
pub struct QueryDataframe {
    /// The data part
    pub data: Vec<String>,
    /// The query action type
    pub actiontype: ActionType,
}

#[derive(Debug)]
pub enum QueryParseResult {
    Query(QueryDataframe),
    RespCode(Vec<u8>),
    Incomplete,
}

use QueryParseResult::*;

/// A TCP connection wrapper
pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    /// Initiailize a new `Connection` instance
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }
    pub async fn read_query(&mut self) -> Result<QueryParseResult, String> {
        self.read_again().await?;
        loop {
            match self.try_query().await? {
                x @ Query(_) | x @ RespCode(_) => return Ok(x),
                _ => (),
            }
            self.read_again().await?;
        }
    }
    async fn try_query(&mut self) -> Result<QueryParseResult, String> {
        self.parse_query().await
    }
    async fn read_again(&mut self) -> Result<(), String> {
        match self.stream.read_buf(&mut self.buffer).await {
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
            Ok(_) => Ok(()),
            Err(e) => return Err(format!("{}", e)),
        }
    }
    async fn parse_query(&mut self) -> Result<QueryParseResult, String> {
        if self.buffer.is_empty() {
            return Ok(Incomplete);
        }
        let mut lf_1: Option<usize> = None;
        let mut i: usize = 4; // Minimum size of metaline, so skip the first 5 elements
        let ref buf = self.buffer;
        while i < buf.len() {
            if buf[i] == b'\n' {
                lf_1 = Some(i);
                break;
            }
            i = i + 1
        }
        if lf_1.is_none() {
            return Ok(RespCode(RespCodes::InvalidMetaframe.into_response()));
        }
        let actiontype = match buf[0] {
            b'*' => ActionType::Simple,
            b'$' => ActionType::Pipeline,
            _ => return Ok(RespCode(RespCodes::InvalidMetaframe.into_response())),
        };
        let lf_1 = lf_1.unwrap();
        let frame_sizes = match self.get_frame_sizes(&buf[1..lf_1]) {
            Some(s) => s,
            None => return Ok(RespCode(RespCodes::InvalidMetaframe.into_response())),
        };
        let metalayout_idx = lf_1 + 1..(lf_1 + frame_sizes[1]);
        let skip_seq = match self.get_skip_sequence(&buf[metalayout_idx]) {
            Some(s) => s,
            None => return Ok(RespCode(RespCodes::InvalidMetaframe.into_response())),
        };
        let endofmetaframe = lf_1 + frame_sizes[1];
        let remaining = buf.len() - endofmetaframe;
        if remaining < frame_sizes[1] {
            return Ok(Incomplete);
        }
        let df = self.read_dataframe(&buf[endofmetaframe + 1..buf.len() - 1], skip_seq);
        println!("Dataframe: '{:?}'", df);
        Ok(Query(QueryDataframe {
            data: df,
            actiontype,
        }))
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
        let len = buf.len();
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
        self.stream.peer_addr()
    }
    pub async fn write_response(&mut self, resp: Vec<u8>) {
        if let Err(_) = self.stream.write_all(&resp).await {
            eprintln!("Error while writing to stream: {:?}", self.get_peer());
            return;
        }
        // Flush the stream to make sure that the data was delivered
        if let Err(_) = self.stream.flush().await {
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
