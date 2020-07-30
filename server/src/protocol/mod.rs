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

pub mod deserializer {
    //! This module provides deserialization primitives for query packets
    use bytes::BytesMut;
    use corelib::terrapipe::{ActionType, RespCodes};
    use std::io::Cursor;
    #[derive(Debug)]
    pub enum QueryParseResult {
        Parsed((Query, usize)),
        RespCode(RespCodes),
        Incomplete,
    }

    pub struct Navigator<'a> {
        buffer: &'a BytesMut,
        cursor: Cursor<&'a [u8]>,
    }
    impl<'a> Navigator<'a> {
        pub fn new(buffer: &'a mut BytesMut) -> Self {
            Navigator {
                cursor: Cursor::new(&buffer[..]),
                buffer,
            }
        }
        pub fn get_line(&mut self, beforehint: Option<usize>) -> Option<&'a [u8]> {
            let ref mut cursor = self.cursor;
            let start = cursor.position() as usize;
            let end = match beforehint {
                Some(hint) => (start + hint),
                None => cursor.get_ref().len() - 1,
            };
            for i in start..end {
                if cursor.get_ref()[i] == b'\n' {
                    cursor.set_position((i + 1) as u64);
                    return cursor.get_ref().get(start..i);
                }
            }
            None
        }
        pub fn get_exact(&mut self, exact: usize) -> Option<&'a [u8]> {
            let ref mut cursor = self.cursor;
            let start = cursor.position() as usize;
            let end = start + exact;
            if let Some(chunk) = cursor.get_ref().get(start..end) {
                self.cursor.set_position(end as u64);
                Some(chunk)
            } else {
                None
            }
        }
        fn get_pos_usize(&self) -> usize {
            self.cursor.position() as usize
        }
    }

    struct Metaline {
        content_size: usize,
        metalayout_size: usize,
        actiontype: ActionType,
    }

    impl Metaline {
        pub fn from_navigator(nav: &mut Navigator) -> Option<Self> {
            if let Some(mline) = nav.get_line(Some(46)) {
                let actiontype = match mline.get(0) {
                    Some(b'$') => ActionType::Pipeline,
                    Some(b'*') => ActionType::Simple,
                    _ => return None,
                };
                if let Some(sizes) = get_frame_sizes(&mline[1..]) {
                    return Some(Metaline {
                        content_size: sizes[0],
                        metalayout_size: sizes[1],
                        actiontype,
                    });
                }
            }
            None
        }
    }

    #[derive(Debug)]
    struct Metalayout(Vec<usize>);

    impl Metalayout {
        pub fn from_navigator(nav: &mut Navigator, mlayoutsize: usize) -> Option<Self> {
            if let Some(layout) = nav.get_line(Some(mlayoutsize)) {
                if let Some(skip_sequence) = get_skip_sequence(&layout) {
                    return Some(Metalayout(skip_sequence));
                }
            }
            None
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct Query {
        pub data: Vec<String>,
        pub actiontype: ActionType,
    }

    impl Query {
        pub fn from_navigator(mut nav: Navigator) -> QueryParseResult {
            if let Some(metaline) = Metaline::from_navigator(&mut nav) {
                if let Some(metalayout) =
                    Metalayout::from_navigator(&mut nav, metaline.metalayout_size)
                {
                    // We reduce the `get_exact`'s by one to avoid including the newline
                    if let Some(content) = nav.get_exact(metaline.content_size) {
                        let data = extract_idents(content, metalayout.0);
                        return QueryParseResult::Parsed((
                            Query {
                                data,
                                actiontype: metaline.actiontype,
                            },
                            nav.get_pos_usize(),
                        ));
                    } else {
                        return QueryParseResult::Incomplete;
                    }
                }
            }
            QueryParseResult::RespCode(RespCodes::InvalidMetaframe)
        }
    }

    fn get_frame_sizes(metaline: &[u8]) -> Option<Vec<usize>> {
        if let Some(s) = extract_sizes_splitoff(metaline, b'!', 2) {
            if s.len() == 2 {
                Some(s)
            } else {
                None
            }
        } else {
            None
        }
    }
    fn get_skip_sequence(metalayout: &[u8]) -> Option<Vec<usize>> {
        let l = metalayout.len() / 2;
        extract_sizes_splitoff(metalayout, b'#', l)
    }

    fn extract_sizes_splitoff(buf: &[u8], splitoff: u8, sizehint: usize) -> Option<Vec<usize>> {
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
    fn extract_idents(buf: &[u8], skip_sequence: Vec<usize>) -> Vec<String> {
        skip_sequence
            .into_iter()
            .scan(buf.into_iter(), |databuf, size| {
                let tok: Vec<u8> = databuf.take(size).map(|val| *val).collect();
                let _ = databuf.next();
                // FIXME(@ohsayan): This is quite slow, we'll have to use SIMD in the future
                Some(String::from_utf8_lossy(&tok).to_string())
            })
            .collect()
    }

    #[cfg(test)]
    #[test]
    fn test_navigator() {
        use bytes::BytesMut;
        let mut mybytes = BytesMut::from("*!5!2\n1#\nHEYA\n".as_bytes());
        let mut nav = Navigator::new(&mut mybytes);
        assert_eq!(Some("*!5!2".as_bytes()), nav.get_line(Some(46)));
        assert_eq!(Some("1#".as_bytes()), nav.get_line(Some(3)));
        assert_eq!(Some("HEYA".as_bytes()), nav.get_line(Some(5)));
    }

    #[cfg(test)]
    #[test]
    fn test_query() {
        use bytes::{Buf, BytesMut};
        let mut mybuf = BytesMut::from("*!14!7\n#3#5#3\nSET\nsayan\n123\n".as_bytes());
        let resulting_data_should_be: Vec<String> = "SET sayan 123"
            .split_whitespace()
            .map(|val| val.to_string())
            .collect();
        let nav = Navigator::new(&mut mybuf);
        let query = Query::from_navigator(nav);
        if let QueryParseResult::Parsed((query, forward)) = query {
            assert_eq!(
                query,
                Query {
                    data: resulting_data_should_be,
                    actiontype: ActionType::Simple,
                }
            );
            mybuf.advance(forward);
            assert_eq!(mybuf.len(), 0);
        } else {
            panic!("Query parsing failed");
        }
    }
}

use bytes::{Buf, BytesMut};
use corelib::terrapipe::{extract_idents, ActionType};
use corelib::terrapipe::{RespBytes, RespCodes};
use corelib::TResult;
use deserializer::{
    Navigator, Query,
    QueryParseResult::{self, *},
};
use std::io::{Cursor, Result as IoResult};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A TCP connection wrapper
pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

pub enum QueryResult {
    Q(Query),
    E(Vec<u8>),
}

impl Connection {
    /// Initiailize a new `Connection` instance
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }
    pub async fn read_query(&mut self) -> Result<QueryResult, String> {
        self.read_again().await?;
        loop {
            match self.try_query() {
                Parsed((query, forward)) => {
                    self.buffer.advance(forward);
                    return Ok(QueryResult::Q(query));
                }
                RespCode(r) => return Ok(QueryResult::E(r.into_response())),
                _ => (),
            }
            self.read_again().await?;
        }
    }
    fn try_query(&mut self) -> QueryParseResult {
        let mut nav = Navigator::new(&mut self.buffer);
        Query::from_navigator(nav)
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
