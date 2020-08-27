/*
 * Created on Tue Aug 04 2020
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

mod deserializer;
use bytes::{Buf, BytesMut};
use deserializer::ClientResult;
use lazy_static::lazy_static;
use libtdb::TResult;
use libtdb::BUF_CAP;
use regex::Regex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

lazy_static! {
    static ref RE: Regex = Regex::new("[^\\s\"']+|\"[^\"]*\"|'[^']*'").unwrap();
}

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub async fn new(host: &str) -> TResult<Self> {
        let stream = TcpStream::connect(host).await?;
        Ok(Connection {
            stream,
            buffer: BytesMut::with_capacity(BUF_CAP),
        })
    }
    pub async fn run_query(&mut self, query: String) {
        let query = proc_query(query);
        match self.stream.write_all(&query).await {
            Ok(_) => (),
            Err(_) => {
                eprintln!("ERROR: Couldn't write data to socket");
                return;
            }
        };
        loop {
            match self.stream.read_buf(&mut self.buffer).await {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                    return;
                }
            }
            match self.try_response().await {
                ClientResult::Empty(f) => {
                    self.buffer.advance(f);
                    eprintln!("ERROR: The remote end reset the connection");
                    return;
                }
                ClientResult::Incomplete => {
                    continue;
                }
                ClientResult::Response(r, f) => {
                    self.buffer.advance(f);
                    if r.len() == 0 {
                        return;
                    }
                    for group in r {
                        println!("{}", group);
                    }
                    return;
                }
                ClientResult::InvalidResponse(_) => {
                    self.buffer.clear();
                    eprintln!("{}", ClientResult::InvalidResponse(0));
                    return;
                }
            }
        }
    }
    async fn try_response(&mut self) -> ClientResult {
        if self.buffer.is_empty() {
            // The connection was possibly reset
            return ClientResult::Empty(0);
        }
        deserializer::parse(&self.buffer)
    }
}

fn proc_query(querystr: String) -> Vec<u8> {
    // TODO(@ohsayan): Enable "" to be escaped
    // let args: Vec<&str> = RE.find_iter(&querystr).map(|val| val.as_str()).collect();
    let args: Vec<&str> = querystr.split_whitespace().collect();
    let mut bytes = Vec::with_capacity(querystr.len());
    bytes.extend(b"#2\n*1\n#");
    let arg_len_bytes = args.len().to_string().into_bytes();
    let arg_len_bytes_len = (arg_len_bytes.len() + 1).to_string().into_bytes();
    bytes.extend(arg_len_bytes_len);
    bytes.extend(b"\n&");
    bytes.extend(arg_len_bytes);
    bytes.push(b'\n');
    args.into_iter().for_each(|arg| {
        bytes.push(b'#');
        let len_bytes = arg.len().to_string().into_bytes();
        bytes.extend(len_bytes);
        bytes.push(b'\n');
        bytes.extend(arg.as_bytes());
        bytes.push(b'\n');
    });
    bytes
}

#[test]
fn test_queryproc() {
    let query = "GET x y".to_owned();
    assert_eq!(
        "#2\n*1\n#2\n&3\n#3\nGET\n#1\nx\n#1\ny\n"
            .as_bytes()
            .to_owned(),
        proc_query(query)
    );
}
