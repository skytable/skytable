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
use libtdb::terrapipe;
use libtdb::TResult;
use libtdb::BUF_CAP;
use openssl::ssl::Ssl;
use openssl::ssl::SslContext;
use openssl::ssl::SslMethod;
use regex::Regex;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

lazy_static! {
    static ref RE: Regex = Regex::new("[^\\s\"']+|\"[^\"]*\"|'[^']*'").unwrap();
}

pub enum Con {
    Secure(SslConnection),
    Insecure(Connection),
}

impl Con {
    pub async fn execute_query(&mut self, query: String) {
        match self {
            Con::Insecure(con) => con.run_query(query).await,
            Con::Secure(con) => con.run_query(query).await,
        }
    }
}

/// A `Connection` is a wrapper around a`TcpStream` and a read buffer
pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    /// Create a new connection, creating a connection to `host`
    pub async fn new(host: &str) -> TResult<Self> {
        let stream = TcpStream::connect(host).await?;
        println!("Connected to tp://{}", host);
        Ok(Connection {
            stream,
            buffer: BytesMut::with_capacity(BUF_CAP),
        })
    }
    /// This function will write a query to the stream and read the response from the
    /// server. It will then determine if the returned response is complete or incomplete
    /// or invalid.
    ///
    /// - If it is complete, then the return is parsed into a `Display`able form
    /// and written to the output stream. If any parsing errors occur, they're also handled
    /// by this function (usually, "Invalid Response" is written to the terminal).
    /// - If the packet is incomplete, it will wait to read the entire response from the stream
    /// - If the packet is corrupted, it will output "Invalid Response"
    pub async fn run_query(&mut self, query: String) {
        let query = terrapipe::proc_query(query);
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
    /// This function is a subroutine of `run_query` used to parse the response packet
    async fn try_response(&mut self) -> ClientResult {
        if self.buffer.is_empty() {
            // The connection was possibly reset
            return ClientResult::Empty(0);
        }
        deserializer::parse(&self.buffer)
    }
}

/// An `SslConnection` is a wrapper around a `SslStream<TcpStream>` provided by OpenSSL and a
/// read buffer
pub struct SslConnection {
    stream: SslStream<TcpStream>,
    buffer: BytesMut,
}

impl SslConnection {
    /// Create a new connection, creating a connection to `host`
    pub async fn new(host: &str, sslcert: &str) -> TResult<Self> {
        let mut ctx = SslContext::builder(SslMethod::tls_client())?;
        ctx.set_ca_file(sslcert)?;
        let ssl = Ssl::new(&ctx.build())?;
        let stream = TcpStream::connect(host).await?;
        let mut stream = SslStream::new(ssl, stream)?;
        Pin::new(&mut stream).connect().await.unwrap();
        println!("Connected to tps://{}", host);
        Ok(SslConnection {
            stream,
            buffer: BytesMut::with_capacity(BUF_CAP),
        })
    }
    /// This function will write a query to the stream and read the response from the
    /// server. It will then determine if the returned response is complete or incomplete
    /// or invalid.
    ///
    /// - If it is complete, then the return is parsed into a `Display`able form
    /// and written to the output stream. If any parsing errors occur, they're also handled
    /// by this function (usually, "Invalid Response" is written to the terminal).
    /// - If the packet is incomplete, it will wait to read the entire response from the stream
    /// - If the packet is corrupted, it will output "Invalid Response"
    pub async fn run_query(&mut self, query: String) {
        let query = terrapipe::proc_query(query);
        match self.stream.write_all(&query).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("ERROR: Couldn't write data to socket with '{}'", e);
                return;
            }
        };
        loop {
            if let Err(e) = self.read_again().await {
                eprintln!("ERROR: Reading from stream failed with: '{}'", e);
                return;
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
                ClientResult::InvalidResponse(r) => {
                    self.buffer.advance(r);
                    eprintln!("{}", ClientResult::InvalidResponse(0));
                    return;
                }
            }
        }
    }
    /// This function is a subroutine of `run_query` used to parse the response packet
    async fn try_response(&mut self) -> ClientResult {
        if self.buffer.is_empty() {
            // The connection was possibly reset
            return ClientResult::Empty(0);
        }
        deserializer::parse(&self.buffer)
    }
    async fn read_again(&mut self) -> Result<(), String> {
        match self.stream.read_buf(&mut self.buffer).await {
            Ok(0) => {
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
    fn get_peer(&self) -> IoResult<SocketAddr> {
        self.stream.get_ref().peer_addr()
    }
}
