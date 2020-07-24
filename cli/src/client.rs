/*
 * Created on Thu Jul 23 2020
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

use corelib::{
    terrapipe::{self, ActionType, QueryBuilder, RespCodes, DEF_QMETALAYOUT_BUFSIZE},
    TResult,
};
use std::{error::Error, fmt};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Errors that may occur while parsing responses from the server
#[derive(Debug)]
pub enum ClientError {
    RespCode(RespCodes),
    InvalidResponse,
    OtherError(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, mut f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ClientError::*;
        match self {
            RespCode(r) => r.fmt(&mut f),
            InvalidResponse => write!(f, "ERROR: The server sent an invalid response"),
            OtherError(e) => write!(f, "ERROR: {}", e),
        }
    }
}

impl Error for ClientError {}

/// A client
pub struct Client {
    con: TcpStream,
}

/// The Request metaline
pub struct RMetaline {
    content_size: usize,
    metalayout_size: usize,
    respcode: RespCodes,
    resptype: ActionType,
}

impl RMetaline {
    /// Decode a metaline from a `String` buffer
    pub fn from_buf(buf: String) -> TResult<Self> {
        let parts: Vec<&str> = buf.split('!').collect();
        if let (Some(resptype), Some(respcode), Some(clength), Some(metalayout_size)) =
            (parts.get(0), parts.get(1), parts.get(2), parts.get(3))
        {
            if resptype == &"$" {
                todo!("Pipelined responses are yet to be implemented");
            }
            if resptype != &"*" {
                return Err(ClientError::InvalidResponse.into());
            }
            if let (Some(respcode), Ok(clength), Ok(metalayout_size)) = (
                RespCodes::from_str(respcode, None),
                clength.trim_matches(char::from(0)).trim().parse::<usize>(),
                metalayout_size
                    .trim_matches(char::from(0))
                    .trim()
                    .parse::<usize>(),
            ) {
                return Ok(RMetaline {
                    content_size: clength,
                    metalayout_size,
                    respcode,
                    resptype: ActionType::Simple,
                });
            } else {
                Err(ClientError::InvalidResponse.into())
            }
        } else {
            Err(ClientError::InvalidResponse.into())
        }
    }
}

impl Client {
    /// Create a new client instance
    pub async fn new(addr: &str) -> TResult<Self> {
        let con = TcpStream::connect(addr).await?;
        Ok(Client { con })
    }
    /// Run a query read from stdin. This function will take care of everything
    /// including printing errors
    pub async fn run(&mut self, cmd: String) {
        if cmd.len() == 0 {
            return;
        } else {
            let mut qbuilder = QueryBuilder::new_simple();
            qbuilder.from_cmd(cmd);
            match self.run_query(qbuilder.prepare_response()).await {
                Ok(res) => {
                    res.into_iter().for_each(|val| println!("{}", val));
                    return;
                }
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };
        }
    }
    /// Run a query, reading and writng to the stream
    async fn run_query(&mut self, (_, query_bytes): (usize, Vec<u8>)) -> TResult<Vec<String>> {
        self.con.write_all(&query_bytes).await?;
        let mut metaline_buf = String::with_capacity(DEF_QMETALAYOUT_BUFSIZE);
        let mut bufreader = BufReader::new(&mut self.con);
        bufreader.read_line(&mut metaline_buf).await?;
        let metaline = RMetaline::from_buf(metaline_buf)?;
        // Skip reading the rest of the data if the metaline says that there is an
        // error. WARNING: This would mean that any other data sent - would simply be
        // ignored
        let mut is_other_error = false;
        match metaline.respcode {
            // Only these two variants have some data in the dataframe, so we continue
            RespCodes::Okay => (),
            RespCodes::OtherError(_) => is_other_error = true,
            code @ _ => return Err(code.into()),
        }
        if metaline.content_size == 0 {
            return Ok(vec![]);
        }
        let (mut metalayout, mut dataframe) = (
            String::with_capacity(metaline.metalayout_size),
            vec![0u8; metaline.content_size],
        );
        bufreader.read_line(&mut metalayout).await?;
        let metalayout = terrapipe::get_sizes(metalayout)?;
        bufreader.read_exact(&mut dataframe).await?;
        if is_other_error {
            Err(ClientError::OtherError(String::from_utf8_lossy(&dataframe).to_string()).into())
        } else {
            Ok(terrapipe::extract_idents(dataframe, metalayout))
        }
    }
}
