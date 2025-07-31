/*
 * Created on Thu Nov 16 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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
    crate::{
        args::{ClientConfig, EndpointConfig},
        error::{CliError, CliResult},
    },
    skytable::{
        error::ClientResult, query::SQParam, response::Response, Config, Connection, ConnectionTls,
        Query,
    },
};

pub fn connect<T>(
    cfg: ClientConfig,
    print_con_info: bool,
    tcp_f: impl Fn(Connection) -> CliResult<T>,
    tls_f: impl Fn(ConnectionTls) -> CliResult<T>,
) -> CliResult<T> {
    match cfg.kind {
        EndpointConfig::Tcp(host, port) => {
            let c = Config::new(&host, port, &cfg.username, &cfg.password).connect()?;
            if print_con_info {
                println!(
                    "Authenticated as '{}' on {}:{} over Skyhash/TCP\n---",
                    &cfg.username, &host, &port
                );
            }
            tcp_f(c)
        }
        EndpointConfig::Tls(host, port, cert) => {
            let c = Config::new(&host, port, &cfg.username, &cfg.password).connect_tls(&cert)?;
            if print_con_info {
                println!(
                    "Authenticated as '{}' on {}:{} over Skyhash/TLS\n---",
                    &cfg.username, &host, &port
                );
            }
            tls_f(c)
        }
    }
}

pub trait IsConnection {
    fn execute_query(&mut self, q: Query) -> ClientResult<Response>;
}

impl IsConnection for Connection {
    fn execute_query(&mut self, q: Query) -> ClientResult<Response> {
        self.query(&q)
    }
}

impl IsConnection for ConnectionTls {
    fn execute_query(&mut self, q: Query) -> ClientResult<Response> {
        self.query(&q)
    }
}

#[derive(Debug, PartialEq)]
enum Item {
    UInt(u64),
    SInt(i64),
    Float(f64),
    String(String),
    Bin(Vec<u8>),
}

impl SQParam for Item {
    fn append_param(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Item::UInt(u) => u.append_param(buf),
            Item::SInt(s) => s.append_param(buf),
            Item::Float(f) => f.append_param(buf),
            Item::String(s) => s.append_param(buf),
            Item::Bin(b) => SQParam::append_param(&*b, buf),
        }
    }
}

pub struct Parameterizer {
    buf: Vec<u8>,
    i: usize,
    params: Vec<Item>,
    query: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum ExecKind {
    Standard(Query),
    UseSpace(Query, String),
    UseNull(Query),
    PrintSpecial(Query),
}

impl ExecKind {
    pub fn into_query(self) -> Query {
        match self {
            Self::Standard(q) | Self::UseSpace(q, _) | Self::UseNull(q) | Self::PrintSpecial(q) => {
                q
            }
        }
    }
}

impl Parameterizer {
    pub fn new(q: String) -> Self {
        Self {
            buf: q.into_bytes(),
            i: 0,
            params: vec![],
            query: vec![],
        }
    }
    pub fn parameterize(mut self) -> CliResult<ExecKind> {
        while self.not_eof() {
            match self.buf[self.i] {
                b if b.is_ascii_alphabetic() || b == b'_' => self.read_ident(),
                b if b.is_ascii_digit() => self.read_unsigned_integer(),
                b'-' => self.read_signed_integer(),
                quote_style @ (b'"' | b'\'') => {
                    self.i += 1;
                    self.read_string(quote_style)
                }
                b'`' => {
                    self.i += 1;
                    self.read_binary()
                }
                sym => {
                    self.i += 1;
                    Vec::push(&mut self.query, sym);
                    Ok(())
                }
            }?
        }
        match String::from_utf8(self.query) {
            Ok(qstr) => {
                let mut q = Query::new(&qstr);
                self.params.into_iter().for_each(|p| {
                    q.push_param(p);
                });
                Ok(if qstr.eq_ignore_ascii_case("use null") {
                    ExecKind::UseNull(q)
                } else {
                    if qstr.len() > 8 {
                        let qstr = &qstr[..8];
                        if qstr.eq_ignore_ascii_case("inspect ") {
                            return Ok(ExecKind::PrintSpecial(q));
                        }
                    }
                    let mut splits = qstr.split_ascii_whitespace();
                    let tok_use = splits.next();
                    let tok_name = splits.next();
                    match (tok_use, tok_name) {
                        (Some(tok_use), Some(tok_name))
                            if tok_use.eq_ignore_ascii_case("use")
                                && !tok_name.eq_ignore_ascii_case("$current") =>
                        {
                            ExecKind::UseSpace(q, tok_name.into())
                        }
                        _ => ExecKind::Standard(q),
                    }
                })
            }
            Err(_) => Err(CliError::QueryError("query is not valid UTF-8".into())),
        }
    }
    fn read_string(&mut self, quote_style: u8) -> CliResult<()> {
        self.query.push(b'?');
        let mut string = Vec::new();
        let mut terminated = false;
        while self.not_eof() && !terminated {
            let b = self.buf[self.i];
            if b == b'\\' {
                self.i += 1;
                // escape sequence
                if self.i == self.buf.len() {
                    // string was not terminated
                    return Err(CliError::QueryError("string not terminated".into()));
                }
                match self.buf[self.i] {
                    b'\\' => {
                        // escaped \
                        string.push(b'\\');
                    }
                    b if b == quote_style => {
                        // escape quote
                        string.push(quote_style);
                    }
                    _ => return Err(CliError::QueryError("unknown escape sequence".into())),
                }
            }
            if b == quote_style {
                terminated = true;
            } else {
                string.push(b);
            }
            self.i += 1;
        }
        if terminated {
            match String::from_utf8(string) {
                Ok(s) => self.params.push(Item::String(s)),
                Err(_) => return Err(CliError::QueryError("invalid UTF-8 string".into())),
            }
            Ok(())
        } else {
            return Err(CliError::QueryError("string not terminated".into()));
        }
    }
    fn read_ident(&mut self) -> CliResult<()> {
        // we're looking at an ident
        let start = self.i;
        self.i += 1;
        while self.not_eof() {
            if self.buf[self.i].is_ascii_alphanumeric() || self.buf[self.i] == b'_' {
                self.i += 1;
            } else {
                break;
            }
        }
        let stop = self.i;
        self.query.extend(&self.buf[start..stop]);
        Ok(())
    }
    fn read_float(&mut self, start: usize) -> CliResult<()> {
        self.read_until_number_escape();
        let stop = self.i;
        match core::str::from_utf8(&self.buf[start..stop]).map(|v| v.parse()) {
            Ok(Ok(num)) => self.params.push(Item::Float(num)),
            _ => {
                return Err(CliError::QueryError(
                    "invalid floating point literal".into(),
                ))
            }
        }
        Ok(())
    }
    fn read_signed_integer(&mut self) -> CliResult<()> {
        self.query.push(b'?');
        // we must have encountered a `-`
        let start = self.i;
        self.read_until_number_escape();
        let stop = self.i;
        match core::str::from_utf8(&self.buf[start..stop]).map(|v| v.parse()) {
            Ok(Ok(s)) => self.params.push(Item::SInt(s)),
            _ => {
                return Err(CliError::QueryError(
                    "invalid signed integer literal".into(),
                ))
            }
        }
        Ok(())
    }
    fn read_unsigned_integer(&mut self) -> CliResult<()> {
        self.query.push(b'?');
        let start = self.i;
        let mut ret = 0u64;
        while self.not_eof() {
            match self.buf[self.i] {
                b if b.is_ascii_digit() => {
                    self.i += 1;
                    ret = match ret
                        .checked_mul(10)
                        .map(|v| v.checked_add((b & 0x0f) as u64))
                    {
                        Some(Some(r)) => r,
                        _ => return Err(CliError::QueryError("bad value for integer".into())),
                    };
                }
                b'.' => {
                    self.i += 1;
                    // uh oh, that's a float
                    return self.read_float(start);
                }
                b if b == b' ' || b == b'\t' || b.is_ascii_punctuation() => {
                    break;
                }
                _ => {
                    // nothing else is valid here
                    return Err(CliError::QueryError(
                        "invalid unsigned integer literal".into(),
                    ));
                }
            }
        }
        self.params.push(Item::UInt(ret));
        Ok(())
    }
    fn read_until_number_escape(&mut self) {
        while self.not_eof() {
            let b = self.buf[self.i];
            if b == b'\n' || b == b'\t' || b.is_ascii_punctuation() {
                break;
            }
            self.i += 1;
        }
    }
    fn read_binary(&mut self) -> CliResult<()> {
        self.query.push(b'?');
        let start = self.i;
        while self.not_eof() {
            let b = self.buf[self.i];
            self.i += 1;
            if b == b'`' {
                self.params
                    .push(Item::Bin(self.buf[start..self.i-1].to_vec()));
                return Ok(());
            }
        }
        Err(CliError::QueryError("binary literal not terminated".into()))
    }
    fn not_eof(&self) -> bool {
        self.i < self.buf.len()
    }
}
