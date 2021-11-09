/*
 * Created on Wed May 12 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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
use crate::tokenizer;
use core::fmt;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use skytable::error::Error;
use skytable::types::Array;
use skytable::types::FlatElement;
use skytable::Pipeline;
use skytable::Query;
use skytable::{aio, Element, RespCode};

type SkyResult<T> = Result<T, Error>;

pub enum Runner {
    Insecure(aio::Connection),
    Secure(aio::TlsConnection),
}

impl Runner {
    pub async fn new_insecure(host: &str, port: u16) -> SkyResult<Self> {
        let con = aio::Connection::new(host, port).await?;
        Ok(Self::Insecure(con))
    }
    pub async fn new_secure(host: &str, port: u16, cert: &str) -> SkyResult<Self> {
        let con = aio::TlsConnection::new(host, port, cert).await?;
        Ok(Self::Secure(con))
    }
    pub async fn run_pipeline(&mut self, pipeline: Pipeline) {
        let ret = match self {
            Self::Insecure(con) => con.run_pipeline(pipeline).await,
            Self::Secure(con) => con.run_pipeline(pipeline).await,
        };
        let retok = match ret {
            Ok(r) => r,
            Err(e) => fatal!("An I/O error occurred while querying: {}", e),
        };
        for (idx, resp) in retok
            .into_iter()
            .enumerate()
            .map(|(idx, resp)| (idx + 1, resp))
        {
            println!("[Response {}]", idx);
            print_element(resp);
        }
    }
    pub async fn run_query(&mut self, unescaped: &str) {
        let query: Query = match tokenizer::get_query(unescaped.as_bytes()) {
            Ok(q) => q,
            Err(e) => {
                err!(format!("[Syntax Error: {}]\n", e));
                return;
            }
        };
        let ret = match self {
            Self::Insecure(con) => con.run_simple_query(&query).await,
            Self::Secure(con) => con.run_simple_query(&query).await,
        };
        match ret {
            Ok(resp) => print_element(resp),
            Err(e) => fatal!("An I/O error occurred while querying: {}", e),
        }
    }
}

fn print_element(el: Element) {
    match el {
        Element::String(st) => write_str!(st),
        Element::Binstr(st) => write_binstr!(st),
        Element::Array(Array::Bin(brr)) => print_bin_array(brr),
        Element::Array(Array::Str(srr)) => print_str_array(srr),
        Element::RespCode(r) => print_rcode(r, None),
        Element::UnsignedInt(int) => write_int!(int),
        Element::Array(Array::Flat(frr)) => write_flat_array(frr),
        Element::Array(Array::Recursive(a)) => print_array(a),
        _ => eskysh!("The server possibly sent a newer data type that we can't parse"),
    }
}

fn print_rcode(rcode: RespCode, idx: Option<usize>) {
    match rcode {
        RespCode::Okay => write_okay!(),
        RespCode::ActionError => write_err!(idx, "Action Error"),
        RespCode::ErrorString(st) => write_err!(idx, st),
        RespCode::OtherError => write_err!(idx, "Other Error"),
        RespCode::NotFound => write_err!(idx, "Not Found"),
        RespCode::OverwriteError => write_err!(idx, "Overwrite Error"),
        RespCode::PacketError => write_err!(idx, "Packet Error"),
        RespCode::ServerError => write_err!(idx, "Server Error"),
        RespCode::UnknownDataType => write_err!(idx, "Unknown data type"),
        _ => write_err!(idx, "Unknown error"),
    }
}

fn print_bin_array(bin_array: Vec<Option<Vec<u8>>>) {
    bin_array.into_iter().enumerate().for_each(|(idx, elem)| {
        let idx = idx + 1;
        match elem {
            Some(ele) => {
                write_binstr!(idx, ele);
            }
            None => print_rcode(RespCode::NotFound, Some(idx)),
        }
    })
}

fn print_str_array(str_array: Vec<Option<String>>) {
    str_array.into_iter().enumerate().for_each(|(idx, elem)| {
        let idx = idx + 1;
        match elem {
            Some(ele) => {
                write_str!(idx, ele);
            }
            None => print_rcode(RespCode::NotFound, Some(idx)),
        }
    })
}

fn write_flat_array(flat_array: Vec<FlatElement>) {
    for (idx, item) in flat_array.into_iter().enumerate() {
        let idx = idx + 1;
        match item {
            FlatElement::String(st) => write_str!(idx, st),
            FlatElement::Binstr(st) => {
                write_binstr!(idx, st)
            }
            FlatElement::RespCode(rc) => print_rcode(rc, Some(idx)),
            FlatElement::UnsignedInt(int) => write_int!(int, idx),
            _ => eskysh!("Element typed cannot yet be parsed"),
        }
    }
}

fn print_array(array: Vec<Element>) {
    for (idx, item) in array.into_iter().enumerate() {
        let idx = idx + 1;
        match item {
            Element::String(st) => write_str!(idx, st),
            Element::RespCode(rc) => print_rcode(rc, Some(idx)),
            Element::UnsignedInt(int) => write_int!(idx, int),
            Element::Array(Array::Bin(brr)) => print_bin_array(brr),
            Element::Array(Array::Str(srr)) => print_str_array(srr),
            _ => eskysh!("Nested arrays cannot be printed just yet"),
        }
    }
}

pub struct BinaryData(Vec<u8>);

impl fmt::Display for BinaryData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "b\"")?;
        for b in self.0.iter() {
            let b = *b;
            // See this: https://doc.rust-lang.org/reference/tokens.html#byte-escapes
            // this idea was borrowed from the Bytes crate
            #[allow(clippy::manual_range_contains)]
            if b == b'\n' {
                write!(f, "\\n")?;
            } else if b == b'\r' {
                write!(f, "\\r")?;
            } else if b == b'\t' {
                write!(f, "\\t")?;
            } else if b == b'\\' || b == b'"' {
                write!(f, "\\{}", b as char)?;
            } else if b == b'\0' {
                write!(f, "\\0")?;
            // ASCII printable
            } else if b >= 0x20 && b < 0x7f {
                write!(f, "{}", b as char)?;
            } else {
                write!(f, "\\x{:02x}", b)?;
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
}
