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
use core::future::Future;
use core::pin::Pin;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use skytable::error::Error;
use skytable::types::Array;
use skytable::types::FlatElement;
use skytable::Query;
use skytable::{aio, Element, RespCode};

pub struct Runner<T: AsyncSocket> {
    con: T,
}

pub trait AsyncSocket {
    fn run_simple_query<'s>(
        &'s mut self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<Element, Error>> + Send + Sync + 's>>;
}

impl AsyncSocket for aio::Connection {
    fn run_simple_query<'s>(
        &'s mut self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<Element, Error>> + Send + Sync + 's>> {
        Box::pin(async move { self.run_simple_query(&query).await })
    }
}

impl AsyncSocket for aio::TlsConnection {
    fn run_simple_query<'s>(
        &'s mut self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<Element, Error>> + Send + Sync + 's>> {
        Box::pin(async move { self.run_simple_query(&query).await })
    }
}

macro_rules! write_string {
    ($st:ident) => {
        println!("\"{}\"", $st)
    };
    ($idx:ident, $st:ident) => {
        println!("({}) \"{}\"", $idx, $st)
    };
}

macro_rules! write_int {
    ($int:ident) => {
        println!("{}", $int)
    };
    ($idx:ident, $st:ident) => {
        println!("({}) \"{}\"", $idx, $st)
    };
}

macro_rules! write_err {
    ($idx:expr, $err:ident) => {
        crossterm::execute!(
            std::io::stdout(),
            SetForegroundColor(Color::Red),
            Print(if let Some(idx) = $idx {
                format!("({}) ({})\n", idx, $err)
            } else {
                format!("({})\n", $err)
            }),
            ResetColor
        )
        .expect("Failed to write to stdout")
    };
    ($idx:ident, $err:literal) => {
        crossterm::execute!(
            std::io::stdout(),
            SetForegroundColor(Color::Red),
            Print(
                (if let Some(idx) = $idx {
                    format!("({}) ({})\n", idx, $err)
                } else {
                    format!("({})\n", $err)
                })
            ),
            ResetColor
        )
        .expect("Failed to write to stdout")
    };
}

macro_rules! str {
    ($in:expr) => {
        String::from_utf8_lossy(&$in)
    };
}

macro_rules! write_okay {
    () => {
        crossterm::execute!(
            std::io::stdout(),
            SetForegroundColor(Color::Cyan),
            Print("(Okay)\n".to_string()),
            ResetColor
        )
        .expect("Failed to write to stdout")
    };
    ($idx:ident) => {
        crossterm::execute!(
            std::io::stdout(),
            SetForegroundColor(Color::Cyan),
            Print(format!("({}) (Okay)\n", $idx)),
            ResetColor
        )
        .expect("Failed to write to stdout")
    };
}

macro_rules! eskysh {
    ($e:expr) => {
        eprintln!("[SKYSH ERROR] {}", $e)
    };
}

impl<T: AsyncSocket> Runner<T> {
    pub fn new(con: T) -> Self {
        Runner { con }
    }
    pub async fn run_query(&mut self, unescaped_items: &str) {
        let query = libsky::turn_into_query(unescaped_items);
        match self.con.run_simple_query(query).await {
            Ok(resp) => match resp {
                Element::String(st) => write_string!(st),
                Element::Binstr(st) => {
                    let st = String::from_utf8_lossy(&st);
                    write_string!(st)
                }
                Element::Array(Array::Bin(brr)) => print_bin_array(brr),
                Element::Array(Array::Str(srr)) => print_str_array(srr),
                Element::RespCode(r) => print_rcode(r, None),
                Element::UnsignedInt(int) => write_int!(int),
                Element::Array(Array::Flat(frr)) => write_flat_array(frr),
                Element::Array(Array::Recursive(a)) => print_array(a),
                _ => eskysh!("The server possibly sent a newer data type that we can't parse"),
            },
            Err(e) => {
                eprintln!("An I/O error occurred while querying: {}", e);
                std::process::exit(1);
            }
        }
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
        _ => write_err!(idx, "Unknown Error"),
    }
}

fn print_bin_array(bin_array: Vec<Option<Vec<u8>>>) {
    bin_array.into_iter().enumerate().for_each(|(idx, elem)| {
        let idx = idx + 1;
        match elem {
            Some(ele) => {
                let st = String::from_utf8_lossy(&ele);
                println!("({}) {}", idx, st)
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
                println!("({}) {}", idx, ele)
            }
            None => print_rcode(RespCode::NotFound, Some(idx)),
        }
    })
}

fn write_flat_array(flat_array: Vec<FlatElement>) {
    for (idx, item) in flat_array.into_iter().enumerate() {
        let idx = idx + 1;
        match item {
            FlatElement::String(st) => write_string!(idx, st),
            FlatElement::Binstr(st) => {
                let st = str!(st);
                write_string!(idx, st)
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
            Element::String(st) => write_string!(idx, st),
            Element::RespCode(rc) => print_rcode(rc, Some(idx)),
            Element::UnsignedInt(int) => write_int!(idx, int),
            Element::Array(Array::Bin(brr)) => print_bin_array(brr),
            Element::Array(Array::Str(srr)) => print_str_array(srr),
            _ => eskysh!("Nested arrays cannot be printed just yet"),
        }
    }
}
