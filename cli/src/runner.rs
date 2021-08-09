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
use skytable::Query;
use skytable::{aio, Element, RespCode, Response};
use std::io::Error as IoError;

pub struct Runner<T: AsyncSocket> {
    con: T,
}

pub trait AsyncSocket {
    fn run_simple_query<'s>(
        &'s mut self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<Response, IoError>> + Send + Sync + 's>>;
}

impl AsyncSocket for aio::Connection {
    fn run_simple_query<'s>(
        &'s mut self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<Response, IoError>> + Send + Sync + 's>> {
        Box::pin(async move { self.run_simple_query(&query).await })
    }
}

impl AsyncSocket for aio::TlsConnection {
    fn run_simple_query<'s>(
        &'s mut self,
        query: Query,
    ) -> Pin<Box<dyn Future<Output = Result<Response, IoError>> + Send + Sync + 's>> {
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

impl<T: AsyncSocket> Runner<T> {
    pub fn new(con: T) -> Self {
        Runner { con }
    }
    pub async fn run_query(&mut self, unescaped_items: &str) {
        let query = libsky::turn_into_query(unescaped_items);
        match self.con.run_simple_query(query).await {
            Ok(resp) => match resp {
                Response::InvalidResponse => {
                    println!("ERROR: The server sent an invalid response");
                }
                Response::Item(element) => match element {
                    Element::String(st) => write_string!(st),
                    Element::FlatArray(arr) => print_flat_array(arr),
                    Element::RespCode(r) => print_rcode(r, None),
                    Element::UnsignedInt(int) => write_int!(int),
                    Element::Array(a) => print_array(a),
                    _ => unimplemented!(),
                },
                Response::ParseError => {
                    println!("ERROR: The client failed to deserialize data sent by the server")
                }
                x => {
                    println!(
                        "The server possibly sent a newer data type that we can't parse: {:?}",
                        x
                    )
                }
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

fn print_flat_array(flat_array: Vec<String>) {
    flat_array.into_iter().enumerate().for_each(|(idx, item)| {
        let idx = idx + 1;
        write_string!(idx, item)
    })
}
fn print_array(array: Vec<Element>) {
    for (idx, item) in array.into_iter().enumerate() {
        let idx = idx + 1;
        match item {
            Element::String(st) => write_string!(idx, st),
            Element::RespCode(rc) => print_rcode(rc, Some(idx)),
            Element::UnsignedInt(int) => write_int!(idx, int),
            Element::FlatArray(a) => print_flat_array(a),
            _ => unimplemented!("Nested arrays cannot be printed just yet"),
        }
    }
}
