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
    crossterm::style::Stylize,
    skytable::response::{Response, Row, Value},
};

macro_rules! pprint {
    ($pretty:expr, $base:literal$(.$f:ident())*) => {
        if $pretty {
            let pretty = $base$(.$f())*;
            println!("{}", pretty);
        } else {
            println!("{}", $base);
        }
    }
}

pub fn format_response(resp: Response, print_special: bool, in_repl: bool) -> bool {
    match resp {
        Response::Empty => {
            if in_repl {
                println!("{}", "(Okay)".cyan())
            }
            // for empty responses outside the repl, it's equivalent to an exit 0, so we don't output anything to stdout or stderr
        }
        Response::Error(e) => {
            if in_repl {
                println!("{}", format!("(server error code: {e})").red());
            } else {
                // outside the repl, just write the error code to stderr. note, the query was technically "successful" because the server received it
                // and responded to it. but on the application end, it was not. so, no need for a nonzero exit code
                eprintln!("{e}");
            }
            return false;
        }
        Response::Value(v) => {
            print_value(v, print_special, in_repl);
            println!();
        }
        Response::Row(r) => {
            print_row(r, in_repl);
            println!();
        }
        Response::Rows(rows) => {
            if rows.is_empty() {
                pprint!(in_repl, "[0 rows returned]".grey().italic());
            } else {
                for (i, row) in rows.into_iter().enumerate().map(|(i, r)| (i + 1, r)) {
                    if in_repl {
                        let fmt = format!("({i}) ").grey().italic();
                        print!("{fmt}")
                    }
                    print_row(row, in_repl);
                    println!();
                }
            }
        }
    };
    true
}

fn print_row(r: Row, pretty_format: bool) {
    if pretty_format {
        print!("(");
    }
    let mut columns = r.into_values().into_iter().peekable();
    while let Some(cell) = columns.next() {
        print_value(cell, false, pretty_format);
        if columns.peek().is_some() {
            if pretty_format {
                print!(", ");
            } else {
                print!(",");
            }
        }
    }
    if pretty_format {
        print!(")");
    }
}

fn print_value(v: Value, print_special: bool, in_repl: bool) {
    match v {
        Value::Null => pprint!(in_repl, "null".grey().italic()),
        Value::String(s) => print_string(&s, print_special, in_repl),
        Value::Binary(b) => print_binary(&b, !in_repl),
        Value::Bool(b) => print!("{b}"),
        Value::UInt8(i) => print!("{i}"),
        Value::UInt16(i) => print!("{i}"),
        Value::UInt32(i) => print!("{i}"),
        Value::UInt64(i) => print!("{i}"),
        Value::SInt8(i) => print!("{i}"),
        Value::SInt16(i) => print!("{i}"),
        Value::SInt32(i) => print!("{i}"),
        Value::SInt64(i) => print!("{i}"),
        Value::Float32(f) => print!("{f}"),
        Value::Float64(f) => print!("{f}"),
        Value::List(items) => {
            print!("[");
            let mut items = items.into_iter().peekable();
            while let Some(item) = items.next() {
                print_value(item, print_special, in_repl);
                if items.peek().is_some() {
                    print!(", ");
                }
            }
            print!("]");
        }
    }
}

fn print_binary(b: &[u8], escape: bool) {
    let mut it = b.into_iter().peekable();
    if escape {
        print!("\"[");
    } else {
        print!("[");
    }
    while let Some(byte) = it.next() {
        print!("{byte}");
        if it.peek().is_some() {
            if escape {
                print!(",");
            } else {
                print!(", ");
            }
        }
    }
    if escape {
        print!("]\"");
    } else {
        print!("]");
    }
}

fn print_string(s: &str, print_special: bool, pretty_format: bool) {
    if print_special {
        if pretty_format {
            print!("{}", s.italic().grey());
        }
    } else {
        print!("\"");
        for ch in s.chars() {
            if ch == '"' {
                print!("\\{ch}");
            } else if ch == '\t' {
                print!("\\t");
            } else if ch == '\n' {
                print!("\\n");
            } else {
                print!("{ch}");
            }
        }
        print!("\"");
    }
}
