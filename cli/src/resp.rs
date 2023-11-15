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
    crate::error::CliResult,
    crossterm::{
        style::{Color, ResetColor, SetForegroundColor},
        ExecutableCommand,
    },
    skytable::response::{Response, Row, Value},
    std::io::{self, Write},
};

pub fn format_response(resp: Response) -> CliResult<()> {
    match resp {
        Response::Empty => print_cyan("(Okay)\n")?,
        Response::Error(e) => print_red(&format!("(server error code: {e})\n"))?,
        Response::Value(v) => {
            print_value(v)?;
            println!();
        }
        Response::Row(r) => {
            print_row(r)?;
            println!();
        }
    };
    Ok(())
}

fn print_row(r: Row) -> CliResult<()> {
    print!("(");
    let mut columns = r.into_values().into_iter().peekable();
    while let Some(cell) = columns.next() {
        print_value(cell)?;
        if columns.peek().is_some() {
            print!(", ");
        }
    }
    print!(")");
    Ok(())
}

fn print_value(v: Value) -> CliResult<()> {
    match v {
        Value::Null => print_gray("null")?,
        Value::String(s) => print_string(&s),
        Value::Binary(b) => print_binary(&b),
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
                print_value(item)?;
                if items.peek().is_some() {
                    print!(", ");
                }
            }
            print!("]");
        }
    }
    Ok(())
}

fn print_binary(b: &[u8]) {
    let mut it = b.into_iter().peekable();
    print!("[");
    while let Some(byte) = it.next() {
        print!("{byte}");
        if it.peek().is_some() {
            print!(", ");
        }
    }
    print!("]");
}

fn print_string(s: &str) {
    print!("\"");
    for ch in s.chars() {
        if ch == '"' || ch == '\'' {
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

fn print_gray(s: &str) -> std::io::Result<()> {
    print_colored_text(s, Color::White)
}

fn print_red(s: &str) -> std::io::Result<()> {
    print_colored_text(s, Color::Red)
}

fn print_cyan(s: &str) -> std::io::Result<()> {
    print_colored_text(s, Color::Cyan)
}

fn print_colored_text(text: &str, color: Color) -> std::io::Result<()> {
    let mut stdout = io::stdout();
    stdout.execute(SetForegroundColor(color))?;
    print!("{text}");
    stdout.flush()?;
    stdout.execute(ResetColor)?;
    Ok(())
}
