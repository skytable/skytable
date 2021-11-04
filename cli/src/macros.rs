/*
 * Created on Wed Nov 03 2021
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

macro_rules! write_str {
    ($st:ident) => {
        println!("\"{}\"", $st)
    };
    ($idx:ident, $st:ident) => {
        println!("({}) \"{}\"", $idx, $st)
    };
}

macro_rules! write_binstr {
    ($st:ident) => {
        println!("{}", BinaryData($st))
    };
    ($idx:ident, $st:ident) => {
        println!("({}) {}", $idx, BinaryData($st))
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
        err!(if let Some(idx) = $idx {
            format!("({}) ({})\n", idx, $err)
        } else {
            format!("({})\n", $err)
        })
    };
    ($idx:ident, $err:literal) => {
        err!(
            (if let Some(idx) = $idx {
                format!("({}) ({})\n", idx, $err)
            } else {
                format!("({})\n", $err)
            })
        )
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

macro_rules! err {
    ($input:expr) => {
        crossterm::execute!(
            std::io::stdout(),
            ::crossterm::style::SetForegroundColor(::crossterm::style::Color::Red),
            ::crossterm::style::Print($input),
            ::crossterm::style::ResetColor
        )
        .expect("Failed to write to stdout")
    };
}

macro_rules! eskysh {
    ($e:expr) => {
        err!(format!("[SKYSH ERROR] {}\n", $e))
    };
}

macro_rules! fatal {
    ($e:expr) => {{
        err!($e);
        ::std::process::exit(0x01);
    }};
    ($e:expr, $desc:expr) => {{
        err!(format!($e, $desc));
        println!();
        ::std::process::exit(0x01)
    }};
}

macro_rules! readln {
    ($editor:expr) => {
        match $editor.readline(SKYSH_BLANK) {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => return,
            Err(err) => fatal!("ERROR: Failed to read line with error: {}", err),
        }
    };
}
