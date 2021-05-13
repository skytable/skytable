/*
 * Created on Wed Jul 01 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

use crate::protocol;
use clap::load_yaml;
use clap::App;
use std::io::stdout;
use libsky::terrapipe::ADDR;
use crossterm::{execute, cursor};
use crossterm::terminal::{Clear, ClearType};
use protocol::{Con, Connection, SslConnection};
use readline::config::Configurer;
use readline::{error::ReadlineError, Editor};
use rustyline as readline;
use std::process;
const MSG_WELCOME: &'static str = "Skytable v0.5.3";

#[macro_use]
macro_rules! close_con {
    ($con:expr) => {
        if let Err(e) = $con.shutdown().await {
            eprintln!(
                "Failed to gracefully terminate connection with error '{}'",
                e
            );
            std::process::exit(0x100);
        }
    };
    ($con:expr, $err:expr) => {
        eprintln!("An error occurred while reading your input: '{}'", $err);
        close_con!($con)
    };
}

/// This creates a REPL on the command line and also parses command-line arguments
///
/// Anything that is entered following a return, is parsed into a query and is
/// written to the socket (which is either `localhost:2003` or it is determined by
/// command line parameters)
pub async fn start_repl() {
    let cfg_layout = load_yaml!("./cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let mut host = match matches.value_of("host") {
        Some(h) => h.to_owned(),
        None => ADDR.to_owned(),
    };
    host.push(':');
    match matches.value_of("port") {
        Some(p) => match p.parse::<u16>() {
            Ok(p) => host.push_str(&p.to_string()),
            Err(_) => {
                eprintln!("ERROR: Invalid port");
                process::exit(0x100);
            }
        },
        None => host.push_str("2003"),
    }
    let ssl = matches.value_of("cert");
    let mut con = if let Some(sslcert) = ssl {
        let con = match SslConnection::new(&host, sslcert).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("ERROR: {}", e);
                process::exit(0x100);
            }
        };
        Con::Secure(con)
    } else {
        let con = match Connection::new(&host).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("ERROR: {}", e);
                process::exit(0x100);
            }
        };
        Con::Insecure(con)
    };
    if let Some(eval_expr) = matches.value_of("eval") {
        if eval_expr.len() == 0 {
            return;
        }
        con.execute_query(eval_expr.to_string()).await;
        return;
    }
    let mut editor = Editor::<()>::new();
    editor.set_auto_add_history(true);
    editor.set_history_ignore_dups(true);
    let _ = editor.load_history(".sky_history");
    println!("{}", MSG_WELCOME);
    loop {
        match editor.readline("skysh> ") {
            Ok(line) => match line.to_lowercase().as_str() {
                "exit" => break,
                "clear" => {
                    let mut stdout = stdout();
                    execute!(stdout, Clear(ClearType::All)).expect("Failed to clear screen");
                    execute!(stdout, cursor::MoveTo(0, 0)).expect("Failed to move cursor to origin");
                    drop(stdout); // aggressively drop stdout
                    continue;
                }
                _ => con.execute_query(line).await,
            },
            Err(ReadlineError::Interrupted) => break,
            Err(err) => {
                close_con!(con, err);
            }
        }
    }
    if let Err(e) = editor.save_history(".sky_history") {
        eprintln!("Failed to save history with error: '{}'", e);
    }
    close_con!(con);
    println!("Goodbye!");
}
