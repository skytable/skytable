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

use crate::runner::Runner;
use clap::load_yaml;
use clap::App;
use crossterm::terminal::{Clear, ClearType};
use crossterm::{cursor, execute};
use libsky::URL;
use libsky::VERSION;
use readline::config::Configurer;
use readline::{error::ReadlineError, Editor};
use rustyline as readline;
use skytable::AsyncConnection;
use std::io::stdout;
use std::process;
use std::process::exit;
const ADDR: &str = "127.0.0.1";

/// This creates a REPL on the command line and also parses command-line arguments
///
/// Anything that is entered following a return, is parsed into a query and is
/// written to the socket (which is either `localhost:2003` or it is determined by
/// command line parameters)
pub async fn start_repl() {
    let cfg_layout = load_yaml!("./cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let host = matches.value_of("host").unwrap_or(ADDR);
    let port = match matches.value_of("port") {
        Some(p) => match p.parse::<u16>() {
            Ok(p) => p,
            Err(_) => {
                eprintln!("ERROR: Invalid port");
                process::exit(0x100);
            }
        },
        None => 2003,
    };
    let con = match AsyncConnection::new(host, port).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: {}", e);
            process::exit(0x100);
        }
    };
    let mut runner = Runner::new(con);
    if let Some(eval_expr) = matches.value_of("eval") {
        if eval_expr.is_empty() {
            return;
        }
        runner.run_query(&eval_expr).await;
        return;
    }
    let mut editor = Editor::<()>::new();
    editor.set_auto_add_history(true);
    editor.set_history_ignore_dups(true);
    let _ = editor.load_history(".sky_history");
    println!("Connected to skyhash://{}:{}", host, port);
    println!("Skytable v{} | {}", VERSION, URL);
    loop {
        match editor.readline("skysh> ") {
            Ok(line) => match line.to_lowercase().as_str() {
                "exit" => break,
                "clear" => {
                    let mut stdout = stdout();
                    execute!(stdout, Clear(ClearType::All)).expect("Failed to clear screen");
                    execute!(stdout, cursor::MoveTo(0, 0))
                        .expect("Failed to move cursor to origin");
                    drop(stdout); // aggressively drop stdout
                    continue;
                }
                _ => runner.run_query(&line).await,
            },
            Err(ReadlineError::Interrupted) => break,
            Err(err) => {
                eprintln!("Failed to read line with error: {}", err);
                exit(1);
            }
        }
    }
    if let Err(e) = editor.save_history(".sky_history") {
        eprintln!("Failed to save history with error: '{}'", e);
    }
    println!("Goodbye!");
}
