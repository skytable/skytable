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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
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
use std::io::stdout;
use std::process;
const ADDR: &str = "127.0.0.1";
const SKYSH_BLANK: &str = "     > ";
const SKYSH_PROMPT: &str = "skysh> ";
const SKYSH_HISTORY_FILE: &str = ".sky_history";

/// This creates a REPL on the command line and also parses command-line arguments
///
/// Anything that is entered following a return, is parsed into a query and is
/// written to the socket (which is either `localhost:2003` or it is determined by
/// command line parameters)
pub async fn start_repl() {
    let cfg_layout = load_yaml!("./cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let host = libsky::option_unwrap_or!(matches.value_of("host"), ADDR);
    let port = match matches.value_of("port") {
        Some(p) => match p.parse::<u16>() {
            Ok(p) => p,
            Err(_) => fatal!("Invalid port"),
        },
        None => 2003,
    };
    let mut editor = Editor::<()>::new();
    editor.set_auto_add_history(true);
    editor.set_history_ignore_dups(true);
    editor.bind_sequence(
        rustyline::KeyEvent(
            rustyline::KeyCode::BracketedPasteStart,
            rustyline::Modifiers::NONE,
        ),
        rustyline::Cmd::Noop,
    );
    let _ = editor.load_history(SKYSH_HISTORY_FILE);
    let con = match matches.value_of("cert") {
        Some(cert) => Runner::new_secure(host, port, cert).await,
        None => Runner::new_insecure(host, port).await,
    };
    let mut runner = match con {
        Ok(c) => c,
        Err(e) => fatal!("Failed to connect to server with error: {}", e),
    };
    if let Some(eval_expr) = matches.value_of("eval") {
        if !eval_expr.is_empty() {
            runner.run_query(eval_expr).await;
        }
        process::exit(0x00);
    }
    println!("Skytable v{} | {}", VERSION, URL);
    loop {
        match editor.readline(SKYSH_PROMPT) {
            Ok(mut line) => match line.to_lowercase().as_str() {
                "exit" => break,
                "clear" => {
                    let mut stdout = stdout();
                    execute!(stdout, Clear(ClearType::All)).expect("Failed to clear screen");
                    execute!(stdout, cursor::MoveTo(0, 0))
                        .expect("Failed to move cursor to origin");
                    drop(stdout); // aggressively drop stdout
                    continue;
                }
                _ => {
                    if line.is_empty() {
                        continue;
                    }
                    if (line.as_bytes()[0]) == b'#' {
                        continue;
                    }
                    while line.len() >= 2 && line[line.len() - 2..].as_bytes().eq(br#" \"#) {
                        // continuation on next line
                        let cl = readln!(editor);
                        line.drain(line.len() - 2..);
                        line.extend(cl.chars());
                    }
                    runner.run_query(&line).await
                }
            },
            Err(ReadlineError::Interrupted) => break,
            Err(err) => fatal!("ERROR: Failed to read line with error: {}", err),
        }
    }
    editor
        .save_history(SKYSH_HISTORY_FILE)
        .map_err(|e| {
            fatal!("ERROR: Failed to save history with error: '{}'", e);
        })
        .unwrap();
}
