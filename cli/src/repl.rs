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
        args::{ClientConfig, ClientConfigKind},
        error::{CliError, CliResult},
        query::{self, IsConnection},
        resp,
    },
    crossterm::{cursor, execute, terminal},
    rustyline::{config::Configurer, error::ReadlineError, DefaultEditor},
    skytable::Config,
    std::io::{stdout, ErrorKind},
};

const SKYSH_HISTORY_FILE: &str = ".sky_history";
const TXT_WELCOME: &str = include_str!("../help_text/welcome");

pub fn start(cfg: ClientConfig) -> CliResult<()> {
    match cfg.kind {
        ClientConfigKind::Tcp(host, port) => {
            let c = Config::new(&host, port, &cfg.username, &cfg.password).connect()?;
            println!(
                "Authenticated as '{}' on {}:{} over Skyhash/TCP\n---",
                &cfg.username, &host, &port
            );
            repl(c)
        }
        ClientConfigKind::Tls(host, port, cert) => {
            let c = Config::new(&host, port, &cfg.username, &cfg.password).connect_tls(&cert)?;
            println!(
                "Authenticated as '{}' on {}:{} over Skyhash/TLS\n---",
                &cfg.username, &host, &port
            );
            repl(c)
        }
    }
}

fn repl<C: IsConnection>(mut con: C) -> CliResult<()> {
    let init_editor = || {
        let mut editor = DefaultEditor::new()?;
        editor.set_auto_add_history(true);
        editor.set_history_ignore_dups(true)?;
        editor.bind_sequence(
            rustyline::KeyEvent(
                rustyline::KeyCode::BracketedPasteStart,
                rustyline::Modifiers::NONE,
            ),
            rustyline::Cmd::Noop,
        );
        match editor.load_history(SKYSH_HISTORY_FILE) {
            Ok(()) => {}
            Err(e) => match e {
                ReadlineError::Io(ref ioe) => match ioe.kind() {
                    ErrorKind::NotFound => {
                        println!("{TXT_WELCOME}");
                    }
                    _ => return Err(e),
                },
                e => return Err(e),
            },
        }
        rustyline::Result::Ok(editor)
    };
    let mut editor = match init_editor() {
        Ok(e) => e,
        Err(e) => fatal!("error: failed to init REPL. {e}"),
    };
    loop {
        match editor.readline("> ") {
            Ok(line) => match line.as_str() {
                "!help" => println!("{TXT_WELCOME}"),
                "exit" => break,
                "clear" => clear_screen()?,
                _ => {
                    if line.is_empty() {
                        continue;
                    }
                    match query::Parameterizer::new(line).parameterize() {
                        Ok(q) => resp::format_response(con.execute_query(q)?)?,
                        Err(e) => match e {
                            CliError::QueryError(e) => {
                                eprintln!("[skysh error]: bad query. {e}");
                                continue;
                            }
                            _ => return Err(e),
                        },
                    };
                }
            },
            Err(e) => match e {
                ReadlineError::Interrupted | ReadlineError::Eof => {
                    // done
                    break;
                }
                ReadlineError::WindowResized => {}
                e => fatal!("error: failed to read line REPL. {e}"),
            },
        }
    }
    editor
        .save_history(SKYSH_HISTORY_FILE)
        .expect("failed to save history");
    println!("Goodbye!");
    Ok(())
}

fn clear_screen() -> std::io::Result<()> {
    let mut stdout = stdout();
    execute!(stdout, terminal::Clear(terminal::ClearType::All))?;
    execute!(stdout, cursor::MoveTo(0, 0))
}
