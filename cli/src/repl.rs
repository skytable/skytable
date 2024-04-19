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
        args::ClientConfig,
        error::{CliError, CliResult},
        query::{self, ExecKind, IsConnection},
        resp,
    },
    crossterm::{cursor, execute, terminal},
    rustyline::{config::Configurer, error::ReadlineError, DefaultEditor},
    std::{
        env,
        io::{stdout, ErrorKind},
        path::PathBuf,
    },
};

const SKYSH_HISTORY_FILE: &str = ".sky_history";
const VAR_SKYSH_HISTORY_FILE_PATH: &str = "SKYSH_HISTORY_FILE";
const TXT_WELCOME: &str = include_str!("../help_text/welcome");

pub fn start(cfg: ClientConfig) -> CliResult<()> {
    query::connect(cfg, true, repl, repl)
}

fn repl<C: IsConnection>(mut con: C) -> CliResult<()> {
    let history_file_path = {
        match env::var_os(VAR_SKYSH_HISTORY_FILE_PATH).map(PathBuf::from) {
            Some(path) => path,
            None => {
                let mut home_directory = libsky::get_home_dir()
                    .ok_or(CliError::OtherError("could not find home directory"))?;
                home_directory.push(SKYSH_HISTORY_FILE);
                home_directory
            }
        }
    };
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
        match editor.load_history(&history_file_path) {
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
    let mut prompt = "> ".to_owned();
    loop {
        match editor.readline(&prompt) {
            Ok(line) => match line.as_str() {
                "!help" => println!("{TXT_WELCOME}"),
                "exit" => break,
                "clear" => clear_screen()?,
                _ => {
                    if line.is_empty() {
                        continue;
                    }
                    match query::Parameterizer::new(line).parameterize() {
                        Ok(q) => {
                            let mut new_prompt = None;
                            let mut special = false;
                            let q = match q {
                                ExecKind::Standard(q) => q,
                                ExecKind::UseNull(q) => {
                                    new_prompt = Some("> ".into());
                                    q
                                }
                                ExecKind::UseSpace(q, space) => {
                                    new_prompt = Some(format!("{space}> "));
                                    q
                                }
                                ExecKind::PrintSpecial(q) => {
                                    special = true;
                                    q
                                }
                            };
                            if resp::format_response(con.execute_query(q)?, special, true) {
                                if let Some(pr) = new_prompt {
                                    prompt = pr;
                                }
                            }
                        }
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
        .save_history(&history_file_path)
        .expect("failed to save history");
    println!("Goodbye!");
    Ok(())
}

fn clear_screen() -> std::io::Result<()> {
    let mut stdout = stdout();
    execute!(stdout, terminal::Clear(terminal::ClearType::All))?;
    execute!(stdout, cursor::MoveTo(0, 0))
}
