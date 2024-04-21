/*
 * Created on Wed Nov 15 2023
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
    crate::error::{CliError, CliResult},
    crossterm::{
        event::{self, Event, KeyCode, KeyEvent},
        terminal,
    },
    libsky::{env_vars, CliAction},
    std::{
        collections::HashMap,
        env, fs,
        io::{self, Write},
        process::exit,
    },
};

const TXT_HELP: &str = include_str!(concat!(env!("OUT_DIR"), "/skysh"));

#[derive(Debug)]
pub struct ClientConfig {
    pub kind: ClientConfigKind,
    pub username: String,
    pub password: String,
}

impl ClientConfig {
    pub fn new(kind: ClientConfigKind, username: String, password: String) -> Self {
        Self {
            kind,
            username,
            password,
        }
    }
}

#[derive(Debug)]
pub enum ClientConfigKind {
    Tcp(String, u16),
    Tls(String, u16, String),
}

#[derive(Debug)]
pub enum Task {
    HelpMessage(String),
    OpenShell(ClientConfig),
    ExecOnce(ClientConfig, String),
}

enum TaskInner {
    HelpMsg(String),
    OpenShell(HashMap<String, String>),
}

fn load_env() -> CliResult<TaskInner> {
    let action = libsky::parse_cli_args_disallow_duplicate()?;
    match action {
        CliAction::Help => Ok(TaskInner::HelpMsg(TXT_HELP.into())),
        CliAction::Version => Ok(TaskInner::HelpMsg(libsky::version_msg("skysh"))),
        CliAction::Action(a) => Ok(TaskInner::OpenShell(a)),
    }
}

pub fn parse() -> CliResult<Task> {
    let mut args = match load_env()? {
        TaskInner::HelpMsg(msg) => return Ok(Task::HelpMessage(msg)),
        TaskInner::OpenShell(args) => args,
    };
    let endpoint = match args.remove("--endpoint") {
        None => ClientConfigKind::Tcp("127.0.0.1".into(), 2003),
        Some(ep) => {
            // should be in the format protocol@host:port
            let proto_host_port: Vec<&str> = ep.split("@").collect();
            if proto_host_port.len() != 2 {
                return Err(CliError::ArgsErr("invalid value for --endpoint".into()));
            }
            let (protocol, host_port) = (proto_host_port[0], proto_host_port[1]);
            let host_port: Vec<&str> = host_port.split(":").collect();
            if host_port.len() != 2 {
                return Err(CliError::ArgsErr("invalid value for --endpoint".into()));
            }
            let (host, port) = (host_port[0], host_port[1]);
            let port = match port.parse::<u16>() {
                Ok(port) => port,
                Err(e) => {
                    return Err(CliError::ArgsErr(format!(
                        "invalid value for endpoint port. {e}"
                    )))
                }
            };
            let tls_cert = args.remove("--tls-cert");
            match protocol {
                "tcp" => {
                    // TODO(@ohsayan): warn!
                    ClientConfigKind::Tcp(host.into(), port)
                }
                "tls" => {
                    // we need a TLS cert
                    match tls_cert {
                        Some(path) => {
                            let cert = fs::read_to_string(path)?;
                            ClientConfigKind::Tls(host.into(), port, cert)
                        }
                        None => {
                            return Err(CliError::ArgsErr(format!(
                                "must provide TLS cert when using TLS endpoint"
                            )))
                        }
                    }
                }
                _ => {
                    return Err(CliError::ArgsErr(format!(
                        "unknown protocol scheme `{protocol}`"
                    )))
                }
            }
        }
    };
    let username = match args.remove("--user") {
        Some(u) => u,
        None => {
            // default
            "root".into()
        }
    };
    let password = match args.remove("--password") {
        Some(p) => check_password(p, "cli arguments")?,
        None => {
            // let us check the environment variable to see if anything was set
            match env::var(env_vars::SKYDB_PASSWORD) {
                Ok(v) => check_password(v, "env")?,
                Err(_) => check_password(read_password("Enter password: ")?, "env")?,
            }
        }
    };
    let eval = args.remove("--eval").or_else(|| args.remove("-e"));
    if args.is_empty() {
        let client = ClientConfig::new(endpoint, username, password);
        match eval {
            Some(query) => Ok(Task::ExecOnce(client, query)),
            None => Ok(Task::OpenShell(client)),
        }
    } else {
        Err(CliError::ArgsErr(format!("found unknown arguments")))
    }
}

fn check_password(p: String, source: &str) -> CliResult<String> {
    if p.is_empty() {
        return Err(CliError::ArgsErr(format!(
            "password value cannot be empty (currently set via {source})"
        )));
    } else {
        Ok(p)
    }
}

fn read_password(prompt: &str) -> Result<String, std::io::Error> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut password = String::new();
    terminal::enable_raw_mode()?;
    loop {
        match event::read()? {
            Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers: event::KeyModifiers::CONTROL,
                kind: event::KeyEventKind::Press,
                ..
            }) => {
                terminal::disable_raw_mode()?;
                println!();
                exit(0x00)
            }
            Event::Key(KeyEvent {
                code,
                modifiers: event::KeyModifiers::NONE,
                kind: event::KeyEventKind::Press,
                ..
            }) => match code {
                KeyCode::Backspace => {
                    let _ = password.pop();
                }
                KeyCode::Char(c) => password.push(c),
                KeyCode::Enter => break,
                _ => {}
            },
            _ => {}
        }
    }
    terminal::disable_raw_mode()?;
    println!();
    Ok(password)
}
