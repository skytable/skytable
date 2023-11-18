/*
 * Created on Sat Nov 18 2023
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
    crate::error::{BenchError, BenchResult},
    std::{
        collections::hash_map::{Entry, HashMap},
        env,
    },
};

const TXT_HELP: &str = include_str!("../help_text/help");

#[derive(Debug)]
enum TaskInner {
    HelpMsg(String),
    CheckConfig(HashMap<String, String>),
}

#[derive(Debug)]
pub enum Task {
    HelpMsg(String),
    BenchConfig(BenchConfig),
}

#[derive(Debug)]
pub struct BenchConfig {
    pub host: String,
    pub port: u16,
    pub root_pass: String,
    pub threads: usize,
    pub key_size: usize,
    pub query_count: usize,
}

impl BenchConfig {
    pub fn new(
        host: String,
        port: u16,
        root_pass: String,
        threads: usize,
        key_size: usize,
        query_count: usize,
    ) -> Self {
        Self {
            host,
            port,
            root_pass,
            threads,
            key_size,
            query_count,
        }
    }
}

fn load_env() -> BenchResult<TaskInner> {
    let mut args = HashMap::new();
    let mut it = env::args().skip(1).into_iter();
    while let Some(arg) = it.next() {
        let (arg, arg_val) = match arg.as_str() {
            "--help" => return Ok(TaskInner::HelpMsg(TXT_HELP.into())),
            "--version" => {
                return Ok(TaskInner::HelpMsg(format!(
                    "sky-bench v{}",
                    libsky::VERSION
                )))
            }
            _ if arg.starts_with("--") => match it.next() {
                Some(arg_val) => (arg, arg_val),
                None => {
                    // self contained?
                    let split: Vec<&str> = arg.split("=").collect();
                    if split.len() != 2 {
                        return Err(BenchError::ArgsErr(format!("expected value for {arg}")));
                    }
                    (split[0].into(), split[1].into())
                }
            },
            unknown_arg => {
                return Err(BenchError::ArgsErr(format!(
                    "unknown argument: {unknown_arg}"
                )))
            }
        };
        match args.entry(arg) {
            Entry::Occupied(oe) => {
                return Err(BenchError::ArgsErr(format!(
                    "found duplicate values for {}",
                    oe.key()
                )))
            }
            Entry::Vacant(ve) => {
                ve.insert(arg_val);
            }
        }
    }
    Ok(TaskInner::CheckConfig(args))
}

fn cdig(n: usize) -> usize {
    if n == 0 {
        1
    } else {
        (n as f64).log10().floor() as usize + 1
    }
}

pub fn parse() -> BenchResult<Task> {
    let mut args = match load_env()? {
        TaskInner::HelpMsg(msg) => return Ok(Task::HelpMsg(msg)),
        TaskInner::CheckConfig(args) => args,
    };
    // endpoint
    let (host, port) = match args.remove("--endpoint") {
        None => ("127.0.0.1".to_owned(), 2003),
        Some(ep) => {
            // proto@host:port
            let ep: Vec<&str> = ep.split("@").collect();
            if ep.len() != 2 {
                return Err(BenchError::ArgsErr(
                    "value for --endpoint must be in the form `[protocol]@[host]:[port]`".into(),
                ));
            }
            let protocol = ep[0];
            let host_port: Vec<&str> = ep[1].split(":").collect();
            if host_port.len() != 2 {
                return Err(BenchError::ArgsErr(
                    "value for --endpoint must be in the form `[protocol]@[host]:[port]`".into(),
                ));
            }
            let (host, port) = (host_port[0], host_port[1]);
            let Ok(port) = port.parse::<u16>() else {
                return Err(BenchError::ArgsErr(
                    "the value for port must be an integer in the range 0-65535".into(),
                ));
            };
            if protocol != "tcp" {
                return Err(BenchError::ArgsErr(
                    "only TCP endpoints can be benchmarked at the moment".into(),
                ));
            }
            (host.to_owned(), port)
        }
    };
    // password
    let passsword = match args.remove("--password") {
        Some(p) => p,
        None => {
            return Err(BenchError::ArgsErr(
                "you must provide a value for `--password`".into(),
            ))
        }
    };
    // threads
    let thread_count = match args.remove("--threads") {
        None => num_cpus::get(),
        Some(tc) => match tc.parse() {
            Ok(tc) if tc > 0 => tc,
            Err(_) | Ok(_) => {
                return Err(BenchError::ArgsErr(
                    "incorrect value for `--threads`. must be a nonzero value".into(),
                ))
            }
        },
    };
    // query count
    let query_count = match args.remove("--rowcount") {
        None => 1_000_000_usize,
        Some(rc) => match rc.parse() {
            Ok(rc) if rc != 0 => rc,
            Err(_) | Ok(_) => {
                return Err(BenchError::ArgsErr(format!(
                    "bad value for `--rowcount` must be a nonzero value"
                )))
            }
        },
    };
    let need_atleast = cdig(query_count);
    let key_size = match args.remove("--keysize") {
        None => need_atleast,
        Some(ks) => match ks.parse() {
            Ok(s) if s >= need_atleast => s,
            Err(_) | Ok(_) => return Err(BenchError::ArgsErr(format!("incorrect value for `--keysize`. must be set to a value that can be used to generate atleast {query_count} unique primary keys"))),
        }
    };
    Ok(Task::BenchConfig(BenchConfig::new(
        host,
        port,
        passsword,
        thread_count,
        key_size,
        query_count,
    )))
}
