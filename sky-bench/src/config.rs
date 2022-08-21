/*
 * Created on Mon Aug 08 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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
    crate::error::{BResult, Error},
    crate::util,
    clap::ArgMatches,
    std::{fmt::Display, str::FromStr},
};

static mut OUTPUT_JSON: bool = false;

#[derive(Clone)]
pub struct ServerConfig {
    /// host
    host: Box<str>,
    /// port
    port: u16,
    /// connection count for network pool
    connections: usize,
}

#[inline(always)]
fn try_update<T: FromStr, S: AsRef<str>>(input: Option<S>, target: &mut T) -> BResult<()>
where
    <T as FromStr>::Err: Display,
{
    if let Some(input) = input {
        let parsed = input
            .as_ref()
            .parse::<T>()
            .map_err(|e| Error::Config(format!("parse error: `{}`", e)))?;
        *target = parsed;
    }
    Ok(())
}

impl ServerConfig {
    const DEFAULT_HOST: &'static str = "127.0.0.1";
    const DEFAULT_PORT: u16 = 2003;
    const DEFAULT_CONNECTIONS: usize = 10;
    /// Init the default server config
    pub fn new(matches: &ArgMatches) -> BResult<Self> {
        let mut slf = Self {
            host: Self::DEFAULT_HOST.into(),
            port: Self::DEFAULT_PORT,
            connections: Self::DEFAULT_CONNECTIONS,
        };
        slf.try_host(matches.value_of_lossy("host"));
        slf.try_port(matches.value_of_lossy("port"))?;
        slf.try_connections(matches.value_of_lossy("connections"))?;
        Ok(slf)
    }
    /// Update the host
    pub fn try_host<T: AsRef<str>>(&mut self, host: Option<T>) {
        if let Some(host) = host {
            self.host = host.as_ref().into();
        }
    }
    /// Attempt to update the port
    pub fn try_port<T: AsRef<str>>(&mut self, port: Option<T>) -> BResult<()> {
        try_update(port, &mut self.port)
    }
    /// Attempt to update the connections
    pub fn try_connections<T: AsRef<str>>(&mut self, con: Option<T>) -> BResult<()> {
        try_update(con, &mut self.connections)
    }
}

impl ServerConfig {
    pub fn host(&self) -> &str {
        self.host.as_ref()
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn connections(&self) -> usize {
        self.connections
    }
}

/// Benchmark configuration
#[derive(Clone)]
pub struct BenchmarkConfig {
    pub server: ServerConfig,
    kvsize: usize,
    queries: usize,
    runs: usize,
}

impl BenchmarkConfig {
    const DEFAULT_QUERIES: usize = 100_000;
    const DEFAULT_KVSIZE: usize = 3;
    const DEFAULT_RUNS: usize = 5;
    pub fn new(server: &ServerConfig, matches: ArgMatches) -> BResult<Self> {
        let mut slf = Self {
            server: server.clone(),
            queries: Self::DEFAULT_QUERIES,
            kvsize: Self::DEFAULT_KVSIZE,
            runs: Self::DEFAULT_RUNS,
        };
        try_update(matches.value_of_lossy("queries"), &mut slf.queries)?;
        try_update(matches.value_of_lossy("size"), &mut slf.kvsize)?;
        try_update(matches.value_of_lossy("runs"), &mut slf.runs)?;
        util::ensure_main_thread();
        unsafe {
            OUTPUT_JSON = matches.is_present("json");
        }
        Ok(slf)
    }
    pub fn kvsize(&self) -> usize {
        self.kvsize
    }
    pub fn query_count(&self) -> usize {
        self.queries
    }
    pub fn runs(&self) -> usize {
        self.runs
    }
}

pub fn should_output_messages() -> bool {
    util::ensure_main_thread();
    unsafe { !OUTPUT_JSON }
}
