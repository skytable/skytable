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

use crate::{util, Cli};

static mut OUTPUT_JSON: bool = false;

#[derive(Clone)]
pub struct ServerConfig {
    /// host
    host: String,
    /// port
    port: u16,
    /// connection count for network pool
    connections: usize,
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

impl From<(&ServerConfig, &Cli)> for BenchmarkConfig {
    fn from(tuple: (&ServerConfig, &Cli)) -> Self {
        let (server_config, cli) = tuple;
        unsafe {
            OUTPUT_JSON = cli.json;
        }
        BenchmarkConfig {
            server: server_config.clone(),
            queries: cli.query_count,
            kvsize: cli.kvsize,
            runs: cli.runs,
        }
    }
}

impl From<&Cli> for ServerConfig {
    fn from(cli: &Cli) -> Self {
        ServerConfig {
            connections: cli.connections,
            host: cli.host.clone(),
            port: cli.port,
        }
    }
}
