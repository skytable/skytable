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
    crate::cli::Cli,
    clap::Parser,
    env_logger::Builder,
    std::{env, process},
};

#[macro_use]
extern crate log;

mod bench;
mod cli;
mod config;
mod error;
mod util;

fn main() {
    Builder::new()
        .parse_filters(&env::var("SKYBENCH_LOG").unwrap_or_else(|_| "info".to_owned()))
        .init();
    if let Err(e) = run() {
        error!("sky-bench exited with error: {}", e);
        process::exit(0x01);
    }
}

fn run() -> error::BResult<()> {
    // Init CLI arg parser
    let cli = &Cli::parse();

    // Parse args and initialize configs
    let server_config = &cli.into();
    let bench_config = (server_config, cli).into();

    // Run our task
    bench::run_bench(server_config, bench_config)?;
    util::cleanup(server_config)
}
