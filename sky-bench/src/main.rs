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

#[macro_use]
extern crate log;
mod args;
mod bench;
mod error;
mod runtime;

fn main() {
    env_logger::Builder::new()
        .parse_filters(&std::env::var("SKYBENCH_LOG").unwrap_or_else(|_| "info".to_owned()))
        .init();
    match run() {
        Ok(()) => {}
        Err(e) => {
            error!("bench error: {e}");
            std::process::exit(0x01);
        }
    }
}

fn run() -> error::BenchResult<()> {
    let task = args::parse()?;
    match task {
        args::Task::HelpMsg(msg) => println!("{msg}"),
        args::Task::BenchConfig(bench) => bench::run(bench)?,
    }
    Ok(())
}
