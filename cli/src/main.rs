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

macro_rules! fatal {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(0x01);
    }}
}

mod args;
mod error;
mod query;
mod repl;
mod resp;

use {args::Task, query::Parameterizer};

fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => fatal!("cli error: {e}"),
    }
}

fn run() -> error::CliResult<()> {
    match args::parse()? {
        Task::HelpMessage(msg) => println!("{msg}"),
        Task::OpenShell(cfg) => repl::start(cfg)?,
        Task::ExecOnce(cfg, query) => {
            let query = Parameterizer::new(query).parameterize()?.into_query();
            let resp = query::connect(
                cfg,
                false,
                |mut c| Ok(c.query(&query)),
                |mut c| Ok(c.query(&query)),
            )??;
            resp::format_response(resp, false, false);
        }
    }
    Ok(())
}
