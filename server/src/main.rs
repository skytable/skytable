/*
 * Created on Thu Jul 02 2020
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

#![deny(unused_crate_dependencies, unused_imports, unused_must_use)]
#![cfg_attr(feature = "nightly", feature(test))]

//! # Skytable
//!
//! The `skyd` crate (or the `server` folder) is Skytable's database server and maybe
//! is the most important part of the project. There are several modules within this crate; see
//! the modules for their respective documentation.

use {env_logger::Builder, std::env};

#[macro_use]
extern crate log;
#[macro_use]
pub mod util;
mod engine;

use {
    crate::util::exit_error,
    libsky::{URL, VERSION},
};

#[cfg(all(not(target_env = "msvc"), not(miri)))]
#[global_allocator]
/// Jemallocator - this is the default memory allocator for platforms other than msvc
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// The terminal art for `!noart` configurations
const TEXT: &str = "
███████ ██   ██ ██    ██ ████████  █████  ██████  ██      ███████
██      ██  ██   ██  ██     ██    ██   ██ ██   ██ ██      ██
███████ █████     ████      ██    ███████ ██████  ██      █████
     ██ ██  ██     ██       ██    ██   ██ ██   ██ ██      ██
███████ ██   ██    ██       ██    ██   ██ ██████  ███████ ███████
";

type IoResult<T> = std::io::Result<T>;

fn main() {
    Builder::new()
        .parse_filters(&env::var("SKY_LOG").unwrap_or_else(|_| "info".to_owned()))
        .init();
    println!("{TEXT}\nSkytable v{VERSION} | {URL}\n");
    let (config, global) = match engine::load_all() {
        Ok(x) => x,
        Err(e) => {
            error!("{e}");
            exit_error()
        }
    };
    let g = global.global.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("server")
        .enable_all()
        .build()
        .unwrap();
    match runtime.block_on(async move { engine::start(config, global).await }) {
        Ok(()) => {
            engine::finish(g);
            info!("finished all pending tasks. Goodbye!");
        }
        Err(e) => {
            error!("{e}");
            exit_error()
        }
    }
}
