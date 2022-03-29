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

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]
#![deny(unused_must_use)]
#![cfg_attr(feature = "nightly", feature(test))]

//! # Skytable
//!
//! The `skyd` crate (or the `server` folder) is Skytable's database server and maybe
//! is the most important part of the project. There are several modules within this crate; see
//! the modules for their respective documentation.

use crate::diskstore::flock::FileLock;
pub use crate::util::exit_error;
use env_logger::Builder;
use libsky::URL;
use libsky::VERSION;
use std::env;
use std::process;
#[macro_use]
pub mod util;
mod actions;
mod admin;
mod arbiter;
mod auth;
mod config;
mod corestore;
mod dbnet;
mod diskstore;
mod kvengine;
mod protocol;
mod queryengine;
pub mod registry;
mod resp;
mod services;
mod storage;
#[cfg(test)]
mod tests;

const PID_FILE_PATH: &str = ".sky_pid";

#[cfg(test)]
const ROOT_DIR: &str = env!("ROOT_DIR");
#[cfg(test)]
const TEST_AUTH_ORIGIN_KEY: &str = env!("TEST_ORIGIN_KEY");

#[cfg(all(not(target_env = "msvc"), not(miri)))]
use jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), not(miri)))]
#[global_allocator]
/// Jemallocator - this is the default memory allocator for platforms other than msvc
static GLOBAL: Jemalloc = Jemalloc;

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
    // Start the server which asynchronously waits for a CTRL+C signal
    // which will safely shut down the server
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("server")
        .enable_all()
        .build()
        .unwrap();
    let (cfg, restore_file) = check_args_and_get_cfg();
    // check if any other process is using the data directory and lock it if not (else error)
    // important: create the pid_file just here and nowhere else because check_args can also
    // involve passing --help or wrong arguments which can falsely create a PID file
    let pid_file = run_pre_startup_tasks();
    let db = runtime.block_on(async move { arbiter::run(cfg, restore_file).await });
    // Make sure all background workers terminate
    drop(runtime);
    let db = match db {
        Ok(d) => d,
        Err(e) => {
            // uh oh, something happened while starting up
            log::error!("{}", e);
            services::pre_shutdown_cleanup(pid_file, None);
            process::exit(1);
        }
    };
    log::info!("Stopped accepting incoming connections");
    arbiter::finalize_shutdown(db, pid_file);
}

use self::config::ConfigurationSet;

/// This function checks the command line arguments and either returns a config object
/// or prints an error to `stderr` and terminates the server
fn check_args_and_get_cfg() -> (ConfigurationSet, Option<String>) {
    match config::get_config() {
        Ok(cfg) => {
            if cfg.is_artful() {
                log::info!("Skytable v{} | {}\n{}", VERSION, URL, TEXT);
            } else {
                log::info!("Skytable v{} | {}", VERSION, URL);
            }
            if cfg.is_custom() {
                log::info!("Using settings from supplied configuration");
            } else {
                log::warn!("No configuration file supplied. Using default settings");
            }
            // print warnings if any
            cfg.print_warnings();
            cfg.finish()
        }
        Err(e) => {
            log::error!("{}", e);
            crate::exit_error();
        }
    }
}

/// On startup, we attempt to check if a `.sky_pid` file exists. If it does, then
/// this file will contain the kernel/operating system assigned process ID of the
/// skyd process. We will attempt to read that and log an error complaining that
/// the directory is in active use by another process. If the file doesn't then
/// we're free to create our own file and write our own PID to it. Any subsequent
/// processes will detect this and this helps us prevent two processes from writing
/// to the same directory which can cause potentially undefined behavior.
///
fn run_pre_startup_tasks() -> FileLock {
    let mut file = match FileLock::lock(PID_FILE_PATH) {
        Ok(fle) => fle,
        Err(e) => {
            log::error!("Startup failure: Failed to lock pid file: {}", e);
            crate::exit_error();
        }
    };
    if let Err(e) = file.write(process::id().to_string().as_bytes()) {
        log::error!("Startup failure: Failed to write to pid file: {}", e);
        crate::exit_error();
    }
    file
}
