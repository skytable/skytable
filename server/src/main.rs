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
#![cfg_attr(feature = "nightly", feature(test))]

//! # Skytable
//!
//! The `skyd` crate (or the `server` folder) is Skytable's database server and maybe
//! is the most important part of the project. There are several modules within this crate; see
//! the modules for their respective documentation.

use crate::corestore::memstore::Memstore;
use crate::diskstore::flock::FileLock;
use env_logger::Builder;
use libsky::util::terminal;
use libsky::URL;
use libsky::VERSION;
use std::env;
use std::process;
use std::thread;
use std::time;
#[macro_use]
mod util;
#[macro_use]
extern crate libsky;
mod actions;
mod admin;
mod arbiter;
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

const PATH: &str = ".sky_pid";

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
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
    let (ports, bgsave_config, snapshot_config, restore_filepath, maxcon) =
        check_args_and_get_cfg();
    // check if any other process is using the data directory and lock it if not (else error)
    // important: create the pid_file just here and nowhere else because check_args can also
    // involve passing --help or wrong arguments which can falsely create a PID file
    let pid_file = run_pre_startup_tasks();
    let db: Result<corestore::Corestore, String> = runtime.block_on(async move {
        arbiter::run(
            ports,
            bgsave_config,
            snapshot_config,
            restore_filepath,
            maxcon,
        )
        .await
    });
    // Make sure all background workers terminate
    drop(runtime);
    let db = match db {
        Ok(d) => d,
        Err(e) => {
            // uh oh, something happened while starting up
            log::error!("{}", e);
            pre_shutdown_cleanup(pid_file, None);
            process::exit(1);
        }
    };
    assert_eq!(
        db.strong_count(),
        1,
        "Maybe the compiler reordered the drop causing more than one instance of Corestore to live at this point"
    );
    log::info!("Stopped accepting incoming connections");
    loop {
        // Keep looping until we successfully write the in-memory table to disk
        match services::bgsave::run_bgsave(&db) {
            Ok(_) => {
                log::info!("Successfully saved data to disk");
                break;
            }
            Err(e) => {
                log::error!(
                    "Failed to write data with error '{}'. Attempting to retry in 10s",
                    e
                );
            }
        }
        thread::sleep(time::Duration::from_secs(10));
    }
    pre_shutdown_cleanup(pid_file, Some(db.get_store()));
    terminal::write_info("Goodbye :)\n").unwrap();
}

pub fn pre_shutdown_cleanup(mut pid_file: FileLock, mr: Option<&Memstore>) {
    if let Err(e) = pid_file.unlock() {
        log::error!("Shutdown failure: Failed to unlock pid file: {}", e);
        process::exit(0x01);
    }
    if let Some(mr) = mr {
        log::info!("Compacting tree");
        if let Err(e) = storage::interface::cleanup_tree(mr) {
            log::error!("Failed to compact tree: {}", e);
            process::exit(0x01);
        }
    }
}

use self::config::{BGSave, PortConfig, SnapshotConfig};

/// This function checks the command line arguments and either returns a config object
/// or prints an error to `stderr` and terminates the server
fn check_args_and_get_cfg() -> (PortConfig, BGSave, SnapshotConfig, Option<String>, usize) {
    let cfg = config::get_config_file_or_return_cfg();
    let binding_and_cfg = match cfg {
        Ok(config::ConfigType::Custom(cfg, file)) => {
            if cfg.is_artful() {
                println!("Skytable v{} | {}\n{}", VERSION, URL, TEXT);
            } else {
                println!("Skytable v{} | {}", VERSION, URL);
            }
            log::info!("Using settings from supplied configuration");
            (cfg.ports, cfg.bgsave, cfg.snapshot, file, cfg.maxcon)
        }
        Ok(config::ConfigType::Def(cfg, file)) => {
            println!("Skytable v{} | {}\n{}", VERSION, URL, TEXT);
            log::warn!("No configuration file supplied. Using default settings");
            (cfg.ports, cfg.bgsave, cfg.snapshot, file, cfg.maxcon)
        }
        Err(e) => {
            log::error!("{}", e);
            std::process::exit(0x01);
        }
    };
    binding_and_cfg
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
    let mut file = match FileLock::lock(PATH) {
        Ok(fle) => fle,
        Err(e) => {
            log::error!("Startup failure: Failed to lock pid file: {}", e);
            process::exit(0x01);
        }
    };
    if let Err(e) = file.write(process::id().to_string().as_bytes()) {
        log::error!("Startup failure: Failed to write to pid file: {}", e);
        process::exit(0x01);
    }
    file
}
