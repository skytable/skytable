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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # Skytable
//!
//! The `skyd` crate (or the `server` folder) is Skytable's database server and maybe
//! is the most important part of the project. There are several modules within this crate; see
//! the modules for their respective documentation.

use crate::config::BGSave;
use crate::config::PortConfig;
use crate::config::SnapshotConfig;
use libsky::URL;
use libsky::VERSION;
use std::io::Write;
use std::path;
use std::thread;
use std::time;
mod config;
use std::env;
use std::fs;
use std::process;
mod actions;
mod admin;
mod coredb;
mod dbnet;
mod diskstore;
mod protocol;
mod queryengine;
mod resp;
use coredb::CoreDB;
use dbnet::run;
mod compat;
use env_logger::*;
use libsky::util::terminal;
use std::sync::Arc;
mod services;
use tokio::signal;
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
static TEXT: &str = "\n███████ ██   ██ ██    ██ ████████  █████  ██████  ██      ███████ \n██      ██  ██   ██  ██     ██    ██   ██ ██   ██ ██      ██      \n███████ █████     ████      ██    ███████ ██████  ██      █████   \n     ██ ██  ██     ██       ██    ██   ██ ██   ██ ██      ██      \n███████ ██   ██    ██       ██    ██   ██ ██████  ███████ ███████ \n                                                                  ";

fn main() {
    Builder::new()
        .parse_filters(&env::var("SKY_LOG").unwrap_or_else(|_| "info".to_owned()))
        .init();
    // check if any other process is using the data directory and lock it if not (else error)
    let pid_file = run_pre_startup_tasks();
    // Start the server which asynchronously waits for a CTRL+C signal
    // which will safely shut down the server
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("server")
        .enable_all()
        .build()
        .unwrap();
    let db = runtime.block_on(async {
        let (tcplistener, bgsave_config, snapshot_config, restore_filepath) =
            check_args_and_get_cfg().await;
        let db = run(
            tcplistener,
            bgsave_config,
            snapshot_config,
            signal::ctrl_c(),
            restore_filepath,
        )
        .await;
        db
    });
    // Make sure all background workers terminate
    drop(runtime);
    assert_eq!(
        Arc::strong_count(&db.shared),
        1,
        "Maybe the compiler reordered the drop causing more than one instance of CoreDB to live at this point"
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
    // close the PID file and remove it
    drop(pid_file);
    if let Err(e) = fs::remove_file(PATH) {
        log::error!("Shutdown failure: Failed to remove pid file: {}", e);
        process::exit(0x100);
    }
    terminal::write_info("Goodbye :)\n").unwrap();
}

/// This function checks the command line arguments and either returns a config object
/// or prints an error to `stderr` and terminates the server
async fn check_args_and_get_cfg() -> (PortConfig, BGSave, SnapshotConfig, Option<String>) {
    let cfg = config::get_config_file_or_return_cfg();
    let binding_and_cfg = match cfg {
        Ok(config::ConfigType::Custom(cfg, file)) => {
            if cfg.is_artful() {
                println!("Skytable v{} | {}\n{}", VERSION, URL, TEXT);
            } else {
                println!("Skytable v{} | {}", VERSION, URL);
            }
            log::info!("Using settings from supplied configuration");
            (cfg.ports, cfg.bgsave, cfg.snapshot, file)
        }
        Ok(config::ConfigType::Def(cfg, file)) => {
            println!("Skytable v{} | {}\n{}", VERSION, URL, TEXT);
            log::warn!("No configuration file supplied. Using default settings");
            (cfg.ports, cfg.bgsave, cfg.snapshot, file)
        }
        Err(e) => {
            log::error!("{}", e);
            std::process::exit(0x100);
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
fn run_pre_startup_tasks() -> fs::File {
    let path = path::Path::new(PATH);
    if path.exists() {
        let pid = fs::read_to_string(path).unwrap_or_else(|_| "unknown".to_owned());
        log::error!(
            "Startup failure: Another process with parent PID {} is using the data directory",
            pid
        );
        process::exit(0x100);
    }
    let mut file = match fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(PATH)
    {
        Ok(fle) => fle,
        Err(e) => {
            log::error!("Startup failure: Failed to open pid file: {}", e);
            process::exit(0x100);
        }
    };
    if let Err(e) = file.write_all(process::id().to_string().as_bytes()) {
        log::error!("Startup failure: Failed to write to pid file: {}", e);
        process::exit(0x100);
    }
    file
}
