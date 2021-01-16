/*
 * Created on Thu Jul 02 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

//! # TerrabaseDB
//!
//! The `tdb` crate (or the `server` folder) is TerrabaseDB's database server and maybe
//! is the most important part of the project. There are several modules within this crate; see
//! the modules for their respective documentation.

use crate::config::BGSave;
use crate::config::PortConfig;
use crate::config::SnapshotConfig;
mod config;
use std::env;
mod admin;
mod coredb;
mod dbnet;
mod diskstore;
mod kvengine;
mod protocol;
mod queryengine;
mod resp;
use coredb::CoreDB;
use dbnet::run;
use env_logger::*;
use tokio::signal;
#[cfg(test)]
mod tests;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
/// Jemallocator - this is the default memory allocator for platforms other than msvc
static GLOBAL: Jemalloc = Jemalloc;

/// The version text
static MSG: &'static str = "TerrabaseDB v0.5.1 | https://github.com/terrabasedb/terrabasedb";
/// The terminal art for `!noart` configurations
static TEXT: &'static str = " 
      _______                       _                        _____   ____  
     |__   __|                     | |                      |  __ \\ |  _ \\ 
        | |  ___  _ __  _ __  __ _ | |__    __ _  ___   ___ | |  | || |_) |
        | | / _ \\| '__|| '__|/ _` || '_ \\  / _` |/ __| / _ \\| |  | ||  _ < 
        | ||  __/| |   | |  | (_| || |_) || (_| |\\__ \\|  __/| |__| || |_) |
        |_| \\___||_|   |_|   \\__,_||_.__/  \\__,_||___/ \\___||_____/ |____/
        
        +-++-++-+ +-++-++-++-+ +-++-++-++-++-+ +-++-++-++-++-++-++-++-+
        |T||h||e| |n||e||x||t| |N||o||S||Q||L| |d||a||t||a||b||a||s||e|
        +-++-++-+ +-++-++-++-+ +-++-++-++-++-+ +-++-++-++-++-++-++-++-+    
";
#[tokio::main]
async fn main() {
    Builder::new()
        .parse_filters(&env::var("TDB_LOG").unwrap_or("info".to_owned()))
        .init();
    // Start the server which asynchronously waits for a CTRL+C signal
    // which will safely shut down the server
    let (tcplistener, bgsave_config, snapshot_config, restore_filepath) =
        check_args_and_get_cfg().await;
    run(
        tcplistener,
        bgsave_config,
        snapshot_config,
        signal::ctrl_c(),
        restore_filepath,
    )
    .await;
}

/// This function checks the command line arguments and either returns a config object
/// or prints an error to `stderr` and terminates the server
async fn check_args_and_get_cfg() -> (
    PortConfig,
    BGSave,
    SnapshotConfig,
    Option<std::path::PathBuf>,
) {
    let cfg = config::get_config_file_or_return_cfg();
    let binding_and_cfg = match cfg {
        Ok(config::ConfigType::Custom(cfg, file)) => {
            if cfg.is_artful() {
                println!("{}\n{}", TEXT, MSG);
            } else {
                println!("{}", MSG);
            }
            log::info!("Using settings from supplied configuration");
            (cfg.ports, cfg.bgsave, cfg.snapshot, file)
        }
        Ok(config::ConfigType::Def(cfg, file)) => {
            println!("{}\n{}", TEXT, MSG);
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
