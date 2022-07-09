/*
 * Created on Wed Jun 16 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use std::thread::available_parallelism;

use {
    libstress::traits::ExitError,
    log::{info, trace, warn},
    rand::thread_rng,
    skytable::Connection,
    std::env,
    sysinfo::{RefreshKind, System, SystemExt},
};
mod linearity_client;
mod utils;

pub const DEFAULT_SIZE_KV: usize = 4;
pub const DEFAULT_QUERY_COUNT: usize = 100_000_usize;

#[macro_export]
macro_rules! logstress {
    ($stressid:expr, $extra:expr) => {
        log::info!("Stress ({}): {}", $stressid, $extra);
    };
}

fn main() {
    // Build the logger
    env_logger::Builder::new()
        .parse_filters(&env::var("SKY_STRESS_LOG").unwrap_or_else(|_| "trace".to_owned()))
        .init();
    warn!("The stress test checks correctness under load and DOES NOT show the true throughput");

    // get the rng and refresh sysinfo
    let mut rng = thread_rng();
    // we only need to refresh memory and CPU info; don't waste time syncing other things
    let to_refresh = RefreshKind::new().with_memory();
    let mut sys = System::new_with_specifics(to_refresh);
    sys.refresh_specifics(to_refresh);
    let core_count = available_parallelism().map_or(1, usize::from);
    let max_workers = core_count * 2;
    trace!(
        "This host has {} logical cores. Will spawn a maximum of {} threads",
        core_count,
        max_workers * 2
    );

    // establish a connection to ensure sanity
    let mut temp_con = Connection::new("127.0.0.1", 2003).exit_error("Failed to connect to server");

    // calculate the maximum keylen
    let max_keylen = utils::calculate_max_keylen(DEFAULT_QUERY_COUNT, &mut sys);
    info!(
        "This host can support a maximum theoretical keylen of: {}",
        max_keylen
    );

    // run the actual stress tests
    linearity_client::stress_linearity_concurrent_clients_set(&mut rng, max_workers, &mut temp_con);
    linearity_client::stress_linearity_concurrent_clients_get(&mut rng, max_workers, &mut temp_con);

    // done, exit
    info!("SUCCESS. Stress test complete!");
}
