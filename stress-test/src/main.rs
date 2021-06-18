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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]

use crossbeam_channel::bounded;
use libstress::rayon::prelude::*;
use libstress::traits::ExitError;
use libstress::utils::generate_random_string_vector;
use libstress::Workpool;
use log::{info, trace, warn};
use rand::thread_rng;
use skytable::actions::Actions;
use skytable::query;
use skytable::Connection;
use skytable::{Element, Query, RespCode, Response};
use std::env;
use sysinfo::{RefreshKind, System, SystemExt};

pub const DEFAULT_SIZE_KV: usize = 4;
pub const DEFAULT_QUERY_COUNT: usize = 100_000_usize;

#[macro_export]
macro_rules! logstress {
    ($stressid:expr, $extra:expr) => {
        log::info!("Stress ({}): {}", $stressid, $extra);
    };
}

#[macro_export]
macro_rules! log_client_linearity {
    ($stressid:expr, $counter:expr, $what:expr) => {
        log::info!(
            "Stress ({}{}) [{}]: Clients: {}; K/V size: {}; Queries: {}",
            $stressid,
            $counter,
            $what,
            $counter,
            DEFAULT_SIZE_KV,
            DEFAULT_QUERY_COUNT
        );
    };
}

fn main() {
    env_logger::Builder::new()
        .parse_filters(&env::var("SKY_LOG").unwrap_or_else(|_| "trace".to_owned()))
        .init();
    warn!("The stress test checks correctness under load and DOES NOT show the true throughput");
    let mut rng = thread_rng();
    let to_refresh = RefreshKind::new().with_memory().with_cpu();
    let mut sys = System::new_with_specifics(to_refresh);
    sys.refresh_specifics(to_refresh);
    let core_count = sys
        .get_physical_core_count()
        .exit_error("Failed to get physical core count");
    let max_workers = core_count * 2;
    trace!(
        "This host has {} logical cores. Will spawn a maximum of {} workers",
        core_count,
        max_workers * 2
    );
    let mut temp_con = Connection::new("127.0.0.1", 2003).exit_error("Failed to connect to server");
    stress_linearity_concurrent_clients_set(&mut rng, max_workers, &mut temp_con);
    stress_linearity_concurrent_clients_get(&mut rng, max_workers, &mut temp_con);
    let max_keylen = calculate_max_keylen(DEFAULT_QUERY_COUNT, &mut sys);
    info!(
        "This host can support a maximum theoretical keylen of: {}",
        max_keylen
    );
    info!("SUCCESS. Stress test complete!");
}

fn stress_linearity_concurrent_clients_set(
    mut rng: &mut impl rand::Rng,
    max_workers: usize,
    temp_con: &mut Connection,
) {
    logstress!(
        "A [SET]",
        "Linearity test with monotonically increasing clients"
    );

    // generate the random k/v pairs
    let keys = generate_random_string_vector(DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV, &mut rng, true);
    let values: Vec<String> =
        generate_random_string_vector(DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV, &mut rng, false);
    let mut current_thread_count = 1usize;

    // make sure the database is empty
    temp_con.flushdb().unwrap();
    while current_thread_count <= max_workers {
        log_client_linearity!("A", current_thread_count, "SET");

        // generate the set packets
        let set_packs: Vec<Query> = keys
            .par_iter()
            .zip(values.par_iter())
            .map(|(k, v)| query!("SET", k, v))
            .collect();
        let workpool = Workpool::new(
            current_thread_count,
            || Connection::new("127.0.0.1", 2003).unwrap(),
            move |sock, query| {
                assert_eq!(
                    sock.run_simple_query(&query).unwrap(),
                    Response::Item(Element::RespCode(RespCode::Okay))
                );
            },
            |_| {},
            true,
        );
        workpool.execute_and_finish_iter(set_packs);
        // clean up the database
        temp_con.flushdb().unwrap();
        current_thread_count += 1;
    }
}

fn stress_linearity_concurrent_clients_get(
    mut rng: &mut impl rand::Rng,
    max_workers: usize,
    temp_con: &mut Connection,
) {
    logstress!(
        "A [GET]",
        "Linearity test with monotonically increasing clients"
    );
    let mut current_thread_count = 1usize;

    // Generate the random k/v pairs
    let keys = generate_random_string_vector(DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV, &mut rng, true);
    let values: Vec<String> =
        generate_random_string_vector(DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV, &mut rng, false);

    // Make sure that the database is empty
    temp_con.flushdb().unwrap();

    // First set the keys
    let set_packs: Vec<Query> = keys
        .par_iter()
        .zip(values.par_iter())
        .map(|(k, v)| query!("SET", k, v))
        .collect();
    let workpool = Workpool::new_default_threads(
        || Connection::new("127.0.0.1", 2003).unwrap(),
        move |sock, query| {
            assert_eq!(
                sock.run_simple_query(&query).unwrap(),
                Response::Item(Element::RespCode(RespCode::Okay))
            );
        },
        |_| {},
        true,
    );
    workpool.execute_and_finish_iter(set_packs);
    while current_thread_count <= max_workers {
        log_client_linearity!("A", current_thread_count, "GET");
        /*
         We create a  mpmc to receive the results returned. This avoids us using
         any kind of locking on the surface which can slow down things
        */
        let (tx, rx) = bounded::<Response>(DEFAULT_QUERY_COUNT);

        // generate the get packets
        let get_packs: Vec<Query> = keys.iter().map(|k| query!("GET", k)).collect();
        let wp = Workpool::new(
            current_thread_count,
            || Connection::new("127.0.0.1", 2003).unwrap(),
            move |sock, query| {
                let tx = tx.clone();
                tx.send(sock.run_simple_query(&query).unwrap()).unwrap();
            },
            |_| {},
            true,
        );
        wp.execute_and_finish_iter(get_packs);
        let rets: Vec<String> = rx
            .into_iter()
            .map(|v| {
                if let Response::Item(Element::String(val)) = v {
                    val
                } else {
                    panic!("Unexpected response from server");
                }
            })
            .collect();
        assert_eq!(
            rets.len(),
            values.len(),
            "Incorrect number of values returned by server"
        );

        // now evaluate them
        assert!(
            rets.into_par_iter().all(|v| values.contains(&v)),
            "Values returned by the server don't match what was sent"
        );
        current_thread_count += 1;
    }
    temp_con.flushdb().unwrap();
}

fn calculate_max_keylen(expected_queries: usize, sys: &mut System) -> usize {
    let total_mem_in_bytes = (sys.get_total_memory() * 1024) as usize;
    trace!(
        "This host has a total memory of: {} Bytes",
        total_mem_in_bytes
    );
    // av_mem gives us 90% of the memory size
    let ninety_percent_of_memory = (0.90_f32 * total_mem_in_bytes as f32) as usize;
    let mut highest_len = 1usize;
    loop {
        let set_pack_len = Query::array_packet_size_hint(vec![3, highest_len, highest_len]);
        let get_pack_len = Query::array_packet_size_hint(vec![3, highest_len]);
        let resulting_size = expected_queries
            * (
                // for the set packets
                set_pack_len +
                // for the get packets
                get_pack_len +
                // for the keys themselves
                highest_len
            );
        if resulting_size >= ninety_percent_of_memory as usize {
            break;
        }
        // increase the length by 5% every time to get the maximum possible length
        // now this 5% increment is a tradeoff, but it's worth it to not wait for
        // so long
        highest_len = (highest_len as f32 * 1.05_f32).ceil() as usize;
    }
    highest_len
}
