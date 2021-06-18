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

use libstress::traits::ExitError;
use libstress::utils::ran_string;
use libstress::Workpool;
use log::{info, trace};
use rand::thread_rng;
use skytable::actions::Actions;
use skytable::Connection;
use skytable::Query;
use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use sysinfo::{System, SystemExt};

pub const DEFAULT_SIZE_KV: usize = 4;
pub const DEFAULT_QUERY_COUNT: usize = 100_000_usize;

macro_rules! logstress {
    ($stressid:expr, $extra:expr) => {
        log::info!("Stress ({}): {}", stringify!($stressid), $extra);
    };
}

macro_rules! log_client_linearity {
    ($stressid:expr, $counter:expr) => {
        log::info!(
            "Stress ({}{}): Clients: {}; K/V size: {}; Queries: {}",
            stringify!($stressid),
            $counter,
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
    let mut rng = thread_rng();
    stress_linearity_concurrent_clients(&mut rng);
    info!("SUCCESS. Stress test complete!");
}

fn stress_linearity_concurrent_clients(mut rng: &mut impl rand::Rng) {
    logstress!(A, "Linearity test with monotonically increasing clients");
    let mut sys = System::new_all();
    sys.refresh_all();
    let num_workers = sys
        .get_physical_core_count()
        .exit_error("Failed to get physical core count")
        * 2;
    trace!("Will spawn a maximum of {} workers", num_workers * 2);
    let mut current_thread_count = 1usize;
    let mut temp_con = Connection::new("127.0.0.1", 2003).exit_error("Failed to connect to server");
    temp_con.flushdb().unwrap();
    let keys: Vec<String> = (0..DEFAULT_QUERY_COUNT)
        .into_iter()
        .map(|_| ran_string(DEFAULT_SIZE_KV, &mut rng))
        .collect();
    let values: Vec<String> = (0..DEFAULT_QUERY_COUNT)
        .into_iter()
        .map(|_| ran_string(DEFAULT_SIZE_KV, &mut rng))
        .collect();
    while current_thread_count < (num_workers + 1) {
        log_client_linearity!(A, current_thread_count);
        let set_packs: Vec<Query> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| {
                let mut q = Query::from("SET");
                q.push(k);
                q.push(v);
                q
            })
            .collect();
        let responses = Arc::new(Mutex::new(Vec::new()));
        let workpool = Workpool::new(
            current_thread_count,
            || Connection::new("127.0.0.1", 2003).unwrap(),
            move |sock, query| {
                let resp = responses.clone();
                resp.lock()
                    .unwrap()
                    .push(sock.run_simple_query(&query).unwrap());
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

fn calculate_max_keylen(expected_queries: usize, sys: System) -> usize {
    let total_mem_in_bytes = (sys.get_total_memory() * 1024) as usize;
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
        highest_len = (highest_len as f32 * 1.21_f32).ceil() as usize;
    }
    highest_len
}
