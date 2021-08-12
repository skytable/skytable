/*
 * Created on Fri Jun 18 2021
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

//! # Client linearity tests
//!
//! This module contains functions to test the linearity of the database with increasing number
//! of clients, i.e how the number of queries scale with increasing clients. These functions
//! however, DO NOT focus on benchmarking and instead focus on correctness under load from
//! concurrent clients.
//!

use crate::logstress;
use crate::{DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV};
use crossbeam_channel::bounded;
use devtimer::SimpleTimer;
use libstress::rayon::prelude::*;
use libstress::utils::generate_random_string_vector;
use libstress::Workpool;
use skytable::actions::Actions;
use skytable::query;
use skytable::Connection;
use skytable::{Element, Query, RespCode, Response};

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

/// This object provides methods to measure the percentage change in the slope
/// of a function that is expected to have linearity.
///
/// For example, we can think of it to work in the following way:
/// Let h(x) be a function with linearity (not proportionality because that isn't
/// applicable in our case). h(x) gives us the time taken to run a given number of
/// queries (invariant; not plotted on axes), where x is the number of concurrent
/// clients. As we would want our database to scale with increasing clients (and cores),
/// we'd expect linearity, hence the gradient should continue to fall with increasing
/// values in the +ve x-axis effectively producing a constantly decreasing slope, reflected
/// by increasing values of abs(get_delta(h(x))).
///
/// TODO(@ohsayan): Of course, some unexpected kernel errors/scheduler hiccups et al can
/// cause there to be a certain epsilon that must be tolerated with a tolerance factor
///
pub struct LinearityMeter {
    init: Option<u128>,
    measure: Vec<f32>,
}

impl LinearityMeter {
    pub const fn new() -> Self {
        Self {
            init: None,
            measure: Vec::new(),
        }
    }
    pub fn get_delta(&mut self, current: u128) -> f32 {
        if let Some(u) = self.init {
            let cur = ((current as f32 - u as f32) / u as f32) * 100.00_f32;
            self.measure.push(cur);
            cur
        } else {
            // if init is not initialized, initialize it
            self.init = Some(current);
            // no change when at base
            0.00
        }
    }
}

pub fn stress_linearity_concurrent_clients_set(
    mut rng: &mut impl rand::Rng,
    max_workers: usize,
    temp_con: &mut Connection,
) {
    logstress!(
        "A [SET]",
        "Linearity test with monotonically increasing clients"
    );
    let mut current_thread_count = 1usize;

    // generate the random k/v pairs
    let keys = generate_random_string_vector(DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV, &mut rng, true);
    let values: Vec<String> =
        generate_random_string_vector(DEFAULT_QUERY_COUNT, DEFAULT_SIZE_KV, &mut rng, false);

    // make sure the database is empty
    temp_con.flushdb().unwrap();

    // initialize the linearity counter
    let mut linearity = LinearityMeter::new();
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
            Some(DEFAULT_QUERY_COUNT),
        );
        let mut timer = SimpleTimer::new();
        timer.start();
        workpool.execute_and_finish_iter(set_packs);
        timer.stop();
        log::info!(
            "Delta: {}%",
            linearity.get_delta(timer.time_in_nanos().unwrap())
        );
        // clean up the database
        temp_con.flushdb().unwrap();
        current_thread_count += 1;
    }
}

pub fn stress_linearity_concurrent_clients_get(
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
        Some(DEFAULT_QUERY_COUNT),
    );
    workpool.execute_and_finish_iter(set_packs);

    // initialize the linearity counter
    let mut linearity = LinearityMeter::new();
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
            Some(DEFAULT_QUERY_COUNT),
        );
        let mut timer = SimpleTimer::new();
        timer.start();
        wp.execute_and_finish_iter(get_packs);
        timer.stop();
        log::info!(
            "Delta: {}%",
            linearity.get_delta(timer.time_in_nanos().unwrap())
        );
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
