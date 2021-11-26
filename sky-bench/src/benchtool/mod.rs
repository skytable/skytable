/*
 * Created on Thu Jun 17 2021
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

use crate::report::AggregatedReport;
use devtimer::DevTime;
use libstress::utils::generate_random_byte_vector;
use libstress::PoolConfig;
use rand::thread_rng;
use skytable::types::RawString;
use skytable::Query;
use std::io::{Read, Write};
use std::net::TcpStream;
mod validation;

use self::validation::SIMPLE_QUERY_SIZE;

const NOTICE_INIT_BENCH: &str = "Finished sanity test. Initializing benchmark ...";
const NOTICE_INIT_COMPLETE: &str = "Initialization complete! Benchmark started";
const CONFIG_TABLE_MODEL: &str = "keymap(binstr,binstr)";
const CONFIG_TABLE_VOLATILITY: &str = "volatile";

/// Run the benchmark tool
pub fn runner(
    host: String,
    port: u16,
    max_connections: usize,
    max_queries: usize,
    per_kv_size: usize,
    json_out: bool,
    runs: usize,
) {
    if !json_out {
        println!("Running sanity test ...");
    }
    if let Err(e) = sanity_test!(host, port) {
        err!(format!("Sanity test failed with error: {}", e));
    }
    if !json_out {
        println!("{}", NOTICE_INIT_BENCH);
        println!("Connections: {}", max_connections);
        println!("Queries: {}", max_queries);
        println!("Data size (key+value): {} bytes", (per_kv_size * 2));
    }

    let host = hoststr!(host, port);
    let mut rand = thread_rng();
    let temp_table = init_temp_table(&mut rand, &host);
    let switch_table = Query::from("use")
        .arg(format!("default:{}", &temp_table))
        .into_raw_query();

    // an okay response code size: `*1\n!1\n0\n`:
    let response_okay_size =
        validation::calculate_monoelement_dataframe_size(1) + SIMPLE_QUERY_SIZE;

    let pool_config = PoolConfig::new(
        max_connections,
        move || {
            let mut stream = TcpStream::connect(&host).unwrap();
            stream.write_all(&switch_table.clone()).unwrap();
            let mut v = vec![0; response_okay_size];
            let _ = stream.read_exact(&mut v).unwrap();
            stream
        },
        move |sock, packet: Vec<u8>| {
            sock.write_all(&packet).unwrap();
            // all `okay`s are returned (for both update and set)
            let mut v = vec![0; response_okay_size];
            let _ = sock.read_exact(&mut v).unwrap();
        },
        |socket| {
            socket.shutdown(std::net::Shutdown::Both).unwrap();
        },
        true,
        Some(max_queries),
    );

    // Create separate connection pools for get and set operations

    let keys = generate_random_byte_vector(max_queries, per_kv_size, &mut rand, true);
    let values = generate_random_byte_vector(max_queries, per_kv_size, &mut rand, false);

    /*
    We create three vectors of vectors: `set_packs`, `get_packs` and `del_packs`
    The bytes in each of `set_packs` has a query packet for setting data;
    The bytes in each of `get_packs` has a query packet for getting a key set by one of `set_packs`
    since we use the same key/value pairs for all;
    The bytes in each of `del_packs` has a query packet for deleting a key created by
    one of `set_packs`
    */
    let set_packs: Vec<Vec<u8>> = (0..max_queries)
        .map(|idx| {
            let mut q = Query::from("SET");
            q.push(RawString::from(keys[idx].clone()));
            q.push(RawString::from(values[idx].clone()));
            q.into_raw_query()
        })
        .collect();
    let get_packs: Vec<Vec<u8>> = (0..max_queries)
        .map(|idx| {
            let mut q = Query::from("GET");
            q.push(RawString::from(keys[idx].clone()));
            q.into_raw_query()
        })
        .collect();
    // just update key -> value to key -> key to avoid unnecessary memory usage
    let update_packs: Vec<Vec<u8>> = (0..max_queries)
        .map(|idx| {
            let mut q = Query::from("UPDATE");
            q.push(RawString::from(keys[idx].clone()));
            q.push(RawString::from(keys[idx].clone()));
            q.into_raw_query()
        })
        .collect();

    if !json_out {
        println!("Per-packet size (GET): {} bytes", get_packs[0].len());
        println!("Per-packet size (SET): {} bytes", set_packs[0].len());
        println!("Per-packet size (UPDATE): {} bytes", update_packs[0].len());
        println!("{}", NOTICE_INIT_COMPLETE);
    }

    let mut report = AggregatedReport::new(3, runs, max_queries);
    for i in 1..runs + 1 {
        let mut dt = DevTime::new_complex();
        // clone in the keys
        let set_packs = set_packs.clone();
        let get_packs = get_packs.clone();
        let update_packs = update_packs.clone();

        // bench SET
        let setpool = pool_config.get_pool();
        dt.create_timer("SET").unwrap();
        dt.start_timer("SET").unwrap();
        setpool.execute_and_finish_iter(set_packs);
        dt.stop_timer("SET").unwrap();

        let get_response_packet_size =
            validation::calculate_monoelement_dataframe_size(per_kv_size) + SIMPLE_QUERY_SIZE;
        let getpool =
            pool_config.with_loop_closure(move |sock: &mut TcpStream, packet: Vec<u8>| {
                sock.write_all(&packet).unwrap();
                // read exact for the key size
                let mut v = vec![0; get_response_packet_size];
                let _ = sock.read_exact(&mut v).unwrap();
            });
        dt.create_timer("GET").unwrap();
        dt.start_timer("GET").unwrap();
        getpool.execute_and_finish_iter(get_packs);
        dt.stop_timer("GET").unwrap();

        // bench UPDATE
        let update_pool = pool_config.get_pool();
        dt.create_timer("UPDATE").unwrap();
        dt.start_timer("UPDATE").unwrap();
        update_pool.execute_and_finish_iter(update_packs);
        dt.stop_timer("UPDATE").unwrap();

        if !json_out {
            println!("Finished run: {}", i);
        }

        // drop table
        let flushdb = Query::new()
            .arg("FLUSHDB")
            .arg(format!("default:{}", &temp_table))
            .into_raw_query();
        let drop_pool = pool_config.get_pool_with_workers(1);
        drop_pool.execute(flushdb);
        drop(drop_pool);
        dt.iter()
            .for_each(|(name, timer)| report.insert(name, timer.time_in_nanos().unwrap()));
    }
    print_results(json_out, report);
}

fn init_temp_table(rand: &mut impl rand::Rng, host: &str) -> String {
    let temp_table = libstress::utils::rand_alphastring(10, rand);
    let create_table = Query::from("create")
        .arg("table")
        .arg(&temp_table)
        .arg(CONFIG_TABLE_MODEL)
        .arg(CONFIG_TABLE_VOLATILITY)
        .into_raw_query();
    let mut create_table_connection = TcpStream::connect(host).unwrap();
    // create table
    create_table_connection.write_all(&create_table).unwrap();
    let mut v = [0u8; 8];
    let _ = create_table_connection.read_exact(&mut v).unwrap();
    temp_table
}

fn print_results(flag_json: bool, report: AggregatedReport) {
    if flag_json {
        let serialized = report.into_json();
        println!("{}", serialized);
    } else {
        println!("===========RESULTS===========");
        let (report, maxpad) = report.into_sorted_stat();
        let pad = |clen: usize| " ".repeat(maxpad - clen);
        report.into_iter().for_each(|block| {
            println!(
                "{}{} {:.6}/sec",
                block.get_report(),
                pad(block.get_report().len()),
                block.get_stat()
            );
        });
        println!("=============================");
    }
}
