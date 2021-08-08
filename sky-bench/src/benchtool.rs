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

use crate::hoststr;
use crate::sanity_test;
use crate::util::calc;
use crate::util::JSONReportBlock;
use devtimer::DevTime;
use libstress::utils::generate_random_string_vector;
use libstress::PoolConfig;
use rand::thread_rng;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Just a sweet `*1\n`
const SIMPLE_QUERY_SIZE: usize = 3;

/// For a dataframe, this returns the dataframe size for array responses.
///
/// For example,
/// ```text
/// &<n>\n
/// (<tsymbol><size>\n<element>)*
/// ```
#[allow(dead_code)] // TODO(@ohsayan): Remove this lint
pub fn calculate_array_dataframe_size(element_count: usize, per_element_size: usize) -> usize {
    let mut s = 0;
    s += 1; // `&`
    s += element_count.to_string().len(); // `<n>`
    s += 1; // `\n`
    let mut subsize = 0;
    subsize += 1; // `+`
    subsize += per_element_size.to_string().len(); // `<n>`
    subsize += 1; // `\n`
    subsize += per_element_size; // the element size itself
    subsize += 1; // `\n`
    s += subsize * element_count;
    s
}

/// For a monoelement dataframe, this returns the size:
/// ```text
/// <tsymbol><size>\n
/// <element>\n
/// ```
///
/// For an `okay` respcode, it will look like this:
/// ```text
/// !1\n
/// 0\n
/// ```
pub fn calculate_monoelement_dataframe_size(per_element_size: usize) -> usize {
    let mut s = 0;
    s += 1; // the tsymbol (always one byte)
    s += per_element_size.to_string().len(); // the bytes in size string
    s += 1; // the LF
    s += per_element_size; // the element itself
    s += 1; // the final LF
    s
}

#[test]
fn test_monoelement_calculation() {
    assert_eq!(calculate_monoelement_dataframe_size(1), 5);
}

/// Returns the metaframe size
/// ```text
/// *<n>\n
/// ```
#[allow(dead_code)] // TODO(@ohsayan): Remove this lint
pub fn calculate_metaframe_size(queries: usize) -> usize {
    let mut s = 0;
    s += 1; // `*`
    s += queries.to_string().len(); // the bytes in size string
    s += 1; // `\n`
    s
}

#[test]
fn test_simple_query_metaframe_size() {
    assert_eq!(calculate_metaframe_size(1), SIMPLE_QUERY_SIZE);
}

/// Run the benchmark tool
pub fn runner(
    host: String,
    port: u16,
    max_connections: usize,
    max_queries: usize,
    per_kv_size: usize,
    json_out: bool,
) {
    if let Err(e) = sanity_test!(host, port) {
        err!(format!("Sanity test failed with error: {}", e));
    }
    if !json_out {
        println!(
            "Initializing benchmark\nConnections: {}\nQueries: {}\nData size (key+value): {} bytes",
            max_connections,
            max_queries,
            (per_kv_size * 2), // key size + value size
        );
    }
    let host = hoststr!(host, port);
    let mut rand = thread_rng();
    let mut dt = DevTime::new_complex();

    let temp_table = libstress::utils::rand_alphastring(10, &mut rand);
    let create_table = libsky::into_raw_query(&format!(
        "create table {} keymap(binstr,binstr)",
        &temp_table
    ));
    let switch_table = libsky::into_raw_query(&format!(
        "use default:{} keymap(binstr,binstr)",
        &temp_table
    ));

    // an okay response code size: `*1\n!1\n0\n`:
    let response_okay_size = calculate_monoelement_dataframe_size(1) + SIMPLE_QUERY_SIZE;

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
    );

    let drop_table = libsky::into_raw_query(&format!("drop table {}", &temp_table));
    let util_pool = pool_config.get_pool_with_workers(1);
    util_pool.execute(create_table);
    drop(util_pool);
    // Create separate connection pools for get and set operations

    let keys: Vec<String> =
        generate_random_string_vector(max_queries, per_kv_size, &mut rand, true);
    let values = generate_random_string_vector(max_queries, per_kv_size, &mut rand, false);
    /*
    We create three vectors of vectors: `set_packs`, `get_packs` and `del_packs`
    The bytes in each of `set_packs` has a query packet for setting data;
    The bytes in each of `get_packs` has a query packet for getting a key set by one of `set_packs`
    since we use the same key/value pairs for all;
    The bytes in each of `del_packs` has a query packet for deleting a key created by
    one of `set_packs`
    */
    let set_packs: Vec<Vec<u8>> = (0..max_queries)
        .map(|idx| libsky::into_raw_query(&format!("SET {} {}", keys[idx], values[idx])))
        .collect();
    let get_packs: Vec<Vec<u8>> = (0..max_queries)
        .map(|idx| libsky::into_raw_query(&format!("GET {}", keys[idx])))
        .collect();
    // just update key -> value to key -> key to avoid unnecessary memory usage
    let update_packs: Vec<Vec<u8>> = (0..max_queries)
        .map(|idx| libsky::into_raw_query(&format!("UPDATE {} {}", keys[idx], keys[idx])))
        .collect();
    if !json_out {
        println!("Per-packet size (GET): {} bytes", get_packs[0].len());
        println!("Per-packet size (SET): {} bytes", set_packs[0].len());
        println!("Per-packet size (UPDATE): {} bytes", update_packs[0].len());
        println!("Initialization complete! Benchmark started");
    }

    // bench SET
    let setpool = pool_config.get_pool();
    dt.create_timer("SET").unwrap();
    dt.start_timer("SET").unwrap();
    setpool.execute_and_finish_iter(set_packs);
    dt.stop_timer("SET").unwrap();

    // TODO: Update the getpool to use correct sizes
    // bench GET
    let get_response_packet_size =
        calculate_monoelement_dataframe_size(per_kv_size) + SIMPLE_QUERY_SIZE;
    let getpool = pool_config.with_loop_closure(move |sock: &mut TcpStream, packet: Vec<u8>| {
        sock.write_all(&packet).unwrap();
        // all `okay`s are returned (for both update and set)
        let mut v = vec![0; get_response_packet_size];
        let _ = sock.read(&mut v).unwrap();
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
        println!("Benchmark completed! Removing created keys...");
    }

    // drop table
    let drop_pool = pool_config.get_pool_with_workers(1);
    drop_pool.execute(drop_table);
    drop(drop_pool);

    let gets_per_sec = calc(max_queries, dt.time_in_nanos("GET").unwrap());
    let sets_per_sec = calc(max_queries, dt.time_in_nanos("SET").unwrap());
    let updates_per_sec = calc(max_queries, dt.time_in_nanos("UPDATE").unwrap());
    if json_out {
        let dat = vec![
            JSONReportBlock::new("GET", gets_per_sec),
            JSONReportBlock::new("SET", sets_per_sec),
            JSONReportBlock::new("UPDATE", updates_per_sec),
        ];
        let serialized = serde_json::to_string(&dat).unwrap();
        println!("{}", serialized);
    } else {
        println!("==========RESULTS==========");
        println!("{} GETs/sec", gets_per_sec);
        println!("{} SETs/sec", sets_per_sec);
        println!("{} UPDATEs/sec", updates_per_sec);
        println!("===========================");
    }
}
