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
use libstress::Workpool;
use rand::thread_rng;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Run the benchmark tool
pub fn runner(
    host: String,
    port: u16,
    max_connections: usize,
    max_queries: usize,
    packet_size: usize,
    json_out: bool,
) {
    sanity_test!(host, port);
    if !json_out {
        println!(
            "Initializing benchmark\nConnections: {}\nQueries: {}\nData size (key+value): {} bytes",
            max_connections,
            max_queries,
            (packet_size * 2), // key size + value size
        );
    }
    let host = hoststr!(host, port);
    let host_clone = host.clone();
    let mut rand = thread_rng();
    let mut dt = DevTime::new_complex();

    // create the temporary table to work on
    let util_pool = Workpool::new(
        1,
        move || TcpStream::connect(&host_clone).unwrap(),
        |sock, packet: Vec<u8>| {
            sock.write_all(&packet).unwrap();
            // we don't care much about what's returned
            let _ = sock.read(&mut [0; 1024]).unwrap();
        },
        |socket| {
            socket.shutdown(std::net::Shutdown::Both).unwrap();
        },
        true,
    );
    let temp_table = libstress::utils::rand_alphastring(10, &mut rand);
    let create_table = libsky::into_raw_query(&format!(
        "create table {} keymap(binstr,binstr)",
        &temp_table
    ));
    let switch_table = libsky::into_raw_query(&format!(
        "use default:{} keymap(binstr,binstr)",
        &temp_table
    ));
    let drop_table = libsky::into_raw_query(&format!("drop table {}", &temp_table));
    util_pool.execute(create_table);
    util_pool.execute(switch_table.clone());
    let drop_pool = util_pool.clone();
    drop(util_pool);
    // Create separate connection pools for get and set operations
    let pool_config = PoolConfig::new(
        max_connections,
        move || {
            let mut stream = TcpStream::connect(&host).unwrap();
            stream.write_all(&switch_table).unwrap();
            let _ = stream.read(&mut [0; 1024]).unwrap();
            stream
        },
        |sock, packet: Vec<u8>| {
            sock.write_all(&packet).unwrap();
            // we don't care much about what's returned
            let _ = sock.read(&mut [0; 1024]).unwrap();
        },
        |socket| {
            socket.shutdown(std::net::Shutdown::Both).unwrap();
        },
        true,
    );
    let keys: Vec<String> =
        generate_random_string_vector(max_queries, packet_size, &mut rand, true);
    let values = generate_random_string_vector(max_queries, packet_size, &mut rand, false);
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
    if !json_out {
        println!("Per-packet size (GET): {} bytes", get_packs[0].len());
        println!("Per-packet size (SET): {} bytes", set_packs[0].len());
        println!("Initialization complete! Benchmark started");
    }

    // bench SET
    let setpool = pool_config.get_pool();
    dt.create_timer("SET").unwrap();
    dt.start_timer("SET").unwrap();
    setpool.execute_and_finish_iter(set_packs);
    dt.stop_timer("SET").unwrap();

    // bench GET
    let getpool = pool_config.get_pool();
    dt.create_timer("GET").unwrap();
    dt.start_timer("GET").unwrap();
    getpool.execute_and_finish_iter(get_packs);
    dt.stop_timer("GET").unwrap();

    if !json_out {
        println!("Benchmark completed! Removing created keys...");
    }

    // drop table
    drop_pool.execute(drop_table);
    drop(drop_pool);

    let gets_per_sec = calc(max_queries, dt.time_in_nanos("GET").unwrap());
    let sets_per_sec = calc(max_queries, dt.time_in_nanos("SET").unwrap());
    if json_out {
        let dat = vec![
            JSONReportBlock::new("GET", gets_per_sec),
            JSONReportBlock::new("SET", sets_per_sec),
        ];
        let serialized = serde_json::to_string(&dat).unwrap();
        println!("{}", serialized);
    } else {
        println!("==========RESULTS==========");
        println!("{} GETs/sec", gets_per_sec);
        println!("{} SETs/sec", sets_per_sec);
        println!("===========================");
    }
}
