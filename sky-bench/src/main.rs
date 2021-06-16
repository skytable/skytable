/*
 * Created on Sun Sep 13 2020
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

//! A generic module for benchmarking SET/GET operations
//! **NOTE:** This is experimental and may show incorrect results - that is,
//! the response times may be shown to be slower than they actually are

mod benchtool {
    use clap::{load_yaml, App};
    use devtimer::DevTime;
    use libstress::Workpool;
    use rand::distributions::Alphanumeric;
    use rand::thread_rng;
    use serde::Serialize;
    use std::error::Error;
    use std::io::prelude::*;
    use std::net::{self, TcpStream};

    #[derive(Serialize)]
    /// A `JSONReportBlock` represents a JSON object which contains the type of report
    /// (for example `GET` or `SET`) and the number of such queries per second
    ///
    /// This is an example of the object, when serialized into JSON:
    /// ```json
    /// {
    ///     "report" : "GET",
    ///     "stat" : 123456789.10,
    /// }
    /// ```
    pub struct JSONReportBlock {
        /// The type of benchmark
        report: String,
        /// The number of such queries per second
        stat: f64,
    }

    impl JSONReportBlock {
        pub fn new(report: &'static str, stat: f64) -> Self {
            JSONReportBlock {
                report: report.to_owned(),
                stat,
            }
        }
    }

    /// Run the benchmark tool
    pub fn runner() {
        let cfg_layout = load_yaml!("./cli.yml");
        let matches = App::from_yaml(cfg_layout).get_matches();
        let mut host = match matches.value_of("host") {
            Some(h) => h.to_owned(),
            None => "127.0.0.1".to_owned(),
        };
        let port = match matches.value_of("port") {
            Some(p) => match p.parse::<u16>() {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("ERROR: Invalid port");
                    std::process::exit(0x100);
                }
            },
            None => 2003,
        };
        println!("Running a sanity test...");
        // Run a sanity test
        if let Err(e) = sanity_test(&host, port) {
            eprintln!("ERROR: Sanity test failed: {}\nBenchmark terminated", e);
            return;
        }
        println!("Sanity test succeeded");
        // now push in the port to the host string
        host.push(':');
        host.push_str(&port.to_string());
        let mut rand = thread_rng();
        if let Some(matches) = matches.subcommand_matches("testkey") {
            let numkeys = matches.value_of("count").unwrap();
            if let Ok(num) = numkeys.parse::<usize>() {
                let mut np = Workpool::new(
                    10,
                    move || TcpStream::connect(host.clone()).unwrap(),
                    |sock, packet: Vec<u8>| {
                        sock.write_all(&packet).unwrap();
                        let _ = sock.read(&mut vec![0; 1024]).unwrap();
                    },
                    |socket| {
                        socket.shutdown(net::Shutdown::Both).unwrap();
                    },
                );
                println!("Generating keys ...");
                let keys: Vec<String> = (0..num)
                    .into_iter()
                    .map(|_| ran_string(8, &mut rand))
                    .collect();
                let values: Vec<String> = (0..num)
                    .into_iter()
                    .map(|_| ran_string(8, &mut rand))
                    .collect();
                let set_packs: Vec<Vec<u8>> = (0..num)
                    .map(|idx| {
                        libsky::into_raw_query(&format!("SET {} {}", keys[idx], values[idx]))
                    })
                    .collect();
                set_packs.into_iter().for_each(|packet| {
                    np.execute(packet);
                });
                drop(np);
                println!("Created mock keys successfully");
                return;
            } else {
                eprintln!("ERROR: Invalid value for `count`");
                std::process::exit(0x100);
            }
        }
        let json_out = matches.is_present("json");
        let (max_connections, max_queries, packet_size) = match (
            matches
                .value_of("connections")
                .unwrap_or("10")
                .parse::<usize>(),
            matches
                .value_of("queries")
                .unwrap_or("100000")
                .parse::<usize>(),
            matches.value_of("size").unwrap_or("4").parse::<usize>(),
        ) {
            (Ok(mx), Ok(mc), Ok(ps)) => (mx, mc, ps),
            _ => {
                eprintln!("Incorrect arguments");
                std::process::exit(0x100);
            }
        };
        eprintln!(
            "Initializing benchmark\nConnections: {}\nQueries: {}\nData size (key+value): {} bytes",
            max_connections,
            max_queries,
            (packet_size * 2), // key size + value size
        );
        let mut rand = thread_rng();
        let mut dt = DevTime::new_complex();
        // Create separate connection pools for get and set operations
        let mut setpool = Workpool::new(
            10,
            move || TcpStream::connect(host.clone()).unwrap(),
            |sock, packet: Vec<u8>| {
                sock.write_all(&packet).unwrap();
                // we don't care much about what's returned
                let _ = sock.read(&mut vec![0; 1024]).unwrap();
            },
            |socket| {
                socket.shutdown(std::net::Shutdown::Both).unwrap();
            },
        );
        let mut getpool = setpool.clone();
        let mut delpool = getpool.clone();
        let keys: Vec<String> = (0..max_queries)
            .into_iter()
            .map(|_| ran_string(packet_size, &mut rand))
            .collect();
        let values: Vec<String> = (0..max_queries)
            .into_iter()
            .map(|_| ran_string(packet_size, &mut rand))
            .collect();
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
        let del_packs: Vec<Vec<u8>> = (0..max_queries)
            .map(|idx| libsky::into_raw_query(&format!("DEL {}", keys[idx])))
            .collect();
        eprintln!("Per-packet size (GET): {} bytes", get_packs[0].len());
        eprintln!("Per-packet size (SET): {} bytes", set_packs[0].len());
        eprintln!("Initialization complete! Benchmark started");
        dt.create_timer("SET").unwrap();
        dt.start_timer("SET").unwrap();
        for packet in set_packs {
            setpool.execute(packet);
        }
        drop(setpool);
        dt.stop_timer("SET").unwrap();
        dt.create_timer("GET").unwrap();
        dt.start_timer("GET").unwrap();
        for packet in get_packs {
            getpool.execute(packet);
        }
        drop(getpool);
        dt.stop_timer("GET").unwrap();
        eprintln!("Benchmark completed! Removing created keys...");
        // Create a connection pool for del operations
        // Delete all the created keys
        for packet in del_packs {
            delpool.execute(packet);
        }
        drop(delpool);
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

    /// # Sanity Test
    ///
    /// This function performs a 'sanity test' to determine if the benchmarks should be run; this test ensures
    /// that the server is functioning as expected and we'll run the benchmarks assuming that the server will
    /// act similarly in the future. This test currently runs a HEYA, SET, GET and DEL test, the latter three of which
    /// are the ones that are benchmarked
    ///
    /// ## Limitations
    /// A 65535 character long key/value pair is created and fetched. This random string has extremely low
    /// chances of colliding with any existing key
    fn sanity_test(host: &str, port: u16) -> Result<(), Box<dyn Error>> {
        use skytable::{Connection, Element, Query, RespCode, Response};
        let mut rng = thread_rng();
        let mut connection = Connection::new(host, port)?;
        // test heya
        let mut query = Query::new();
        query.push("heya");
        if !connection
            .run_simple_query(&query)
            .unwrap()
            .eq(&Response::Item(Element::String("HEY!".to_owned())))
        {
            return Err("HEYA test failed".into());
        }
        let key = ran_string(65536, &mut rng);
        let value = ran_string(65536, &mut rng);
        let mut query = Query::new();
        query.push("set");
        query.push(&key);
        query.push(&value);
        if !connection
            .run_simple_query(&query)
            .unwrap()
            .eq(&Response::Item(Element::RespCode(RespCode::Okay)))
        {
            return Err("SET test failed".into());
        }
        let mut query = Query::new();
        query.push("get");
        query.push(&key);
        if !connection
            .run_simple_query(&query)
            .unwrap()
            .eq(&Response::Item(Element::String(value)))
        {
            return Err("GET test failed".into());
        }
        let mut query = Query::new();
        query.push("del");
        query.push(&key);
        if !connection
            .run_simple_query(&query)
            .unwrap()
            .eq(&Response::Item(Element::UnsignedInt(1)))
        {
            return Err("DEL test failed".into());
        }
        Ok(())
    }

    /// Returns the number of queries/sec
    fn calc(reqs: usize, time: u128) -> f64 {
        reqs as f64 / (time as f64 / 1_000_000_000_f64)
    }

    fn ran_string(len: usize, rand: impl rand::Rng) -> String {
        let rand_string: String = rand
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect();
        rand_string
    }
}

fn main() {
    benchtool::runner();
}
