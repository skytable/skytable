/*
 * Created on Sun Sep 13 2020
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

//! A generic module for benchmarking SET/GET operations
//! **NOTE:** This is experimental and may show incorrect results - that is,
//! the response times may be shown to be slower than they actually are

mod benchtool {
    use clap::{load_yaml, App};
    use devtimer::DevTime;
    use libtdb::terrapipe;
    use rand::distributions::Alphanumeric;
    use rand::thread_rng;
    use serde::Serialize;
    use std::io::prelude::*;
    use std::net::{self, TcpStream};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    /// A Netpool is a threadpool that holds several workers
    ///
    /// Essentially, a `NetPool` is a connection pool
    pub struct Netpool {
        workers: Vec<Worker>,
        sender: mpsc::Sender<WhatToDo>,
    }
    /// The job
    ///
    /// A `NewJob` has a `Vec<u8>` field for the bytes it has to write to a stream held
    /// by the worker. If the `Job` is `Nothing`, then it is time for the worker
    /// to terminate
    enum WhatToDo {
        NewJob(Vec<u8>),
        Nothing,
    }
    /// A worker holds a thread which also holds a persistent connection to
    /// `localhost:2003`, as long as the thread is not told to terminate
    struct Worker {
        thread: Option<thread::JoinHandle<()>>,
    }
    impl Netpool {
        /// Create a new `Netpool` instance with `size` number of connections (and threads)
        pub fn new(size: usize, host: &String) -> Netpool {
            assert!(size > 0);
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            let mut workers = Vec::with_capacity(size);
            for _ in 0..size {
                workers.push(Worker::new(Arc::clone(&receiver), host.to_owned()));
            }
            Netpool { workers, sender }
        }
        /// Execute the job
        pub fn execute(&mut self, action: Vec<u8>) {
            self.sender.send(WhatToDo::NewJob(action)).unwrap();
        }
    }
    impl Worker {
        /// Create a new `Worker` which also means that a connection to port 2003
        /// will be established
        fn new(
            receiver: Arc<Mutex<mpsc::Receiver<WhatToDo>>>,
            host: std::string::String,
        ) -> Worker {
            let thread = thread::spawn(move || {
                let mut connection = TcpStream::connect(host).unwrap();
                loop {
                    let action = receiver.lock().unwrap().recv().unwrap();
                    match action {
                        WhatToDo::NewJob(someaction) => {
                            // We have to write something to the socket
                            connection.write_all(&someaction).unwrap();
                            // Ignore whatever we get, we don't need them
                            connection.read(&mut vec![0; 1024]).unwrap();
                        }
                        WhatToDo::Nothing => {
                            // A termination signal - just close the stream and
                            // return
                            connection.shutdown(net::Shutdown::Both).unwrap();
                            break;
                        }
                    }
                }
            });
            Worker {
                thread: Some(thread),
            }
        }
    }
    impl Drop for Netpool {
        fn drop(&mut self) {
            // Signal all the workers to shut down
            for _ in &mut self.workers {
                self.sender.send(WhatToDo::Nothing).unwrap();
            }
            // Terminate all the threads
            for worker in &mut self.workers {
                if let Some(thread) = worker.thread.take() {
                    thread.join().unwrap();
                }
            }
        }
    }

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
        host.push(':');
        match matches.value_of("port") {
            Some(p) => match p.parse::<u16>() {
                Ok(p) => host.push_str(&p.to_string()),
                Err(_) => {
                    eprintln!("ERROR: Invalid port");
                    std::process::exit(0x100);
                }
            },
            None => host.push_str("2003"),
        }
        let mut rand = thread_rng();
        if let Some(matches) = matches.subcommand_matches("testkey") {
            let numkeys = matches.value_of("count").unwrap();
            if let Ok(num) = numkeys.parse::<usize>() {
                let mut np = Netpool::new(10, &host);
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
                    .map(|idx| terrapipe::proc_query(format!("SET {} {}", keys[idx], values[idx])))
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
        let mut setpool = Netpool::new(max_connections, &host);
        let mut getpool = Netpool::new(max_connections, &host);
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
            .map(|idx| terrapipe::proc_query(format!("SET {} {}", keys[idx], values[idx])))
            .collect();
        let get_packs: Vec<Vec<u8>> = (0..max_queries)
            .map(|idx| terrapipe::proc_query(format!("GET {}", keys[idx])))
            .collect();
        let del_packs: Vec<Vec<u8>> = (0..max_queries)
            .map(|idx| terrapipe::proc_query(format!("DEL {}", keys[idx])))
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
        let mut delpool = Netpool::new(max_connections, &host);
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

    /// Returns the number of queries/sec
    fn calc(reqs: usize, time: u128) -> f64 {
        reqs as f64 / (time as f64 / 1_000_000_000 as f64)
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
