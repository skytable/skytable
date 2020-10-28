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
    use devtimer::DevTime;
    use libtdb::terrapipe;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
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
        pub fn new(size: usize) -> Netpool {
            assert!(size > 0);
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            let mut workers = Vec::with_capacity(size);
            for _ in 0..size {
                workers.push(Worker::new(Arc::clone(&receiver)));
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
        fn new(receiver: Arc<Mutex<mpsc::Receiver<WhatToDo>>>) -> Worker {
            let thread = thread::spawn(move || {
                let mut connection = TcpStream::connect("127.0.0.1:2003").unwrap();
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

    /// Run the benchmark tool
    pub fn runner() {
        let mut args: Vec<String> = std::env::args().collect();
        args.remove(0);
        println!(
            "------------------------------------------------------------\
            \nTerrabaseDB Benchmark Tool v0.1.0\
            \nReport issues here: https://github.com/terrabasedb/terrabasedb\
            \n------------------------------------------------------------"
        );
        // connections queries packetsize
        if args.len() != 3 {
            eprintln!(
                "Insufficient arguments!\
                \nUSAGE: tdb-bench <connections> <queries> <packetsize-in-bytes>"
            );
            std::process::exit(0x100);
        }
        let (max_connections, max_queries, packet_size) = match (
            args[0].parse::<usize>(),
            args[1].parse::<usize>(),
            args[2].parse::<usize>(),
        ) {
            (Ok(mx), Ok(mc), Ok(ps)) => (mx, mc, ps),
            _ => {
                eprintln!("Incorrect arguments");
                std::process::exit(0x100);
            }
        };
        println!(
            "Initializing benchmark\nConnections: {}\nQueries: {}\nData size (key+value): {} bytes",
            max_connections,
            max_queries,
            (packet_size * 2), // key size + value size
        );
        let rand = thread_rng();
        let mut dt = DevTime::new_complex();
        // Create separate connection pools for get and set operations
        let mut setpool = Netpool::new(max_connections);
        let mut getpool = Netpool::new(max_connections);
        let keys: Vec<String> = (0..max_queries)
            .into_iter()
            .map(|_| {
                let rand_string: String =
                    rand.sample_iter(&Alphanumeric).take(packet_size).collect();
                rand_string
            })
            .collect();
        let values: Vec<String> = (0..max_queries)
            .into_iter()
            .map(|_| {
                let rand_string: String =
                    rand.sample_iter(&Alphanumeric).take(packet_size).collect();
                rand_string
            })
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
        println!("Per-packet size (GET): {} bytes", get_packs[0].len());
        println!("Per-packet size (SET): {} bytes", set_packs[0].len());
        println!("Initialization complete! Benchmark started");
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
        println!("Benchmark completed! Removing created keys...");
        // Create a connection pool for del operations
        let mut delpool = Netpool::new(max_connections);
        // Delete all the created keys
        for packet in del_packs {
            delpool.execute(packet);
        }
        drop(delpool);
        println!("==========RESULTS==========");
        println!(
            "{} GETs/sec",
            calc(max_queries, dt.time_in_nanos("GET").unwrap())
        );
        println!(
            "{} SETs/sec",
            calc(max_queries, dt.time_in_nanos("SET").unwrap())
        );
        println!("===========================");
    }

    /// Returns the number of queries/sec
    fn calc(reqs: usize, time: u128) -> f64 {
        reqs as f64 / (time as f64 / 1_000_000_000 as f64)
    }
}

fn main() {
    benchtool::runner();
}
