/*
 * Created on Sat Nov 18 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

use crate::error::BenchResult;

use {
    crate::{
        args::BenchConfig,
        error::{self, BenchmarkTaskWorkerError},
        pool::{RuntimeStats, Taskpool, ThreadedTask},
    },
    skytable::{query, response::Response, Config, Connection, Query},
    std::time::Instant,
};

#[derive(Debug)]
pub struct BenchmarkTask {
    cfg: Config,
}

impl BenchmarkTask {
    pub fn new(host: &str, port: u16, username: &str, password: &str) -> Self {
        Self {
            cfg: Config::new(host, port, username, password),
        }
    }
}

impl ThreadedTask for BenchmarkTask {
    type TaskWorker = Connection;
    type TaskWorkerInitError = BenchmarkTaskWorkerError;
    type TaskWorkerTerminateError = BenchmarkTaskWorkerError;
    type TaskWorkerWorkError = BenchmarkTaskWorkerError;
    type TaskInput = (Query, Response);
    fn initialize_worker(&self) -> Result<Self::TaskWorker, Self::TaskWorkerInitError> {
        self.cfg.connect().map_err(Into::into)
    }
    fn drive_worker_timed(
        worker: &mut Self::TaskWorker,
        (query, expected_resp): Self::TaskInput,
    ) -> Result<(Instant, Instant), Self::TaskWorkerWorkError> {
        let start = Instant::now();
        let resp = worker.query(&query)?;
        let stop = Instant::now();
        if resp == expected_resp {
            Ok((start, stop))
        } else {
            Err(BenchmarkTaskWorkerError::Error(format!(
                "response from server did not match expected response: {:?}",
                resp
            )))
        }
    }
    fn terminate_worker(
        &self,
        _: &mut Self::TaskWorker,
    ) -> Result<(), Self::TaskWorkerTerminateError> {
        Ok(())
    }
}

pub fn run(bench: BenchConfig) -> error::BenchResult<()> {
    let bench_config = BenchmarkTask::new(&bench.host, bench.port, "root", &bench.root_pass);
    info!("running preliminary checks and creating model `bench.bench` with definition: `{{un: string, pw: uint8}}`");
    let mut main_thread_db = bench_config.cfg.connect()?;
    main_thread_db.query_parse::<()>(&query!("create space bench"))?;
    main_thread_db.query_parse::<()>(&query!("create model bench.bench(un: string, pw: uint8)"))?;
    info!(
        "initializing connection pool with {} connections",
        bench.threads
    );
    let mut p = Taskpool::new(bench.threads, bench_config)?;
    info!(
        "pool initialized successfully. preparing {} `INSERT` queries with primary key size={} bytes",
        bench.query_count, bench.key_size
    );
    let mut insert_stats = Default::default();
    let mut update_stats = Default::default();
    let mut delete_stats = Default::default();
    match || -> BenchResult<()> {
        // bench insert
        let insert_queries: Vec<(Query, Response)> = (0..bench.query_count)
            .into_iter()
            .map(|i| {
                (
                    query!(
                        "insert into bench.bench(?, ?)",
                        format!("{:0>width$}", i, width = bench.key_size),
                        0u64
                    ),
                    Response::Empty,
                )
            })
            .collect();
        info!("benchmarking `INSERT` queries");
        insert_stats = p.blocking_bombard(insert_queries)?;
        // bench update
        info!("completed benchmarking `INSERT`. preparing `UPDATE` queries");
        let update_queries: Vec<(Query, Response)> = (0..bench.query_count)
            .into_iter()
            .map(|i| {
                (
                    query!(
                        "update bench.bench set pw += ? where un = ?",
                        1u64,
                        format!("{:0>width$}", i, width = bench.key_size),
                    ),
                    Response::Empty,
                )
            })
            .collect();
        info!("benchmarking `UPDATE` queries");
        update_stats = p.blocking_bombard(update_queries)?;
        // bench delete
        info!("completed benchmarking `UPDATE`. preparing `DELETE` queries");
        let delete_queries: Vec<(Query, Response)> = (0..bench.query_count)
            .into_iter()
            .map(|i| {
                (
                    query!(
                        "delete from bench.bench where un = ?",
                        format!("{:0>width$}", i, width = bench.key_size),
                    ),
                    Response::Empty,
                )
            })
            .collect();
        info!("benchmarking `DELETE` queries");
        delete_stats = p.blocking_bombard(delete_queries)?;
        info!("completed benchmarking `DELETE` queries");
        Ok(())
    }() {
        Ok(()) => {}
        Err(e) => {
            error!("benchmarking failed. attempting to clean up");
            match cleanup(main_thread_db) {
                Ok(()) => return Err(e),
                Err(e_cleanup) => {
                    error!("failed to clean up db: {e_cleanup}. please remove model `bench.bench` manually");
                    return Err(e);
                }
            }
        }
    }
    drop(p);
    warn!("benchmarks might appear to be slower. this tool is currently experimental");
    // print results
    info!("results:");
    print_table(vec![
        ("INSERT", insert_stats),
        ("UPDATE", update_stats),
        ("DELETE", delete_stats),
    ]);
    cleanup(main_thread_db)?;
    Ok(())
}

fn cleanup(mut main_thread_db: Connection) -> Result<(), error::BenchError> {
    trace!("dropping space and table");
    main_thread_db.query_parse::<()>(&query!("drop model bench.bench"))?;
    main_thread_db.query_parse::<()>(&query!("drop space bench"))?;
    Ok(())
}

fn print_table(data: Vec<(&'static str, RuntimeStats)>) {
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
    println!(
        "| Query   | Effective real-world QPS | Slowest Query (nanos) | Fastest Query (nanos)  |"
    );
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
    for (
        query,
        RuntimeStats {
            qps,
            avg_per_query_ns: _,
            head_ns,
            tail_ns,
        },
    ) in data
    {
        println!(
            "| {:<7} | {:>24.2} | {:>21} | {:>22} |",
            query, qps, tail_ns, head_ns
        );
    }
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
}
