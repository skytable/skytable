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

use {
    crate::{
        args::BenchConfig,
        error::{self, BenchResult},
        runtime::{BombardPool, RuntimeStats, ThreadedBombardTask},
    },
    skytable::{error::Error, query, response::Response, Config, Connection, Query},
    std::{fmt, time::Instant},
};

/*
    task impl
*/

/// A bombard task used for benchmarking

#[derive(Debug)]
pub struct BombardTask {
    config: Config,
}

impl BombardTask {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone)]
pub enum BombardTaskKind {
    Insert(u8),
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct BombardTaskSpec {
    kind: BombardTaskKind,
    base_query: String,
    pk_len: usize,
}

impl BombardTaskSpec {
    pub fn insert(base_query: String, pk_len: usize, second_column: u8) -> Self {
        Self {
            kind: BombardTaskKind::Insert(second_column),
            base_query,
            pk_len,
        }
    }
    pub fn update(base_query: String, pk_len: usize) -> Self {
        Self {
            kind: BombardTaskKind::Update,
            base_query,
            pk_len,
        }
    }
    pub fn delete(base_query: String, pk_len: usize) -> Self {
        Self {
            kind: BombardTaskKind::Delete,
            base_query,
            pk_len,
        }
    }
    fn generate(&self, current: u64) -> (Query, Response) {
        let mut q = query!(&self.base_query);
        let resp = match self.kind {
            BombardTaskKind::Insert(second_column) => {
                q.push_param(format!("{:0>width$}", current, width = self.pk_len));
                q.push_param(second_column);
                Response::Empty
            }
            BombardTaskKind::Update => {
                q.push_param(1u64);
                q.push_param(format!("{:0>width$}", current, width = self.pk_len));
                Response::Empty
            }
            BombardTaskKind::Delete => {
                q.push_param(format!("{:0>width$}", current, width = self.pk_len));
                Response::Empty
            }
        };
        (q, resp)
    }
}

/// Errors while running a bombard
#[derive(Debug)]
pub enum BombardTaskError {
    DbError(Error),
    Mismatch,
}

impl fmt::Display for BombardTaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DbError(e) => write!(f, "a bombard subtask failed with {e}"),
            Self::Mismatch => write!(f, "got unexpected response for bombard subtask"),
        }
    }
}

impl From<Error> for BombardTaskError {
    fn from(dbe: Error) -> Self {
        Self::DbError(dbe)
    }
}

impl ThreadedBombardTask for BombardTask {
    type Worker = Connection;
    type WorkerTask = (Query, Response);
    type WorkerTaskSpec = BombardTaskSpec;
    type WorkerInitError = Error;
    type WorkerTaskError = BombardTaskError;
    fn worker_init(&self) -> Result<Self::Worker, Self::WorkerInitError> {
        self.config.connect()
    }
    fn generate_task(spec: &Self::WorkerTaskSpec, current: u64) -> Self::WorkerTask {
        spec.generate(current)
    }
    fn worker_drive_timed(
        worker: &mut Self::Worker,
        (query, response): Self::WorkerTask,
    ) -> Result<u128, Self::WorkerTaskError> {
        let start = Instant::now();
        let ret = worker.query(&query)?;
        let stop = Instant::now();
        if ret == response {
            Ok(stop.duration_since(start).as_nanos())
        } else {
            Err(BombardTaskError::Mismatch)
        }
    }
}

/*
    runner
*/

pub fn run(bench: BenchConfig) -> error::BenchResult<()> {
    let bench_config = BombardTask::new(Config::new(
        &bench.host,
        bench.port,
        "root",
        &bench.root_pass,
    ));
    info!("running preliminary checks and creating model `bench.bench` with definition: `{{un: string, pw: uint8}}`");
    let mut main_thread_db = bench_config.config.connect()?;
    main_thread_db.query_parse::<()>(&query!("create space bench"))?;
    main_thread_db.query_parse::<()>(&query!("create model bench.bench(un: string, pw: uint8)"))?;
    let stats = match bench_internal(bench_config, bench) {
        Ok(stats) => stats,
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
    };
    warn!("benchmarks might appear to be slower. this tool is currently experimental");
    // print results
    print_table(stats);
    cleanup(main_thread_db)?;
    Ok(())
}

/*
    util
*/

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
    for (query, RuntimeStats { qps, head, tail }) in data {
        println!(
            "| {:<7} | {:>24.2} | {:>21} | {:>22} |",
            query, qps, tail, head
        );
    }
    println!(
        "+---------+--------------------------+-----------------------+------------------------+"
    );
}

/*
    bench runner
*/

fn bench_internal(
    config: BombardTask,
    bench: BenchConfig,
) -> BenchResult<Vec<(&'static str, RuntimeStats)>> {
    let mut ret = vec![];
    // initialize pool
    info!("initializing connection pool");
    let mut pool = BombardPool::new(bench.threads, config)?;
    // bench INSERT
    info!("benchmarking `INSERT`");
    let insert = BombardTaskSpec::insert("insert into bench.bench(?, ?)".into(), bench.key_size, 0);
    let insert_stats = pool.blocking_bombard(insert, bench.query_count)?;
    ret.push(("INSERT", insert_stats));
    // bench UPDATE
    info!("benchmarking `UPDATE`");
    let update = BombardTaskSpec::update(
        "update bench.bench set pw += ? where un = ?".into(),
        bench.key_size,
    );
    let update_stats = pool.blocking_bombard(update, bench.query_count)?;
    ret.push(("UPDATE", update_stats));
    // bench DELETE
    info!("benchmarking `DELETE`");
    let delete = BombardTaskSpec::delete(
        "delete from bench.bench where un = ?".into(),
        bench.key_size,
    );
    let delete_stats = pool.blocking_bombard(delete, bench.query_count)?;
    ret.push(("DELETE", delete_stats));
    info!("completed benchmarks. closing pool");
    drop(pool);
    Ok(ret)
}
