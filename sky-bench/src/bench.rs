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

use crate::args::BenchEngine;

use {
    crate::{
        args::BenchConfig,
        error::{self, BenchResult},
        runtime::{fury, rookie, RuntimeStats},
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
    pub fn generate(&self, current: u64) -> (Query, Response) {
        let mut q = query!(&self.base_query);
        let resp = match self.kind {
            BombardTaskKind::Insert(second_column) => {
                self.push_pk(&mut q, current);
                q.push_param(second_column);
                Response::Empty
            }
            BombardTaskKind::Update => {
                q.push_param(1u64);
                self.push_pk(&mut q, current);
                Response::Empty
            }
            BombardTaskKind::Delete => {
                self.push_pk(&mut q, current);
                Response::Empty
            }
        };
        (q, resp)
    }
    fn push_pk(&self, q: &mut Query, current: u64) {
        q.push_param(self.get_primary_key(current));
    }
    fn get_primary_key(&self, current: u64) -> String {
        format!("{:0>width$}", current, width = self.pk_len)
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

impl rookie::ThreadedBombardTask for BombardTask {
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
    let stats = match bench.engine {
        BenchEngine::Rookie => bench_rookie(bench_config, bench),
        BenchEngine::Fury => bench_fury(bench),
    };
    let (total_queries, stats) = match stats {
        Ok(ret) => ret,
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
    info!(
        "{} queries executed. benchmark complete.",
        fmt_u64(total_queries)
    );
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

struct BenchItem {
    name: &'static str,
    spec: BombardTaskSpec,
    count: usize,
}

impl BenchItem {
    fn new(name: &'static str, spec: BombardTaskSpec, count: usize) -> Self {
        Self { name, spec, count }
    }
    fn print_log_start(&self) {
        info!(
            "benchmarking `{}`. average payload size = {} bytes. queries = {}",
            self.name,
            self.spec.generate(0).0.debug_encode_packet().len(),
            self.count
        )
    }
    fn run(self, pool: &mut rookie::BombardPool<BombardTask>) -> BenchResult<RuntimeStats> {
        pool.blocking_bombard(self.spec, self.count)
            .map_err(From::from)
    }
    async fn run_async(self, pool: &mut fury::Fury) -> BenchResult<RuntimeStats> {
        pool.bombard(self.count, self.spec)
            .await
            .map_err(From::from)
    }
}

fn prepare_bench_spec(bench: &BenchConfig) -> Vec<BenchItem> {
    vec![
        BenchItem::new(
            "INSERT",
            BombardTaskSpec::insert("insert into bench.bench(?, ?)".into(), bench.key_size, 0),
            bench.query_count,
        ),
        BenchItem::new(
            "UPDATE",
            BombardTaskSpec::update(
                "update bench.bench set pw += ? where un = ?".into(),
                bench.key_size,
            ),
            bench.query_count,
        ),
        BenchItem::new(
            "DELETE",
            BombardTaskSpec::delete(
                "delete from bench.bench where un = ?".into(),
                bench.key_size,
            ),
            bench.query_count,
        ),
    ]
}

fn fmt_u64(n: u64) -> String {
    let num_str = n.to_string();
    let mut result = String::new();
    let chars_rev: Vec<_> = num_str.chars().rev().collect();
    for (i, ch) in chars_rev.iter().enumerate() {
        if i % 3 == 0 && i != 0 {
            result.push(',');
        }
        result.push(*ch);
    }
    result.chars().rev().collect()
}

fn bench_rookie(
    task: BombardTask,
    bench: BenchConfig,
) -> BenchResult<(u64, Vec<(&'static str, RuntimeStats)>)> {
    // initialize pool
    info!(
        "initializing connections. engine=rookie, threads={}, primary key size ={} bytes",
        bench.threads, bench.key_size
    );
    let mut pool = rookie::BombardPool::new(bench.threads, task)?;
    // prepare benches
    let benches = prepare_bench_spec(&bench);
    // bench
    let total_queries = bench.query_count as u64 * benches.len() as u64;
    let mut results = vec![];
    for task in benches {
        let name = task.name;
        task.print_log_start();
        let this_result = task.run(&mut pool)?;
        results.push((name, this_result));
    }
    Ok((total_queries, results))
}

fn bench_fury(bench: BenchConfig) -> BenchResult<(u64, Vec<(&'static str, RuntimeStats)>)> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(bench.threads)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        info!(
            "initializing connections. engine=fury, threads={}, connections={}, primary key size ={} bytes",
            bench.threads, bench.connections, bench.key_size
        );
        let mut pool = fury::Fury::new(
            bench.connections,
            Config::new(&bench.host, bench.port, "root", &bench.root_pass),
        )
        .await?;
        // prepare benches
        let benches = prepare_bench_spec(&bench);
        // bench
        let total_queries = bench.query_count as u64 * benches.len() as u64;
        let mut results = vec![];
        for task in benches {
            let name = task.name;
            task.print_log_start();
            let this_result = task.run_async(&mut pool).await?;
            results.push((name, this_result));
        }
        Ok((total_queries,results))
    })
}
