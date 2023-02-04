/*
 * Created on Tue Aug 09 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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
    self::report::AggregateReport,
    crate::{
        config,
        config::{BenchmarkConfig, ServerConfig},
        error::{BResult, Error},
        util,
    },
    clap::ArgMatches,
    devtimer::SimpleTimer,
    libstress::utils::{generate_random_byte_vector, ran_bytes},
    skytable::{Connection, Element, Query, RespCode},
};

mod benches;
mod report;
mod validation;

macro_rules! binfo {
    ($($arg:tt)+) => {
        if $crate::config::should_output_messages() {
            ::log::info!($($arg)+)
        }
    };
}

/// The loop monitor can be used for maintaining a loop for a given benchmark
struct LoopMonitor<'a> {
    /// cleanup instructions
    inner: Option<CleanupInner<'a>>,
    /// maximum iterations
    max: usize,
    /// current iteration
    current: usize,
    /// total time
    time: u128,
    /// name of test
    name: &'static str,
}

impl<'a> LoopMonitor<'a> {
    /// Create a benchmark loop monitor that doesn't need any cleanup
    pub fn new(max: usize, name: &'static str) -> Self {
        Self {
            inner: None,
            max,
            current: 0,
            time: 0,
            name,
        }
    }
    /// Create a new benchmark loop monitor that uses the given cleanup instructions:
    /// - `max`: Total iterations
    /// - `name`: Name of benchmark
    /// - `connection`: A connection to use for cleanup instructions
    /// - `query`: Query to run for cleanup
    /// - `response`: Response expected when cleaned up
    /// - `skip_on_last`: Skip running the cleanup instructions on the last loop
    pub fn new_cleanup(
        max: usize,
        name: &'static str,
        connection: &'a mut Connection,
        query: Query,
        response: Element,
        skip_on_last: bool,
    ) -> Self {
        Self {
            inner: Some(CleanupInner::new(query, response, connection, skip_on_last)),
            max,
            current: 0,
            time: 0,
            name,
        }
    }
    /// Run cleanup
    fn cleanup(&mut self) -> BResult<()> {
        let last_iter = self.is_last_iter();
        if let Some(ref mut cleanup) = self.inner {
            let should_run_cleanup = !(last_iter && cleanup.skip_on_last);
            if should_run_cleanup {
                return cleanup.cleanup(self.name);
            }
        }
        Ok(())
    }
    /// Check if this is the last iteration
    fn is_last_iter(&self) -> bool {
        (self.max - 1) == self.current
    }
    /// Step the counter ahead
    fn step(&mut self) {
        self.current += 1;
    }
    /// Determine if we should continue executing
    fn should_continue(&self) -> bool {
        self.current < self.max
    }
    /// Append a new time to the sum
    fn incr_time(&mut self, dt: &SimpleTimer) {
        self.time += dt.time_in_nanos().unwrap();
    }
    /// Return the sum
    fn sum(&self) -> u128 {
        self.time
    }
    /// Return the name of the benchmark
    fn name(&self) -> &'static str {
        self.name
    }
}

/// Cleanup instructions
struct CleanupInner<'a> {
    /// the connection to use for cleanup processes
    connection: &'a mut Connection,
    /// the query to be run
    query: Query,
    /// the response to expect
    response: Element,
    /// whether we should skip on the last loop
    skip_on_last: bool,
}

impl<'a> CleanupInner<'a> {
    /// Init cleanup instructions
    fn new(q: Query, r: Element, connection: &'a mut Connection, skip_on_last: bool) -> Self {
        Self {
            query: q,
            response: r,
            connection,
            skip_on_last,
        }
    }
    /// Run cleanup
    fn cleanup(&mut self, name: &'static str) -> BResult<()> {
        let r: Element = self.connection.run_query(&self.query)?;
        if r.ne(&self.response) {
            Err(Error::Runtime(format!(
                "Failed to run cleanup for benchmark `{}`",
                name
            )))
        } else {
            Ok(())
        }
    }
}

#[inline(always)]
/// Returns a vec with the given cap, ensuring that we don't overflow memory
fn vec_with_cap<T>(cap: usize) -> BResult<Vec<T>> {
    let mut v = Vec::new();
    v.try_reserve_exact(cap)?;
    Ok(v)
}

/// Run the actual benchmarks
pub fn run_bench(servercfg: &ServerConfig, matches: ArgMatches) -> BResult<()> {
    // init bench config
    let bench_config = BenchmarkConfig::new(servercfg, matches)?;
    // check if we have enough combinations for the given query count and key size
    if !util::has_enough_ncr(bench_config.kvsize(), bench_config.query_count()) {
        return Err(Error::Runtime(
            "too low sample space for given query count. use larger kvsize".into(),
        ));
    }
    // run sanity test; this will also set up the temporary table for benchmarking
    binfo!("Running sanity test ...");
    util::run_sanity_test(&bench_config.server)?;

    // pool pre-exec setup
    let servercfg = servercfg.clone();
    let switch_table = Query::from("use default.tmpbench").into_raw_query();

    // init pool config; side_connection is for cleanups
    let mut misc_connection = Connection::new(servercfg.host(), servercfg.port())?;

    // init timer and reports
    let mut reports = AggregateReport::new(bench_config.query_count());

    // init test data
    binfo!("Initializing test data ...");
    let mut rng = rand::thread_rng();
    let keys = generate_random_byte_vector(
        bench_config.query_count(),
        bench_config.kvsize(),
        &mut rng,
        true,
    )?;
    let values = generate_random_byte_vector(
        bench_config.query_count(),
        bench_config.kvsize(),
        &mut rng,
        false,
    )?;
    let new_updated_key = ran_bytes(bench_config.kvsize(), &mut rng);

    // run tests; the idea here is to run all tests one-by-one instead of generating all packets at once
    // such an approach helps us keep memory usage low
    // bench set
    binfo!("Benchmarking SET ...");
    benches::bench_set(
        &keys,
        &values,
        &mut misc_connection,
        &bench_config,
        &switch_table,
        &mut reports,
    )?;

    // bench update
    binfo!("Benchmarking UPDATE ...");
    benches::bench_update(
        &keys,
        &new_updated_key,
        &bench_config,
        &switch_table,
        &mut reports,
    )?;

    // bench get
    binfo!("Benchmarking GET ...");
    benches::bench_get(&keys, &bench_config, &switch_table, &mut reports)?;

    // remove all test data
    binfo!("Finished benchmarks. Cleaning up ...");
    let r: Element = misc_connection.run_query(Query::from("drop model default.tmpbench force"))?;
    if r != Element::RespCode(RespCode::Okay) {
        return Err(Error::Runtime("failed to clean up after benchmarks".into()));
    }

    if config::should_output_messages() {
        // normal output
        println!("===========RESULTS===========");
        let (maxpad, reports) = reports.finish();
        for report in reports {
            let padding = " ".repeat(maxpad - report.name().len());
            println!(
                "{}{} {:.6}/sec",
                report.name().to_uppercase(),
                padding,
                report.stat(),
            );
        }
        println!("=============================");
    } else {
        // JSON
        println!("{}", reports.into_json())
    }
    Ok(())
}
