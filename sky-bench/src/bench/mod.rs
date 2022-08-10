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
    self::report::{AggregateReport, SingleReport},
    crate::{
        config,
        config::{BenchmarkConfig, ServerConfig},
        error::{BResult, Error},
        util,
    },
    clap::ArgMatches,
    devtimer::SimpleTimer,
    libstress::{
        utils::{generate_random_byte_vector, ran_bytes},
        PoolConfig,
    },
    skytable::{types::RawString, Connection, Element, Query, RespCode},
    std::{
        io::{Read, Write},
        net::TcpStream,
    },
};

mod report;
mod validation;

macro_rules! binfo {
    ($($arg:tt)+) => {
        if $crate::config::should_output_messages() {
            ::log::info!($($arg)+)
        }
    };
}

pub fn run_bench(servercfg: &ServerConfig, matches: ArgMatches) -> BResult<()> {
    // init bench config
    let bench_config = BenchmarkConfig::new(servercfg, matches)?;
    // check if we have enough combinations for the given query count and key size
    if !util::has_enough_ncr(bench_config.kvsize(), bench_config.query_count()) {
        return Err(Error::RuntimeError(
            "too low sample space for given query count. use larger kvsize".into(),
        ));
    }
    // run sanity test; this will also set up the temporary table for benchmarking
    binfo!("Running sanity test ...");
    util::run_sanity_test(&bench_config.server)?;

    // pool pre-exec setup
    let servercfg = servercfg.clone();
    let switch_table = Query::from("use default.tmpbench").into_raw_query();
    let get_response_size = validation::calculate_response_size(bench_config.kvsize());
    let rcode_okay_size = validation::RESPCODE_OKAY.len();

    // init pool config; side_connection is for cleanups
    let mut misc_connection = Connection::new(servercfg.host(), servercfg.port())?;
    let pool_config = PoolConfig::new(
        servercfg.connections(),
        move || {
            let mut stream = TcpStream::connect((servercfg.host(), servercfg.port())).unwrap();
            stream.write_all(&switch_table.clone()).unwrap();
            let mut v = vec![0; rcode_okay_size];
            let _ = stream.read_exact(&mut v).unwrap();
            stream
        },
        move |_sock, _packet: Box<[u8]>| panic!("on_loop exec unset"),
        |socket| {
            socket.shutdown(std::net::Shutdown::Both).unwrap();
        },
        true,
        Some(bench_config.query_count()),
    );

    // init timer and reports
    let mut report = AggregateReport::new(bench_config.query_count());

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
    let set_packets: Vec<Box<[u8]>> = (0..bench_config.query_count())
        .map(|i| {
            Query::from("SET")
                .arg(RawString::from(keys[i].clone()))
                .arg(RawString::from(values[i].clone()))
                .into_raw_query()
                .into_boxed_slice()
        })
        .collect();
    run_bench_for(
        &pool_config,
        move |sock, packet: Box<[u8]>| {
            sock.write_all(&packet).unwrap();
            // expect rcode 0
            let mut v = vec![0; rcode_okay_size];
            let _ = sock.read_exact(&mut v).unwrap();
            assert_eq!(v, validation::RESPCODE_OKAY);
        },
        "set",
        &mut report,
        set_packets,
        bench_config.runs(),
        &mut misc_connection,
        Some((
            Query::from("FLUSHDB").arg("default.tmpbench"),
            Element::RespCode(RespCode::Okay),
            true,
        )),
    )?;

    // bench update
    binfo!("Benchmarking UPDATE ...");
    let update_packets: Vec<Box<[u8]>> = (0..bench_config.query_count())
        .map(|i| {
            Query::from("UPDATE")
                .arg(RawString::from(keys[i].clone()))
                .arg(RawString::from(new_updated_key.clone()))
                .into_raw_query()
                .into_boxed_slice()
        })
        .collect();
    run_bench_for(
        &pool_config,
        move |sock, packet: Box<[u8]>| {
            sock.write_all(&packet).unwrap();
            // expect rcode 0
            let mut v = vec![0; rcode_okay_size];
            let _ = sock.read_exact(&mut v).unwrap();
            assert_eq!(v, validation::RESPCODE_OKAY);
        },
        "update",
        &mut report,
        update_packets,
        bench_config.runs(),
        &mut misc_connection,
        None,
    )?;

    // bench get
    binfo!("Benchmarking GET ...");
    let get_packets: Vec<Box<[u8]>> = (0..bench_config.query_count())
        .map(|i| {
            Query::from("GET")
                .arg(RawString::from(keys[i].clone()))
                .into_raw_query()
                .into_boxed_slice()
        })
        .collect();
    run_bench_for(
        &pool_config,
        move |sock, packet: Box<[u8]>| {
            sock.write_all(&packet).unwrap();
            // expect kvsize byte count
            let mut v = vec![0; get_response_size];
            let _ = sock.read_exact(&mut v).unwrap();
        },
        "get",
        &mut report,
        get_packets,
        bench_config.runs(),
        &mut misc_connection,
        None,
    )?;

    // remove all test data
    binfo!("Finished benchmarks. Cleaning up ...");
    let r: Element = misc_connection.run_query(Query::from("drop model default.tmpbench force"))?;
    if r != Element::RespCode(RespCode::Okay) {
        return Err(Error::RuntimeError(
            "failed to clean up after benchmarks".into(),
        ));
    }

    if config::should_output_messages() {
        // normal output
        println!("===========RESULTS===========");
        let (maxpad, reports) = report.finish();
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
        println!("{}", report.into_json())
    }
    Ok(())
}

fn run_bench_for<F, Inp, UIn, Lv, Lp, Ex>(
    pool: &PoolConfig<Inp, UIn, Lv, Lp, Ex>,
    closure: F,
    name: &'static str,
    reports: &mut AggregateReport,
    input: Vec<UIn>,
    runs: usize,
    tmp_con: &mut Connection,
    cleanup: Option<(Query, Element, bool)>,
) -> BResult<()>
where
    F: Send + Sync + Fn(&mut Inp, UIn) + Clone + 'static,
    Ex: Clone + Fn(&mut Inp) + Send + Sync + 'static,
    Inp: Sync + 'static,
    Lp: Clone + Fn(&mut Inp, UIn) + Send + Sync + 'static,
    Lv: Clone + Fn() -> Inp + Send + 'static + Sync,
    UIn: Clone + Send + Sync + 'static,
{
    let mut sum: u128 = 0;
    for i in 0..runs {
        // run local copy
        let this_input = input.clone();
        let pool = pool.with_loop_closure(closure.clone());
        // time
        let mut tm = SimpleTimer::new();
        tm.start();
        pool.execute_and_finish_iter(this_input);
        tm.stop();
        sum += tm.time_in_nanos().unwrap();
        // cleanup
        if let Some((ref cleanup_after_run, ref resp_cleanup_after_run, skip_on_last)) = cleanup {
            if !(skip_on_last && (i == runs - 1)) {
                let r: Element = tmp_con.run_query(cleanup_after_run)?;
                if r.ne(resp_cleanup_after_run) {
                    return Err(Error::RuntimeError(format!(
                        "Failed to run cleanup for benchmark `{name}` in iteration {i}"
                    )));
                }
            }
        }
    }
    // return average time
    reports.push(SingleReport::new(name, sum as f64 / runs as f64));
    Ok(())
}
