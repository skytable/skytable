/*
 * Created on Sat Aug 13 2022
 *
 * This file is a part of S{
    let ref this = loopmon;
    this.current
}le (formerly known as TerrabaseDB or Skybase) is a free and open-source
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
    super::{
        report::{AggregateReport, SingleReport},
        validation, vec_with_cap, BenchmarkConfig, LoopMonitor,
    },
    crate::error::BResult,
    devtimer::SimpleTimer,
    libstress::Workpool,
    skytable::{types::RawString, Connection, Element, Query, RespCode},
    std::{
        io::{Read, Write},
        net::{Shutdown, TcpStream},
    },
};

/// Run a benchmark using the given pre-loop, in-loop and post-loop closures
fn run_bench_custom<Inp, Lp, Lv, Ex>(
    bench_config: BenchmarkConfig,
    packets: Vec<Box<[u8]>>,
    on_init: Lv,
    on_loop: Lp,
    on_loop_exit: Ex,
    loopmon: LoopMonitor,
    reports: &mut AggregateReport,
) -> BResult<()>
where
    Ex: Clone + Fn(&mut Inp) + Send + Sync + 'static,
    Inp: Sync + 'static,
    Lp: Clone + Fn(&mut Inp, Box<[u8]>) + Send + Sync + 'static,
    Lv: Clone + Fn() -> Inp + Send + 'static + Sync,
{
    // now do our runs
    let mut loopmon = loopmon;

    while loopmon.should_continue() {
        // now create our connection pool
        let pool = Workpool::new(
            bench_config.server.connections(),
            on_init.clone(),
            on_loop.clone(),
            on_loop_exit.clone(),
            true,
            Some(bench_config.query_count()),
        )?;

        // get our local copy
        let this_packets = packets.clone();

        // run and time our operations
        let mut dt = SimpleTimer::new();
        dt.start();
        pool.execute_and_finish_iter(this_packets);
        dt.stop();
        loopmon.incr_time(&dt);

        // cleanup
        loopmon.cleanup()?;
        loopmon.step();
    }

    // save time
    reports.push(SingleReport::new(
        loopmon.name(),
        loopmon.sum() as f64 / bench_config.runs() as f64,
    ));
    Ok(())
}

#[inline(always)]
/// Init connection and buffer
fn init_connection_and_buf(
    host: &str,
    port: u16,
    start_command: Vec<u8>,
    bufsize: usize,
) -> (TcpStream, Vec<u8>) {
    let mut con = TcpStream::connect((host, port)).unwrap();
    con.write_all(&start_command).unwrap();
    let mut ret = [0u8; validation::RESPCODE_OKAY.len()];
    con.read_exact(&mut ret).unwrap();
    let readbuf = vec![0; bufsize];
    (con, readbuf)
}

/// Benchmark SET
pub fn bench_set(
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    connection: &mut Connection,
    bench_config: &BenchmarkConfig,
    create_table: &[u8],
    reports: &mut AggregateReport,
) -> BResult<()> {
    let bench_config = bench_config.clone();
    let create_table = create_table.to_owned();
    let loopmon = LoopMonitor::new_cleanup(
        bench_config.runs(),
        "set",
        connection,
        Query::from("FLUSHDB").arg("default.tmpbench"),
        Element::RespCode(RespCode::Okay),
        true,
    );
    let mut packets = vec_with_cap(bench_config.query_count())?;
    (0..bench_config.query_count()).for_each(|i| {
        packets.push(
            Query::from("SET")
                .arg(RawString::from(keys[i].to_owned()))
                .arg(RawString::from(values[i].to_owned()))
                .into_raw_query()
                .into_boxed_slice(),
        )
    });
    run_bench_custom(
        bench_config.clone(),
        packets,
        move || {
            init_connection_and_buf(
                bench_config.server.host(),
                bench_config.server.port(),
                create_table.to_owned(),
                validation::RESPCODE_OKAY.len(),
            )
        },
        |(con, buf), packet| {
            con.write_all(&packet).unwrap();
            con.read_exact(buf).unwrap();
            assert_eq!(buf, validation::RESPCODE_OKAY);
        },
        |(con, _)| con.shutdown(Shutdown::Both).unwrap(),
        loopmon,
        reports,
    )
}

/// Benchmark UPDATE
pub fn bench_update(
    keys: &[Vec<u8>],
    new_value: &[u8],
    bench_config: &BenchmarkConfig,
    create_table: &[u8],
    reports: &mut AggregateReport,
) -> BResult<()> {
    let bench_config = bench_config.clone();
    let create_table = create_table.to_owned();
    let loopmon = LoopMonitor::new(bench_config.runs(), "update");
    let mut packets = vec_with_cap(bench_config.query_count())?;
    (0..bench_config.query_count()).for_each(|i| {
        packets.push(
            Query::from("update")
                .arg(RawString::from(keys[i].clone()))
                .arg(RawString::from(new_value.to_owned()))
                .into_raw_query()
                .into_boxed_slice(),
        )
    });
    run_bench_custom(
        bench_config.clone(),
        packets,
        move || {
            init_connection_and_buf(
                bench_config.server.host(),
                bench_config.server.port(),
                create_table.to_owned(),
                validation::RESPCODE_OKAY.len(),
            )
        },
        |(con, buf), packet| {
            con.write_all(&packet).unwrap();
            con.read_exact(buf).unwrap();
            assert_eq!(buf, validation::RESPCODE_OKAY);
        },
        |(con, _)| con.shutdown(Shutdown::Both).unwrap(),
        loopmon,
        reports,
    )
}

/// Benchmark GET
pub fn bench_get(
    keys: &[Vec<u8>],
    bench_config: &BenchmarkConfig,
    create_table: &[u8],
    reports: &mut AggregateReport,
) -> BResult<()> {
    let bench_config = bench_config.clone();
    let create_table = create_table.to_owned();
    let loopmon = LoopMonitor::new(bench_config.runs(), "get");
    let mut packets = vec_with_cap(bench_config.query_count())?;
    (0..bench_config.query_count()).for_each(|i| {
        packets.push(
            Query::from("get")
                .arg(RawString::from(keys[i].clone()))
                .into_raw_query()
                .into_boxed_slice(),
        )
    });
    run_bench_custom(
        bench_config.clone(),
        packets,
        move || {
            init_connection_and_buf(
                bench_config.server.host(),
                bench_config.server.port(),
                create_table.to_owned(),
                validation::calculate_response_size(bench_config.kvsize()),
            )
        },
        |(con, buf), packet| {
            con.write_all(&packet).unwrap();
            con.read_exact(buf).unwrap();
        },
        |(con, _)| con.shutdown(Shutdown::Both).unwrap(),
        loopmon,
        reports,
    )
}
