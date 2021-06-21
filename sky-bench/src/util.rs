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

use libstress::utils::ran_string;
use rand::thread_rng;
use serde::Serialize;
use std::error::Error;

pub const DEFAULT_WORKER_COUNT: usize = 10;
pub const DEFAULT_PACKET_SIZE: usize = 8;
pub const DEFAULT_QUERY_COUNT: usize = 100_000;

#[macro_export]
macro_rules! hoststr {
    ($host:expr, $port:expr) => {{
        let mut hst: String = $host.to_string();
        hst.push(':');
        hst.push_str(&$port.to_string());
        hst
    }};
}

#[macro_export]
macro_rules! sanity_test {
    ($host:expr, $port:expr) => {
        println!("Running a sanity test...");
        // Run a sanity test
        if let Err(e) = crate::util::run_sanity_test(&$host, $port) {
            eprintln!("ERROR: Sanity test failed: {}", e);
            return;
        }
        println!("Sanity test succeeded");
    };
}

#[macro_export]
macro_rules! err {
    ($note:expr) => {{
        eprintln!("{}", $note);
        std::process::exit(0x100);
    }};
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

/// Returns the number of queries/sec
pub fn calc(reqs: usize, time: u128) -> f64 {
    reqs as f64 / (time as f64 / 1_000_000_000_f64)
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
pub fn run_sanity_test(host: &str, port: u16) -> Result<(), Box<dyn Error>> {
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
