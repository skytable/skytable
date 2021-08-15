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
use std::error::Error;

pub const DEFAULT_WORKER_COUNT: usize = 10;
pub const DEFAULT_PACKET_SIZE: usize = 8;
pub const DEFAULT_QUERY_COUNT: usize = 100_000;
pub const DEFAULT_REPEAT: usize = 10;

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
    ($host:expr, $port:expr) => {{
        // Run a sanity test
        if let Err(e) = crate::util::run_sanity_test(&$host, $port) {
            Err(e)
        } else {
            Ok(())
        }
    }};
}

#[macro_export]
macro_rules! err {
    ($note:expr) => {{
        eprintln!("ERROR: {}", $note);
        std::process::exit(0x01);
    }};
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
        .eq(&Response::Item(Element::Str("HEY!".to_owned())))
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
        .eq(&Response::Item(Element::Str(value)))
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
