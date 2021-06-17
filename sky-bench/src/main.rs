/*
 * Created on Sun Sep 13 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

mod benchtool;
mod testkey;
mod util;
use crate::util::DEFAULT_PACKET_SIZE;
use crate::util::DEFAULT_QUERY_COUNT;
use crate::util::DEFAULT_WORKER_COUNT;
// external imports
use clap::{load_yaml, App};
use core::hint::unreachable_unchecked;

fn main() {
    let cfg_layout = load_yaml!("./cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let host = match matches.value_of("host") {
        Some(h) => h.to_owned(),
        None => "127.0.0.1".to_owned(),
    };
    let port = match matches.value_of("port") {
        Some(p) => match p.parse::<u16>() {
            Ok(p) => p,
            Err(_) => err!("Invalid Port"),
        },
        None => 2003,
    };
    let json_out = matches.is_present("json");
    let max_connections = match matches.value_of("connections").map(|v| v.parse::<usize>()) {
        Some(Ok(con)) => con,
        None => DEFAULT_WORKER_COUNT,
        _ => err!("Bad value for maximum connections"),
    };
    let max_queries = match matches.value_of("queries").map(|v| v.parse::<usize>()) {
        Some(Ok(qr)) => qr,
        None => DEFAULT_QUERY_COUNT,
        _ => err!("Bad value for max queries"),
    };
    let packet_size = match matches.value_of("size").map(|v| v.parse::<usize>()) {
        Some(Ok(size)) => size,
        None => DEFAULT_PACKET_SIZE,
        _ => err!("Bad value for key/value size"),
    };
    if let Some(cmd) = matches.subcommand_matches("testkey") {
        let count = match cmd.value_of_lossy("count") {
            Some(cnt) => match cnt.to_string().parse::<usize>() {
                Ok(cnt) => cnt,
                Err(_) => err!("Bad value for testkey count"),
            },
            None => unsafe {
                // UNSAFE(@ohsayan): This is completely safe because clap takes care that
                // the count argument is supplied
                unreachable_unchecked();
            },
        };
        println!("warning: Ignoring any other invalid flags/options (if they were supplied)");
        testkey::create_testkeys(&host, port, count, max_connections, packet_size);
    } else {
        benchtool::runner(
            host,
            port,
            max_connections,
            max_queries,
            packet_size,
            json_out,
        );
    }
}
