/*
 * Created on Tue Aug 17 2021
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

#![allow(clippy::unit_arg)]

mod cli;

use {
    crate::cli::Cli,
    clap::Parser,
    env_logger::Builder,
    log::{error as err, info},
    skytable::{query, sync::Connection, Element, Query, RespCode},
    std::{collections::HashMap, env, fs, process},
};

type Bytes = Vec<u8>;

fn main() {
    // first evaluate config
    let cli = Cli::parse();
    Builder::new()
        .parse_filters(&env::var("SKY_LOG").unwrap_or_else(|_| "info".to_owned()))
        .init();
    let serial = cli.serial;
    let hostsplit: Vec<&str> = cli.new.split(':').collect();
    if hostsplit.len() != 2 {
        err(err!("Bad value for --new"));
    }
    let (host, port) = unsafe { (hostsplit.get_unchecked(0), hostsplit.get_unchecked(1)) };
    let port = match port.parse() {
        Ok(p) => p,
        Err(e) => err(err!("Bad value for port in --new: {}", e)),
    };
    let mut old_dir = cli.prevdir;
    old_dir.push_str("data.bin");
    // now connect
    let mut con = match Connection::new(host, port) {
        Ok(con) => con,
        Err(e) => err(err!("Failed to connect to new instance with error: {}", e)),
    };
    // run sanity test
    let q = query!("HEYA");
    match con.run_query_raw(&q) {
        Ok(Element::String(s)) if s.eq("HEY!") => {}
        Ok(_) => err(err!("Unknown response from server")),
        Err(e) => err(err!(
            "An I/O error occurred while running sanity test: {}",
            e
        )),
    }
    info!("Sanity test complete");

    // now de old file
    let read = match fs::read(old_dir) {
        Ok(r) => r,
        Err(e) => err(err!(
            "Failed to read data.bin file from old directory: {}",
            e
        )),
    };
    let de: HashMap<Bytes, Bytes> = match bincode::deserialize(&read) {
        Ok(r) => r,
        Err(e) => err(err!("Failed to unpack old file with: {}", e)),
    };
    unsafe {
        if serial {
            // transfer serially
            for (key, value) in de.into_iter() {
                let q = query!(
                    "USET",
                    String::from_utf8_unchecked(key),
                    String::from_utf8_unchecked(value)
                );
                okay(&mut con, q)
            }
        } else {
            // transfer all at once
            let mut query = Query::from("USET");
            for (key, value) in de.into_iter() {
                query.push(String::from_utf8_unchecked(key));
                query.push(String::from_utf8_unchecked(value));
            }
            okay(&mut con, query)
        }
    }
    info!("Finished migration");
}

fn err(_i: ()) -> ! {
    process::exit(0x01)
}

fn okay(con: &mut Connection, q: Query) {
    match con.run_query_raw(&q) {
        Ok(Element::RespCode(RespCode::Okay)) => {}
        Err(e) => err(err!("An I/O error occurred while running query: {}", e)),
        Ok(_) => err(err!("Unknown response from server")),
    }
}
