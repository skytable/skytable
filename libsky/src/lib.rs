/*
 * Created on Mon Jul 20 2020
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

//! The core library for the Skytable
//!
//! This contains modules which are shared by both the `cli` and the `server` modules

pub mod util;
use skytable::Query;
use std::error::Error;
/// A generic result
pub type TResult<T> = Result<T, Box<dyn Error>>;
/// The size of the read buffer in bytes
pub const BUF_CAP: usize = 8 * 1024; // 8 KB per-connection

use std::str::FromStr;

lazy_static::lazy_static! {
    static ref RE: regex::Regex = regex::Regex::from_str(r#"("[^"]*"|'[^']*'|[\S]+)+"#).unwrap();
}

pub fn split_into_args(q: &str) -> Vec<String> {
    let args: Vec<String> = RE
        .find_iter(q)
        .map(|val| val.as_str().replace("'", "").replace("\"", ""))
        .collect();
    args
}

pub fn turn_into_query(q: &str) -> Query {
    let mut query = Query::new();
    split_into_args(q).into_iter().for_each(|arg| {
        query.push(arg);
    });
    query
}

pub fn into_raw_query(q: &str) -> Vec<u8> {
    turn_into_query(q).into_raw_query()
}
