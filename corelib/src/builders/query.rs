/*
 * Created on Tue Aug 04 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

//! # The `Query` module
//! This module can be used to build queries that can be sent to the database
//! server. It prepares packets following the Terrapipe protocol.

use crate::terrapipe::DEF_QMETALINE_BUFSIZE;

/// A `QueryBuilder` which enables building simple and pipelined queries
pub enum QueryBuilder {
    /// A `SimpleQuery`
    SimpleQuery,
    // TODO(@ohsayan): Add pipelined queries here
}

impl QueryBuilder {
    /// Instantiate a new `SimpleQuery` instance which can be used to build
    /// queries
    pub fn new_simple() -> SimpleQuery {
        SimpleQuery::new()
    }
}

/// A simple query is used for simple queries - queries that run only one command
pub struct SimpleQuery {
    /// The metaline of the simple query
    metaline: Vec<u8>,
    /// The metalayout of the simple query
    metalayout: Vec<u8>,
    /// The dataframe of the simple query
    dataframe: Vec<u8>,
}

impl SimpleQuery {
    /// Create a new `SimpleQuery` object
    pub fn new() -> Self {
        let mut metaline = Vec::with_capacity(DEF_QMETALINE_BUFSIZE);
        metaline.push(b'*');
        metaline.push(b'!');
        SimpleQuery {
            metaline,
            metalayout: Vec::with_capacity(128),
            dataframe: Vec::with_capacity(1024),
        }
    }
    /// Add an item to the query packet
    ///
    /// This accepts anything which can be turned into a sequence of bytes
    pub fn add(&mut self, cmd: impl Into<Vec<u8>>) {
        let cmd = cmd.into();
        let l = cmd.len().to_string();
        self.metalayout.push(b'#');
        self.metalayout.extend(l.as_bytes());
        self.dataframe.extend(&cmd);
        self.dataframe.push(b'\n');
    }
    /// Prepare a query packet that can be directly written to the socket
    pub fn prepare_query(mut self) -> Vec<u8> {
        self.metaline
            .extend(self.dataframe.len().to_string().as_bytes());
        self.metaline.push(b'!');
        self.metaline
            .extend((self.metalayout.len() + 1).to_string().as_bytes());
        self.metaline.push(b'\n');
        self.metalayout.push(b'\n');
        [self.metaline, self.metalayout, self.dataframe].concat()
    }
    /// Create a query from a command line input - usually separated by whitespaces
    pub fn from_cmd(&mut self, cmd: String) {
        let cmd: Vec<&str> = cmd.split_whitespace().collect();
        cmd.into_iter().for_each(|val| self.add(val));
    }
}

#[test]
fn test_squery() {
    let mut q = SimpleQuery::new();
    q.add("SET");
    q.add("sayan");
    q.add("17");
    println!("{}", String::from_utf8_lossy(&q.prepare_query()));
}
