/*
 * Created on Mon May 02 2022
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
    super::Parser,
    crate::protocol::{ParseError, Query},
};

#[cfg(test)]
const SQPAYLOAD: &[u8] = b"*1\n~3\n3\nSET\n1\nx\n3\n100\n";
#[cfg(test)]
const PQPAYLOAD: &[u8] = b"*2\n~3\n3\nSET\n1\nx\n3\n100\n~2\n3\nGET\n1\nx\n";

#[test]
fn parse_simple_query() {
    let payload = SQPAYLOAD.to_vec();
    let (q, f) = Parser::parse(&payload).unwrap();
    let q: Vec<String> = if let Query::Simple(q) = q {
        q.as_slice()
            .iter()
            .map(|v| String::from_utf8_lossy(unsafe { v.as_slice() }).to_string())
            .collect()
    } else {
        panic!("Expected simple query")
    };
    assert_eq!(f, payload.len());
    assert_eq!(q, vec!["SET".to_owned(), "x".into(), "100".into()]);
}

#[test]
fn parse_simple_query_incomplete() {
    for i in 0..SQPAYLOAD.len() - 1 {
        let slice = &SQPAYLOAD[..i];
        assert_eq!(Parser::parse(slice).unwrap_err(), ParseError::NotEnough);
    }
}

#[test]
fn parse_pipelined_query() {
    let payload = PQPAYLOAD.to_vec();
    let (q, f) = Parser::parse(&payload).unwrap();
    let q: Vec<Vec<String>> = if let Query::Pipelined(q) = q {
        q.into_inner()
            .iter()
            .map(|sq| {
                sq.iter()
                    .map(|v| String::from_utf8_lossy(unsafe { v.as_slice() }).to_string())
                    .collect()
            })
            .collect()
    } else {
        panic!("Expected pipelined query query")
    };
    assert_eq!(f, payload.len());
    assert_eq!(
        q,
        vec![
            vec!["SET".to_owned(), "x".into(), "100".into()],
            vec!["GET".into(), "x".into()]
        ]
    );
}

#[test]
fn parse_pipelined_query_incomplete() {
    for i in 0..PQPAYLOAD.len() - 1 {
        let slice = &PQPAYLOAD[..i];
        assert_eq!(Parser::parse(slice).unwrap_err(), ParseError::NotEnough);
    }
}
