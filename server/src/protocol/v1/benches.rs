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

extern crate test;
use {
    super::{super::Query, Parser},
    test::Bencher,
};

#[bench]
fn simple_query(b: &mut Bencher) {
    const PAYLOAD: &[u8] = b"*1\n~3\n3\nSET\n1\nx\n3\n100\n";
    let expected = vec!["SET".to_owned(), "x".to_owned(), "100".to_owned()];
    b.iter(|| {
        let (query, forward) = Parser::parse(PAYLOAD).unwrap();
        assert_eq!(forward, PAYLOAD.len());
        let query = if let Query::Simple(sq) = query {
            sq
        } else {
            panic!("Got pipeline instead of simple query");
        };
        let ret: Vec<String> = query
            .as_slice()
            .iter()
            .map(|s| String::from_utf8_lossy(s.as_slice()).to_string())
            .collect();
        assert_eq!(ret, expected)
    });
}

#[bench]
fn pipelined_query(b: &mut Bencher) {
    const PAYLOAD: &[u8] = b"*2\n~3\n3\nSET\n1\nx\n3\n100\n~2\n3\nGET\n1\nx\n";
    let expected = vec![
        vec!["SET".to_owned(), "x".to_owned(), "100".to_owned()],
        vec!["GET".to_owned(), "x".to_owned()],
    ];
    b.iter(|| {
        let (query, forward) = Parser::parse(PAYLOAD).unwrap();
        assert_eq!(forward, PAYLOAD.len());
        let query = if let Query::Pipelined(sq) = query {
            sq
        } else {
            panic!("Got simple instead of pipeline query");
        };
        let ret: Vec<Vec<String>> = query
            .into_inner()
            .iter()
            .map(|query| {
                query
                    .as_slice()
                    .iter()
                    .map(|v| String::from_utf8_lossy(v.as_slice()).to_string())
                    .collect()
            })
            .collect();
        assert_eq!(ret, expected)
    });
}
