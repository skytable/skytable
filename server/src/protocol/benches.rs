/*
 * Created on Tue Nov 02 2021
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

/*
 Do note that the result of the benches might actually be slower, than faster! The reason it is so, is simply because of
 the fact that we generate owned queries, by copying bytes which adds an overhead, but offers simplicity in writing tests
 and/or benches
*/

extern crate test;
use super::{element::OwnedElement, OwnedQuery, Parser};
use bytes::Bytes;
use test::Bencher;

#[bench]
fn bench_simple_query_string(b: &mut Bencher) {
    const PAYLOAD: &[u8] = b"*1\n+5\nsayan\n";
    unsafe {
        b.iter(|| {
            assert_eq!(
                Parser::new(PAYLOAD).parse().unwrap().0.into_owned_query(),
                OwnedQuery::SimpleQuery(OwnedElement::String(Bytes::from("sayan")))
            );
        })
    }
}

#[bench]
fn bench_simple_query_uint(b: &mut Bencher) {
    const PAYLOAD: &[u8] = b"*1\n:5\n12345\n";
    unsafe {
        b.iter(|| {
            assert_eq!(
                Parser::new(PAYLOAD).parse().unwrap().0.into_owned_query(),
                OwnedQuery::SimpleQuery(OwnedElement::UnsignedInt(12345))
            );
        })
    }
}

#[bench]
fn bench_simple_query_any_array(b: &mut Bencher) {
    const PAYLOAD: &[u8] = b"*1\n~3\n3\nthe\n3\ncat\n6\nmeowed\n";
    unsafe {
        b.iter(|| {
            assert_eq!(
                Parser::new(PAYLOAD).parse().unwrap().0.into_owned_query(),
                OwnedQuery::SimpleQuery(OwnedElement::AnyArray(vec![
                    "the".into(),
                    "cat".into(),
                    "meowed".into()
                ]))
            )
        })
    }
}
