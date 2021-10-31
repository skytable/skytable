/*
 * Created on Sat Aug 21 2021
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

use super::element::{FlatElement, OwnedElement};
use super::{OwnedQuery, ParseError, Parser};
use bytes::Bytes;

#[test]
fn test_read_line() {
    let line = "abcdef\n".as_bytes();
    let mut parser = Parser::new(line);
    let retline = unsafe { parser.read_line().unwrap().to_owned() };
    assert_eq!(retline, &line[..line.len() - 1]);
    assert_eq!(parser.remaining(), 0);
}

#[test]
fn test_read_until() {
    let mybytes = "123456789".as_bytes();
    let mut parser = Parser::new(mybytes);
    let retline = unsafe { parser.read_until(7).unwrap().to_owned() };
    assert_eq!(retline, &mybytes[..mybytes.len() - 2]);
    assert_eq!(parser.remaining(), 2);
}

#[test]
fn test_will_cursor_give_char() {
    let mybytes = "\r\n".as_bytes();
    let parser = Parser::new(mybytes);
    assert!(parser.will_cursor_give_char(b'\r', false).unwrap());
}

#[test]
fn test_will_cursor_give_linefeed() {
    let mybytes = "\n".as_bytes();
    let parser = Parser::new(mybytes);
    assert!(parser.will_cursor_give_linefeed().unwrap());
}

#[test]
fn test_will_cursor_give_linefeed_fail() {
    let mybytes = "9\n".as_bytes();
    let parser = Parser::new(mybytes);
    assert!(!parser.will_cursor_give_linefeed().unwrap());
}

#[test]
fn test_parse_into_usize() {
    let bytes = "12345678".as_bytes();
    let usz = Parser::parse_into_usize(bytes).unwrap();
    assert_eq!(usz, 12345678);
}

#[test]
fn test_parse_into_usize_fail() {
    let bytes = "12345678ab".as_bytes();
    let usze = Parser::parse_into_usize(bytes).unwrap_err();
    assert_eq!(usze, ParseError::DatatypeParseFailure);
}

#[test]
fn test_parse_into_u64() {
    let bytes = "12345678".as_bytes();
    let usz = Parser::parse_into_u64(bytes).unwrap();
    assert_eq!(usz, 12345678);
}

#[test]
fn test_parse_into_u64_fail() {
    let bytes = "12345678ab".as_bytes();
    let usze = Parser::parse_into_u64(bytes).unwrap_err();
    assert_eq!(usze, ParseError::DatatypeParseFailure);
}

#[test]
fn test_metaframe_parse_count() {
    let bytes = "*1\n".as_bytes();
    let mut parser = Parser::new(bytes);
    assert_eq!(parser.parse_metaframe_get_datagroup_count().unwrap(), 1);
    assert!(parser.remaining() == 0);
    let bytes = "*10\n".as_bytes();
    let mut parser = Parser::new(bytes);
    assert_eq!(parser.parse_metaframe_get_datagroup_count().unwrap(), 10);
    assert!(parser.remaining() == 0);
}

#[test]
fn test_metaframe_parse_count_fail() {
    let bytes = "*1A\n".as_bytes();
    let mut parser = Parser::new(bytes);
    assert_eq!(
        parser.parse_metaframe_get_datagroup_count().unwrap_err(),
        ParseError::DatatypeParseFailure
    );
    assert!(parser.remaining() == 0);
    let bytes = "*10A\n".as_bytes();
    let mut parser = Parser::new(bytes);
    assert_eq!(
        parser.parse_metaframe_get_datagroup_count().unwrap_err(),
        ParseError::DatatypeParseFailure
    );
    assert!(parser.remaining() == 0);
}

#[test]
fn test_next() {
    let x = "5\nsayan\n".as_bytes();
    let mut parser = Parser::new(x);
    let ret = unsafe { parser._next().unwrap().to_owned() };
    assert_eq!(ret, "sayan".as_bytes());
    // WHY 1? Newline is not skipped
    assert_eq!(parser.remaining(), 1);
}

#[test]
fn test_next_fail() {
    let x = "5\nsaya".as_bytes();
    let mut parser = Parser::new(x);
    let ret = parser._next().unwrap_err();
    assert_eq!(ret, ParseError::NotEnough);
}

#[test]
fn test_parse_next_string() {
    let mystr = "+5\nsayan\n".as_bytes();
    let mut parser = Parser::new(mystr);
    // we do this since we skip `+`
    parser.incr_cursor();
    let st = unsafe { parser.parse_next_string().unwrap().to_owned() };
    assert_eq!(st, "sayan".as_bytes());
    // WHY 0? Because parse_next_<ty> forwards LF
    assert_eq!(parser.remaining(), 0);
}

#[test]
fn test_parse_next_string_fail() {
    let mystr = "5\nsayan".as_bytes();
    let mut parser = Parser::new(mystr);
    let st = parser.parse_next_string().unwrap_err();
    // NO LF, so not enough
    assert_eq!(st, ParseError::NotEnough);
}

#[test]
fn test_parse_next_u64() {
    let myint = "5\n12345\n".as_bytes();
    let mut parser = Parser::new(myint);
    let int = parser.parse_next_u64().unwrap();
    assert_eq!(int, 12345);
}

#[test]
fn test_parse_next_u64_fail() {
    let myint = "5\n12345".as_bytes();
    let mut parser = Parser::new(myint);
    let int = parser.parse_next_u64().unwrap_err();
    assert_eq!(int, ParseError::NotEnough);
}

#[test]
fn test_parse_next_null() {
    let empty_buf = "".as_bytes();
    assert_eq!(
        Parser::new(empty_buf)._next().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_metaframe_parse() {
    let metaframe = "*2\n".as_bytes();
    let mut parser = Parser::new(metaframe);
    assert_eq!(2, parser.parse_metaframe_get_datagroup_count().unwrap());
}

#[test]
fn test_cursor_next_char() {
    let bytes = &[b'\n'];
    assert!(Parser::new(&bytes[..])
        .will_cursor_give_char(b'\n', false)
        .unwrap());
    let bytes = &[];
    assert!(Parser::new(&bytes[..])
        .will_cursor_give_char(b'\r', true)
        .unwrap());
    let bytes = &[];
    assert!(
        Parser::new(&bytes[..])
            .will_cursor_give_char(b'\n', false)
            .unwrap_err()
            == ParseError::NotEnough
    );
}

#[test]
fn test_metaframe_parse_fail() {
    // First byte should be CR and not $
    let metaframe = "$2\n*2\n".as_bytes();
    let mut parser = Parser::new(metaframe);
    assert_eq!(
        parser.parse_metaframe_get_datagroup_count().unwrap_err(),
        ParseError::UnexpectedByte
    );
    // Give a wrong length approximation
    let metaframe = "\r1\n*2\n".as_bytes();
    assert_eq!(
        Parser::new(metaframe)
            .parse_metaframe_get_datagroup_count()
            .unwrap_err(),
        ParseError::UnexpectedByte
    );
}

#[test]
fn test_query_fail_not_enough() {
    let query_packet = "*".as_bytes();
    assert_eq!(
        Parser::new(query_packet).parse().err().unwrap(),
        ParseError::NotEnough
    );
    let metaframe = "*2".as_bytes();
    assert_eq!(
        Parser::new(metaframe)
            .parse_metaframe_get_datagroup_count()
            .unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_parse_next_u64_max() {
    let max = 18446744073709551615;
    assert!(u64::MAX == max);
    let bytes = "20\n18446744073709551615\n".as_bytes();
    let our_u64 = Parser::new(bytes).parse_next_u64().unwrap();
    assert_eq!(our_u64, max);
    // now overflow the u64
    let bytes = "21\n184467440737095516156\n".as_bytes();
    let our_u64 = Parser::new(bytes).parse_next_u64().unwrap_err();
    assert_eq!(our_u64, ParseError::DatatypeParseFailure);
}

#[test]
fn test_parse_next_element_string() {
    let bytes = "+5\nsayan\n".as_bytes();
    let next_element = Parser::new(bytes).parse_next_element().unwrap();
    unsafe {
        assert_eq!(
            next_element.as_owned_element(),
            OwnedElement::String(Bytes::from("sayan"))
        );
    }
}

#[test]
fn test_parse_next_element_string_fail() {
    let bytes = "+5\nsayan".as_bytes();
    assert_eq!(
        Parser::new(bytes).parse_next_element().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_parse_next_element_u64() {
    let bytes = ":20\n18446744073709551615\n".as_bytes();
    let our_u64 = unsafe {
        Parser::new(bytes)
            .parse_next_element()
            .unwrap()
            .as_owned_element()
    };
    assert_eq!(our_u64, OwnedElement::UnsignedInt(u64::MAX));
}

#[test]
fn test_parse_next_element_u64_fail() {
    let bytes = ":20\n18446744073709551615".as_bytes();
    assert_eq!(
        Parser::new(bytes).parse_next_element().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_parse_next_element_array() {
    let bytes = "&3\n+4\nMGET\n+3\nfoo\n+3\nbar\n".as_bytes();
    let mut parser = Parser::new(bytes);
    let array = parser.parse_next_element().unwrap();
    assert_eq!(
        unsafe { array.as_owned_element() },
        OwnedElement::Array(vec![
            OwnedElement::String(Bytes::from("MGET".to_owned())),
            OwnedElement::String(Bytes::from("foo".to_owned())),
            OwnedElement::String(Bytes::from("bar".to_owned()))
        ])
    );
    assert_eq!(parser.remaining(), 0);
}

#[test]
fn test_parse_next_element_array_fail() {
    // should've been three elements, but there are two!
    let bytes = "&3\n+4\nMGET\n+3\nfoo\n+3\n".as_bytes();
    let mut parser = Parser::new(bytes);
    assert_eq!(
        parser.parse_next_element().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_parse_nested_array() {
    // let's test a nested array
    let bytes =
        "&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n+6\nreally\n+4\nhard\n"
            .as_bytes();
    let mut parser = Parser::new(bytes);
    let array = parser.parse_next_element().unwrap();
    assert_eq!(
        unsafe { array.as_owned_element() },
        OwnedElement::Array(vec![
            OwnedElement::String(Bytes::from("ACT".to_owned())),
            OwnedElement::String(Bytes::from("foo".to_owned())),
            OwnedElement::Array(vec![
                OwnedElement::String(Bytes::from("sayan".to_owned())),
                OwnedElement::String(Bytes::from("is".to_owned())),
                OwnedElement::String(Bytes::from("working".to_owned())),
                OwnedElement::Array(vec![
                    OwnedElement::String(Bytes::from("really".to_owned())),
                    OwnedElement::String(Bytes::from("hard".to_owned()))
                ])
            ])
        ])
    );
    assert_eq!(parser.remaining(), 0);
}

#[test]
fn test_parse_multitype_array() {
    // let's test a nested array
    let bytes = "&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n:2\n23\n+5\napril\n"
        .as_bytes();
    let mut parser = Parser::new(bytes);
    let array = parser.parse_next_element().unwrap();
    assert_eq!(
        unsafe { array.as_owned_element() },
        OwnedElement::Array(vec![
            OwnedElement::String(Bytes::from("ACT".to_owned())),
            OwnedElement::String(Bytes::from("foo".to_owned())),
            OwnedElement::Array(vec![
                OwnedElement::String(Bytes::from("sayan".to_owned())),
                OwnedElement::String(Bytes::from("is".to_owned())),
                OwnedElement::String(Bytes::from("working".to_owned())),
                OwnedElement::Array(vec![
                    OwnedElement::UnsignedInt(23),
                    OwnedElement::String(Bytes::from("april".to_owned()))
                ])
            ])
        ])
    );
    assert_eq!(parser.remaining(), 0);
}

#[test]
fn test_parse_a_query() {
    let bytes =
        "*1\n&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n:2\n23\n+5\napril\n"
            .as_bytes();
    let mut parser = Parser::new(bytes);
    let (resp, _) = parser.parse().unwrap();
    assert_eq!(
        unsafe { resp.into_owned_query() },
        OwnedQuery::SimpleQuery(OwnedElement::Array(vec![
            OwnedElement::String(Bytes::from("ACT".to_owned())),
            OwnedElement::String(Bytes::from("foo".to_owned())),
            OwnedElement::Array(vec![
                OwnedElement::String(Bytes::from("sayan".to_owned())),
                OwnedElement::String(Bytes::from("is".to_owned())),
                OwnedElement::String(Bytes::from("working".to_owned())),
                OwnedElement::Array(vec![
                    OwnedElement::UnsignedInt(23),
                    OwnedElement::String(Bytes::from("april".to_owned()))
                ])
            ])
        ]))
    );
    assert_eq!(parser.remaining(), 0);
}

#[test]
fn test_parse_a_query_fail_moredata() {
    let bytes =
        "*1\n&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&1\n:2\n23\n+5\napril\n"
            .as_bytes();
    let mut parser = Parser::new(bytes);
    assert_eq!(parser.parse().unwrap_err(), ParseError::UnexpectedByte);
}

#[test]
fn test_pipelined_query_incomplete() {
    // this was a pipelined query: we expected two queries but got one!
    let bytes =
        "*2\n&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n:2\n23\n+5\napril\n"
            .as_bytes();
    assert_eq!(
        Parser::new(bytes).parse().unwrap_err(),
        ParseError::NotEnough
    )
}

#[test]
fn test_pipelined_query() {
    let bytes =
        "*2\n&3\n+3\nACT\n+3\nfoo\n&3\n+5\nsayan\n+2\nis\n+7\nworking\n+4\nHEYA\n".as_bytes();
    /*
    (*2\n)(&3\n)({+3\nACT\n}{+3\nfoo\n}{[&3\n][+5\nsayan\n][+2\nis\n][+7\nworking\n]})(+4\nHEYA\n)
    */
    let (res, forward_by) = Parser::new(bytes).parse().unwrap();
    assert_eq!(
        unsafe { res.into_owned_query() },
        OwnedQuery::PipelineQuery(vec![
            OwnedElement::Array(vec![
                OwnedElement::String(Bytes::from("ACT".to_owned())),
                OwnedElement::String(Bytes::from("foo".to_owned())),
                OwnedElement::Array(vec![
                    OwnedElement::String(Bytes::from("sayan".to_owned())),
                    OwnedElement::String(Bytes::from("is".to_owned())),
                    OwnedElement::String(Bytes::from("working".to_owned()))
                ])
            ]),
            OwnedElement::String(Bytes::from("HEYA".to_owned()))
        ])
    );
    assert_eq!(forward_by, bytes.len());
}

#[test]
fn test_query_with_part_of_next_query() {
    let bytes =
        "*1\n&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n:2\n23\n+5\napril\n*1\n"
            .as_bytes();
    let (res, forward_by) = Parser::new(bytes).parse().unwrap();
    assert_eq!(
        unsafe { res.into_owned_query() },
        OwnedQuery::SimpleQuery(OwnedElement::Array(vec![
            OwnedElement::String(Bytes::from("ACT".to_owned())),
            OwnedElement::String(Bytes::from("foo".to_owned())),
            OwnedElement::Array(vec![
                OwnedElement::String(Bytes::from("sayan".to_owned())),
                OwnedElement::String(Bytes::from("is".to_owned())),
                OwnedElement::String(Bytes::from("working".to_owned())),
                OwnedElement::Array(vec![
                    OwnedElement::UnsignedInt(23),
                    OwnedElement::String(Bytes::from("april".to_owned()))
                ])
            ])
        ]))
    );
    // there are some ingenious folks on this planet who might just go bombing one query after the other
    // we happily ignore those excess queries and leave it to the next round of parsing
    assert_eq!(forward_by, bytes.len() - "*1\n".len());
}

#[test]
fn test_parse_flat_array() {
    let bytes = "_3\n+3\nSET\n+5\nHello\n+5\nWorld\n".as_bytes();
    let res = Parser::new(bytes).parse_next_element().unwrap();
    assert_eq!(
        unsafe { res.as_owned_element() },
        OwnedElement::FlatArray(vec![
            FlatElement::String(Bytes::from("SET".to_owned())),
            FlatElement::String(Bytes::from("Hello".to_owned())),
            FlatElement::String(Bytes::from("World".to_owned()))
        ])
    );
}

#[test]
fn test_flat_array_incomplete() {
    let bytes = "*1\n_1\n".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n_1".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n_".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
}

#[test]
fn test_array_incomplete() {
    let bytes = "*1\n&1\n".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n&1".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n&".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
}

#[test]
fn test_string_incomplete() {
    let bytes = "*1\n+1\n".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n+1".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n+".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
}

#[test]
fn test_u64_incomplete() {
    let bytes = "*1\n:1\n".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n:1".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
    let bytes = "*1\n:".as_bytes();
    let res = Parser::new(bytes).parse().unwrap_err();
    assert_eq!(res, ParseError::NotEnough);
}

#[test]
fn test_parse_any_array() {
    let anyarray = "*1\n~3\n3\nthe\n3\ncat\n6\nmeowed\n".as_bytes();
    let (query, forward_by) = Parser::new(anyarray).parse().unwrap();
    assert_eq!(forward_by, anyarray.len());
    assert_eq!(
        unsafe { query.into_owned_query() },
        OwnedQuery::SimpleQuery(OwnedElement::AnyArray(vec![
            "the".into(),
            "cat".into(),
            "meowed".into()
        ]))
    )
}
