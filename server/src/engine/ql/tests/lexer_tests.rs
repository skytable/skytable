/*
 * Created on Sun Dec 18 2022
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
    super::{
        super::lex::{Ident, Token},
        lex_insecure, lex_secure,
    },
    crate::engine::{data::lit::Lit, error::QueryError},
};

macro_rules! v(
    ($e:literal) => {{
        $e.as_bytes().to_vec()
    }};
    ($($e:literal),* $(,)?) => {{
        ($(v!($e)),*)
    }};
);

#[test]
fn lex_ident() {
    let src = v!("hello");
    assert_eq!(
        lex_insecure(&src).unwrap(),
        vec![Token::Ident(Ident::from("hello"))]
    );
}

// literals
#[test]
fn lex_unsigned_int() {
    let number = v!("123456");
    assert_eq!(
        lex_insecure(&number).unwrap(),
        vec![Token::Lit(Lit::new_uint(123456))]
    );
}
#[test]
fn lex_signed_int() {
    let number = v!("-123456");
    assert_eq!(
        lex_insecure(&number).unwrap(),
        vec![Token::Lit(Lit::new_sint(-123456))]
    );
}
#[test]
fn lex_bool() {
    let (t, f) = v!("true", "false");
    assert_eq!(
        lex_insecure(&t).unwrap(),
        vec![Token::Lit(Lit::new_bool(true))]
    );
    assert_eq!(
        lex_insecure(&f).unwrap(),
        vec![Token::Lit(Lit::new_bool(false))]
    );
}
#[test]
fn lex_string() {
    let s = br#" "hello, world" "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::new_string("hello, world".into()))]
    );
    let s = br#" 'hello, world' "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::new_string("hello, world".into()))]
    );
}
#[test]
fn lex_string_test_escape_quote() {
    let s = br#" "\"hello world\"" "#; // == "hello world"
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::new_string("\"hello world\"".into()))]
    );
    let s = br#" '\'hello world\'' "#; // == 'hello world'
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::new_string("'hello world'".into()))]
    );
}
#[test]
fn lex_string_use_different_quote_style() {
    let s = br#" "he's on it" "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::new_string("he's on it".into()))]
    );
    let s = br#" 'he thinks that "that girl" fixed it' "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::new_string(
            "he thinks that \"that girl\" fixed it".into()
        ))]
    )
}
#[test]
fn lex_string_escape_bs() {
    let s = v!(r#" "windows has c:\\" "#);
    assert_eq!(
        lex_insecure(&s).unwrap(),
        vec![Token::Lit(Lit::new_string("windows has c:\\".into()))]
    );
    let s = v!(r#" 'windows has c:\\' "#);
    assert_eq!(
        lex_insecure(&s).unwrap(),
        vec![Token::Lit(Lit::new_string("windows has c:\\".into()))]
    );
    let lol = v!(r#"'\\\\\\\\\\'"#);
    let lexed = lex_insecure(&lol).unwrap();
    assert_eq!(
        lexed,
        vec![Token::Lit(Lit::new_string("\\".repeat(5)))],
        "lol"
    )
}
#[test]
fn lex_string_bad_escape() {
    let wth = br#" '\a should be an alert on windows apparently' "#;
    assert_eq!(lex_insecure(wth).unwrap_err(), QueryError::LexInvalidInput);
}
#[test]
fn lex_string_unclosed() {
    let wth = br#" 'omg where did the end go "#;
    assert_eq!(lex_insecure(wth).unwrap_err(), QueryError::LexInvalidInput);
    let wth = br#" 'see, we escaped the end\' "#;
    assert_eq!(lex_insecure(wth).unwrap_err(), QueryError::LexInvalidInput);
}
#[test]
fn lex_unsafe_literal_mini() {
    let usl = lex_insecure("\r0\n".as_bytes()).unwrap();
    assert_eq!(usl.len(), 1);
    assert_eq!(Token::Lit(Lit::new_bin(b"")), usl[0]);
}
#[test]
fn lex_unsafe_literal() {
    let usl = lex_insecure("\r9\nabcdefghi".as_bytes()).unwrap();
    assert_eq!(usl.len(), 1);
    assert_eq!(Token::Lit(Lit::new_bin(b"abcdefghi")), usl[0]);
}
#[test]
fn lex_unsafe_literal_pro() {
    let usl = lex_insecure("\r18\nabcdefghi123456789".as_bytes()).unwrap();
    assert_eq!(usl.len(), 1);
    assert_eq!(Token::Lit(Lit::new_bin(b"abcdefghi123456789")), usl[0]);
}

/*
    safe query tests
*/

fn make_safe_query(a: &[u8], b: &[u8]) -> (Vec<u8>, usize) {
    let mut s = Vec::with_capacity(a.len() + b.len());
    s.extend(a);
    s.extend(b);
    (s, a.len())
}

#[test]
fn safe_query_param_empty() {
    for i in 0..100 {
        let (query, query_window) = make_safe_query(&b"?".repeat(i), b"");
        let ret = lex_secure(&query, query_window).unwrap();
        assert_eq!(
            ret,
            (0..i).map(|_| Token![?]).collect::<Vec<Token<'static>>>()
        );
    }
}

#[test]
fn safe_query_less_param() {
    for i in 1..=10 {
        /*
        test placeholder combinations:
        - 1PH 1P
        - 2PH 1P, 2PH 2P
        - 3PH 1P, 3PH 2P, 3PH 3P
        */
        for j in 1..=i {
            let (query, query_window) =
                make_safe_query(&b"?".repeat(i), &b"\x065\nsayan".repeat(j));
            let ret = lex_secure(&query, query_window).unwrap();
            // check
            let exp_params = (0..j)
                .map(|_| Token::Lit(Lit::new_str("sayan")))
                .collect::<Vec<Token<'static>>>();
            let exp_placeholders = (0..i - j)
                .map(|_| Token![?])
                .collect::<Vec<Token<'static>>>();
            assert_eq!(ret, [exp_params, exp_placeholders].concat());
        }
    }
}

#[test]
fn safe_query_all_literals() {
    let (query, query_window) = make_safe_query(
        b"? ? ? ? ? ? ?",
        b"\x00\x01\x01\x021234\n\x03-1234\n\x041234.5678\n\x0513\nbinarywithlf\n\x065\nsayan",
    );
    let ret = lex_secure(&query, query_window).unwrap();
    assert_eq!(
        ret,
        into_vec![Token<'static> => (
            Token![null],
            Lit::new_bool(true),
            Lit::new_uint(1234),
            Lit::new_sint(-1234),
            Lit::new_float(1234.5678),
            Lit::new_bin(b"binarywithlf\n"),
            Lit::new_string("sayan".into()),
        )],
    );
}

const SFQ_NULL: &[u8] = b"\x00";
const SFQ_BOOL_FALSE: &[u8] = b"\x01\0";
const SFQ_BOOL_TRUE: &[u8] = b"\x01\x01";
const SFQ_UINT: &[u8] = b"\x0218446744073709551615\n";
const SFQ_SINT: &[u8] = b"\x03-9223372036854775808\n";
const SFQ_FLOAT: &[u8] = b"\x043.141592654\n";
const SFQ_BINARY: &[u8] = "\x0546\ncringe😃😄😁😆😅😂🤣😊😸😺".as_bytes();
const SFQ_STRING: &[u8] = "\x0646\ncringe😃😄😁😆😅😂🤣😊😸😺".as_bytes();

#[test]
fn safe_query_null() {
    let (query, query_window) = make_safe_query(b"?", SFQ_NULL);
    let r = lex_secure(&query, query_window).unwrap();
    assert_eq!(r, vec![Token![null]])
}

#[test]
fn safe_query_bool() {
    let (query, query_window) = make_safe_query(b"?", SFQ_BOOL_FALSE);
    let b_false = lex_secure(&query, query_window).unwrap();
    let (query, query_window) = make_safe_query(b"?", SFQ_BOOL_TRUE);
    let b_true = lex_secure(&query, query_window).unwrap();
    assert_eq!(
        [b_false, b_true].concat(),
        vec![
            Token::from(Lit::new_bool(false)),
            Token::from(Lit::new_bool(true))
        ]
    );
}

#[test]
fn safe_query_uint() {
    let (query, query_window) = make_safe_query(b"?", SFQ_UINT);
    let int = lex_secure(&query, query_window).unwrap();
    assert_eq!(int, vec![Token::Lit(Lit::new_uint(u64::MAX))]);
}

#[test]
fn safe_query_sint() {
    let (query, query_window) = make_safe_query(b"?", SFQ_SINT);
    let int = lex_secure(&query, query_window).unwrap();
    assert_eq!(int, vec![Token::Lit(Lit::new_sint(i64::MIN))]);
}

#[test]
fn safe_query_float() {
    let (query, query_window) = make_safe_query(b"?", SFQ_FLOAT);
    let float = lex_secure(&query, query_window).unwrap();
    assert_eq!(float, vec![Token::Lit(Lit::new_float(3.141592654))]);
}

#[test]
fn safe_query_binary() {
    for (test_payload_string, expected) in [
        (b"\x050\n".as_slice(), b"".as_slice()),
        (SFQ_BINARY, "cringe😃😄😁😆😅😂🤣😊😸😺".as_bytes()),
    ] {
        let (query, query_window) = make_safe_query(b"?", test_payload_string);
        let binary = lex_secure(&query, query_window).unwrap();
        assert_eq!(binary, vec![Token::Lit(Lit::new_bin(expected))]);
    }
}

#[test]
fn safe_query_string() {
    for (test_payload_string, expected) in [
        (b"\x060\n".as_slice(), ""),
        (SFQ_STRING, "cringe😃😄😁😆😅😂🤣😊😸😺"),
    ] {
        let (query, query_window) = make_safe_query(b"?", test_payload_string);
        let binary = lex_secure(&query, query_window).unwrap();
        assert_eq!(binary, vec![Token::Lit(Lit::new_str(expected))]);
    }
}

#[test]
fn safe_params_shuffled() {
    let expected = [
        (SFQ_NULL, Token![null]),
        (SFQ_BOOL_FALSE, Token::Lit(Lit::new_bool(false))),
        (SFQ_BOOL_TRUE, Token::Lit(Lit::new_bool(true))),
        (SFQ_UINT, Token::Lit(Lit::new_uint(u64::MAX))),
        (SFQ_SINT, Token::Lit(Lit::new_sint(i64::MIN))),
        (SFQ_FLOAT, Token::Lit(Lit::new_float(3.141592654))),
        (
            SFQ_BINARY,
            Token::Lit(Lit::new_bin("cringe😃😄😁😆😅😂🤣😊😸😺".as_bytes())),
        ),
        (
            SFQ_STRING,
            Token::Lit(Lit::new_string(
                "cringe😃😄😁😆😅😂🤣😊😸😺".to_owned().into(),
            )),
        ),
    ];
    let mut rng = crate::util::test_utils::randomizer();
    for _ in 0..expected.len().pow(2) {
        let mut this_expected = expected.clone();
        crate::util::test_utils::shuffle_slice(&mut this_expected, &mut rng);
        let param_segment: Vec<u8> = this_expected
            .iter()
            .map(|(raw, _)| raw.to_vec())
            .flatten()
            .collect();
        let (query, query_window) = make_safe_query(b"? ? ? ? ? ? ? ?", &param_segment);
        let ret = lex_secure(&query, query_window).unwrap();
        assert_eq!(
            ret,
            this_expected
                .into_iter()
                .map(|(_, expected)| expected)
                .collect::<Vec<_>>()
        )
    }
}
