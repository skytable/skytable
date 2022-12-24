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
        super::lexer::{Lit, Token},
        lex_insecure,
    },
    crate::engine::ql::LangError,
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
        vec![Token::Ident("hello".into())]
    );
}

// literals
#[test]
fn lex_unsigned_int() {
    let number = v!("123456");
    assert_eq!(
        lex_insecure(&number).unwrap(),
        vec![Token::Lit(Lit::UnsignedInt(123456))]
    );
}
#[test]
fn lex_signed_int() {
    let number = v!("-123456");
    assert_eq!(
        lex_insecure(&number).unwrap(),
        vec![Token::Lit(Lit::SignedInt(-123456))]
    );
}
#[test]
fn lex_bool() {
    let (t, f) = v!("true", "false");
    assert_eq!(lex_insecure(&t).unwrap(), vec![Token::Lit(Lit::Bool(true))]);
    assert_eq!(
        lex_insecure(&f).unwrap(),
        vec![Token::Lit(Lit::Bool(false))]
    );
}
#[test]
fn lex_string() {
    let s = br#" "hello, world" "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::Str("hello, world".into()))]
    );
    let s = br#" 'hello, world' "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::Str("hello, world".into()))]
    );
}
#[test]
fn lex_string_test_escape_quote() {
    let s = br#" "\"hello world\"" "#; // == "hello world"
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::Str("\"hello world\"".into()))]
    );
    let s = br#" '\'hello world\'' "#; // == 'hello world'
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::Str("'hello world'".into()))]
    );
}
#[test]
fn lex_string_use_different_quote_style() {
    let s = br#" "he's on it" "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::Str("he's on it".into()))]
    );
    let s = br#" 'he thinks that "that girl" fixed it' "#;
    assert_eq!(
        lex_insecure(s).unwrap(),
        vec![Token::Lit(Lit::Str(
            "he thinks that \"that girl\" fixed it".into()
        ))]
    )
}
#[test]
fn lex_string_escape_bs() {
    let s = v!(r#" "windows has c:\\" "#);
    assert_eq!(
        lex_insecure(&s).unwrap(),
        vec![Token::Lit(Lit::Str("windows has c:\\".into()))]
    );
    let s = v!(r#" 'windows has c:\\' "#);
    assert_eq!(
        lex_insecure(&s).unwrap(),
        vec![Token::Lit(Lit::Str("windows has c:\\".into()))]
    );
    let lol = v!(r#"'\\\\\\\\\\'"#);
    assert_eq!(
        lex_insecure(&lol).unwrap(),
        vec![Token::Lit(Lit::Str("\\".repeat(5).into_boxed_str()))],
        "lol"
    )
}
#[test]
fn lex_string_bad_escape() {
    let wth = br#" '\a should be an alert on windows apparently' "#;
    assert_eq!(
        lex_insecure(wth).unwrap_err(),
        LangError::InvalidStringLiteral
    );
}
#[test]
fn lex_string_unclosed() {
    let wth = br#" 'omg where did the end go "#;
    assert_eq!(
        lex_insecure(wth).unwrap_err(),
        LangError::InvalidStringLiteral
    );
    let wth = br#" 'see, we escaped the end\' "#;
    assert_eq!(
        lex_insecure(wth).unwrap_err(),
        LangError::InvalidStringLiteral
    );
}
#[test]
fn lex_unsafe_literal_mini() {
    let usl = lex_insecure("\r0\n".as_bytes()).unwrap();
    assert_eq!(usl.len(), 1);
    assert_eq!(Token::Lit(Lit::Bin("".into())), usl[0]);
}
#[test]
fn lex_unsafe_literal() {
    let usl = lex_insecure("\r9\nabcdefghi".as_bytes()).unwrap();
    assert_eq!(usl.len(), 1);
    assert_eq!(Token::Lit(Lit::Bin("abcdefghi".into())), usl[0]);
}
#[test]
fn lex_unsafe_literal_pro() {
    let usl = lex_insecure("\r18\nabcdefghi123456789".as_bytes()).unwrap();
    assert_eq!(usl.len(), 1);
    assert_eq!(Token::Lit(Lit::Bin("abcdefghi123456789".into())), usl[0]);
}

mod num_tests {
    use crate::engine::ql::lexer::decode_num_ub as ubdc;
    mod uint8 {
        use super::*;
        #[test]
        fn ndecub_u8_ok() {
            const SRC: &[u8] = b"123\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<u8>(SRC, &mut b, &mut i);
            assert!(b);
            assert_eq!(i, SRC.len());
            assert_eq!(x, 123);
        }
        #[test]
        fn ndecub_u8_lb() {
            const SRC: &[u8] = b"0\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<u8>(SRC, &mut b, &mut i);
            assert!(b);
            assert_eq!(i, SRC.len());
            assert_eq!(x, 0);
        }
        #[test]
        fn ndecub_u8_ub() {
            const SRC: &[u8] = b"255\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<u8>(SRC, &mut b, &mut i);
            assert!(b);
            assert_eq!(i, SRC.len());
            assert_eq!(x, 255);
        }
        #[test]
        fn ndecub_u8_ub_of() {
            const SRC: &[u8] = b"256\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<u8>(SRC, &mut b, &mut i);
            assert!(!b);
            assert_eq!(i, 2);
            assert_eq!(x, 0);
        }
    }
    mod sint8 {
        use super::*;
        #[test]
        pub(crate) fn ndecub_i8_ok() {
            const SRC: &[u8] = b"-123\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<i8>(SRC, &mut b, &mut i);
            assert!(b);
            assert_eq!(i, SRC.len());
            assert_eq!(x, -123);
        }
        #[test]
        pub(crate) fn ndecub_i8_lb() {
            const SRC: &[u8] = b"-128\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<i8>(SRC, &mut b, &mut i);
            assert!(b);
            assert_eq!(i, SRC.len());
            assert_eq!(x, -128);
        }

        #[test]
        pub(crate) fn ndecub_i8_lb_of() {
            const SRC: &[u8] = b"-129\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<i8>(SRC, &mut b, &mut i);
            assert!(!b);
            assert_eq!(i, 3);
            assert_eq!(x, 0);
        }
        #[test]
        pub(crate) fn ndecub_i8_ub() {
            const SRC: &[u8] = b"127\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<i8>(SRC, &mut b, &mut i);
            assert!(b);
            assert_eq!(i, SRC.len());
            assert_eq!(x, 127);
        }
        #[test]
        pub(crate) fn ndecub_i8_ub_of() {
            const SRC: &[u8] = b"128\n";
            let mut i = 0;
            let mut b = true;
            let x = ubdc::<i8>(SRC, &mut b, &mut i);
            assert!(!b);
            assert_eq!(i, 2);
            assert_eq!(x, 0);
        }
    }
}

mod safequery_params {
    use rand::seq::SliceRandom;

    use crate::engine::ql::lexer::{LitIR, SafeQueryData};
    #[test]
    fn param_uint() {
        let src = b"12345\n";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::uint(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::UInt(12345)]);
    }
    #[test]
    fn param_sint() {
        let src = b"-12345\n";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::sint(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::SInt(-12345)]);
    }
    #[test]
    fn param_bool_true() {
        let src = b"true\n";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::bool(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::Bool(true)]);
    }
    #[test]
    fn param_bool_false() {
        let src = b"false\n";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::bool(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::Bool(false)]);
    }
    #[test]
    fn param_float() {
        let src = b"4\n3.14";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::float(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::Float(3.14)]);
    }
    #[test]
    fn param_bin() {
        let src = b"5\nsayan";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::bin(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::Bin(b"sayan")]);
    }
    #[test]
    fn param_str() {
        let src = b"5\nsayan";
        let mut d = Vec::new();
        let mut i = 0;
        assert!(SafeQueryData::str(src, &mut i, &mut d));
        assert_eq!(i, src.len());
        assert_eq!(d, vec![LitIR::Str("sayan")]);
    }
    #[test]
    fn param_full_uint() {
        let src = b"\x0012345\n";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::UInt(12345)]);
    }
    #[test]
    fn param_full_sint() {
        let src = b"\x01-12345\n";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::SInt(-12345)]);
    }
    #[test]
    fn param_full_bool() {
        let src = b"\x02true\n";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::Bool(true)]);
        let src = b"\x02false\n";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::Bool(false)]);
    }
    #[test]
    fn param_full_float() {
        let src = b"\x034\n3.14";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::Float(3.14)]);
        let src = b"\x035\n-3.14";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::Float(-3.14)]);
    }
    #[test]
    fn param_full_bin() {
        let src = b"\x0412\nhello, world";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::Bin(b"hello, world")]);
    }
    #[test]
    fn param_full_str() {
        let src = b"\x0512\nhello, world";
        let r = SafeQueryData::p_revloop(src, 1).unwrap();
        assert_eq!(r.as_ref(), [LitIR::Str("hello, world")]);
    }
    #[test]
    fn params_mix() {
        let mut rng = rand::thread_rng();
        const DATA: [&[u8]; 6] = [
            b"\x0012345\n",
            b"\x01-12345\n",
            b"\x02true\n",
            b"\x0311\n12345.67890",
            b"\x0430\none two three four five binary",
            b"\x0527\none two three four five str",
        ];
        const RETMAP: [LitIR; 6] = [
            LitIR::UInt(12345),
            LitIR::SInt(-12345),
            LitIR::Bool(true),
            LitIR::Float(12345.67890),
            LitIR::Bin(b"one two three four five binary"),
            LitIR::Str("one two three four five str"),
        ];
        for _ in 0..DATA.len().pow(2) {
            let mut local_data = DATA;
            local_data.shuffle(&mut rng);
            let ret: Vec<LitIR> = local_data
                .iter()
                .map(|v| RETMAP[v[0] as usize].clone())
                .collect();
            let src: Vec<u8> = local_data.into_iter().flat_map(|v| v.to_owned()).collect();
            let r = SafeQueryData::p_revloop(&src, 6).unwrap();
            assert_eq!(r.as_ref(), ret);
        }
    }
}

mod safequery_full_param {
    use crate::engine::ql::lexer::{LitIR, SafeQueryData, Token};
    #[test]
    fn p_mini() {
        let query = b"select * from myapp where username = ?";
        let params = b"\x055\nsayan";
        let sq = SafeQueryData::parse(query, params, 1).unwrap();
        assert_eq!(
            sq,
            SafeQueryData::new_test(
                vec![LitIR::Str("sayan")].into_boxed_slice(),
                vec![
                    Token![select],
                    Token![*],
                    Token![from],
                    Token::Ident("myapp".into()),
                    Token![where],
                    Token::Ident("username".into()),
                    Token![=],
                    Token![?]
                ]
            )
        );
    }
    #[test]
    fn p() {
        let query = b"select * from myapp where username = ? and pass = ?";
        let params = b"\x055\nsayan\x048\npass1234";
        let sq = SafeQueryData::parse(query, params, 2).unwrap();
        assert_eq!(
            sq,
            SafeQueryData::new_test(
                vec![LitIR::Str("sayan"), LitIR::Bin(b"pass1234")].into_boxed_slice(),
                vec![
                    Token![select],
                    Token![*],
                    Token![from],
                    Token::Ident("myapp".into()),
                    Token![where],
                    Token::Ident("username".into()),
                    Token![=],
                    Token![?],
                    Token![and],
                    Token::Ident("pass".into()),
                    Token![=],
                    Token![?]
                ]
            )
        );
    }
    #[test]
    fn p_pro() {
        let query = b"select $notes[~?] from myapp where username = ? and pass = ?";
        let params = b"\x00100\n\x055\nsayan\x048\npass1234";
        let sq = SafeQueryData::parse(query, params, 3).unwrap();
        assert_eq!(
            sq,
            SafeQueryData::new_test(
                vec![
                    LitIR::UInt(100),
                    LitIR::Str("sayan"),
                    LitIR::Bin(b"pass1234")
                ]
                .into_boxed_slice(),
                vec![
                    Token![select],
                    Token![$],
                    Token::Ident("notes".into()),
                    Token![open []],
                    Token![~],
                    Token![?],
                    Token![close []],
                    Token![from],
                    Token::Ident("myapp".into()),
                    Token![where],
                    Token::Ident("username".into()),
                    Token![=],
                    Token![?],
                    Token![and],
                    Token::Ident("pass".into()),
                    Token![=],
                    Token![?]
                ]
            )
        );
    }
}
