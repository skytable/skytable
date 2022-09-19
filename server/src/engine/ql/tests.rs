/*
 * Created on Tue Sep 13 2022
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
        lexer::{Lexer, Token},
        LangResult,
    },
    crate::util::Life,
};

fn lex(src: &[u8]) -> LangResult<Life<Vec<Token>>> {
    Lexer::lex(src)
}

mod lexer_tests {
    use super::{
        super::lexer::{Lit, Token},
        lex,
    };
    use crate::engine::ql::LangError;

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
        assert_eq!(lex(&src).unwrap(), vec![Token::Ident("hello".into())]);
    }

    // literals
    #[test]
    fn lex_number() {
        let number = v!("123456");
        assert_eq!(lex(&number).unwrap(), vec![Token::Lit(Lit::Num(123456))]);
    }
    #[test]
    fn lex_bool() {
        let (t, f) = v!("true", "false");
        assert_eq!(lex(&t).unwrap(), vec![Token::Lit(Lit::Bool(true))]);
        assert_eq!(lex(&f).unwrap(), vec![Token::Lit(Lit::Bool(false))]);
    }
    #[test]
    fn lex_string() {
        let s = br#" "hello, world" "#;
        assert_eq!(
            lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("hello, world".into()))]
        );
        let s = br#" 'hello, world' "#;
        assert_eq!(
            lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("hello, world".into()))]
        );
    }
    #[test]
    fn lex_string_test_escape_quote() {
        let s = br#" "\"hello world\"" "#; // == "hello world"
        assert_eq!(
            lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("\"hello world\"".into()))]
        );
        let s = br#" '\'hello world\'' "#; // == 'hello world'
        assert_eq!(
            lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("'hello world'".into()))]
        );
    }
    #[test]
    fn lex_string_use_different_quote_style() {
        let s = br#" "he's on it" "#;
        assert_eq!(
            lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("he's on it".into()))]
        );
        let s = br#" 'he thinks that "that girl" fixed it' "#;
        assert_eq!(
            lex(s).unwrap(),
            vec![Token::Lit(Lit::Str(
                "he thinks that \"that girl\" fixed it".into()
            ))]
        )
    }
    #[test]
    fn lex_string_escape_bs() {
        let s = v!(r#" "windows has c:\\" "#);
        assert_eq!(
            lex(&s).unwrap(),
            vec![Token::Lit(Lit::Str("windows has c:\\".into()))]
        );
        let s = v!(r#" 'windows has c:\\' "#);
        assert_eq!(
            lex(&s).unwrap(),
            vec![Token::Lit(Lit::Str("windows has c:\\".into()))]
        );
        let lol = v!(r#"'\\\\\\\\\\'"#);
        assert_eq!(
            lex(&lol).unwrap(),
            vec![Token::Lit(Lit::Str("\\".repeat(5).into_boxed_str()))],
            "lol"
        )
    }
    #[test]
    fn lex_string_bad_escape() {
        let wth = br#" '\a should be an alert on windows apparently' "#;
        assert_eq!(lex(wth).unwrap_err(), LangError::InvalidStringLiteral);
    }
    #[test]
    fn lex_string_unclosed() {
        let wth = br#" 'omg where did the end go "#;
        assert_eq!(lex(wth).unwrap_err(), LangError::InvalidStringLiteral);
        let wth = br#" 'see, we escaped the end\' "#;
        assert_eq!(lex(wth).unwrap_err(), LangError::InvalidStringLiteral);
    }
}

mod schema_tests {
    use {
        super::{
            super::{
                lexer::{Lit, Symbol, Token},
                schema,
            },
            lex,
        },
        crate::util::test_utils,
        rand::{self, Rng},
    };

    /// A very "basic" fuzzer that will randomly inject tokens wherever applicable
    fn fuzz_tokens(src: &[Token], fuzzwith: impl Fn(bool, &[Token])) {
        static FUZZ_TARGETS: [Token; 2] = [Token::Symbol(Symbol::SymComma), Token::IgnorableComma];
        let mut rng = rand::thread_rng();
        #[inline(always)]
        fn inject(new_src: &mut Vec<Token>, rng: &mut impl Rng) -> usize {
            let start = new_src.len();
            (0..test_utils::random_number(0, 5, rng))
                .for_each(|_| new_src.push(Token::Symbol(Symbol::SymComma)));
            new_src.len() - start
        }
        let fuzz_amount = src.iter().filter(|tok| FUZZ_TARGETS.contains(tok)).count();
        for _ in 0..(fuzz_amount.pow(2)) {
            let mut new_src = Vec::with_capacity(src.len());
            let mut should_pass = true;
            src.iter().for_each(|tok| match tok {
                Token::IgnorableComma => {
                    let did_add = test_utils::random_bool(&mut rng);
                    if did_add {
                        new_src.push(Token::Symbol(Symbol::SymComma));
                    }
                    let added = inject(&mut new_src, &mut rng);
                    should_pass &= added <= !did_add as usize;
                }
                Token::Symbol(Symbol::SymComma) => {
                    let did_add = test_utils::random_bool(&mut rng);
                    if did_add {
                        new_src.push(Token::Symbol(Symbol::SymComma));
                    } else {
                        should_pass = false;
                    }
                    let added = inject(&mut new_src, &mut rng);
                    should_pass &= added == !did_add as usize;
                }
                tok => new_src.push(tok.clone()),
            });
            fuzzwith(should_pass, &new_src);
        }
    }

    mod dict {
        use super::*;

        macro_rules! fold_dict {
        ($($input:expr),* $(,)?) => {
            ($({schema::fold_dict(&super::lex($input).unwrap()).unwrap()}),*)
        }
    }

        #[test]
        fn dict_read_mini() {
            let (d1, d2) = fold_dict! {
                br#"{name: "sayan"}"#,
                br#"{name: "sayan",}"#,
            };
            let r = dict!("name" => Lit::Str("sayan".into()));
            multi_assert_eq!(d1, d2 => r);
        }
        #[test]
        fn dict_read() {
            let (d1, d2) = fold_dict! {
                br#"
                {
                    name: "sayan",
                    verified: true,
                    burgers: 152
                }
            "#,
                br#"
                {
                    name: "sayan",
                    verified: true,
                    burgers: 152,
                }
            "#,
            };
            let r = dict! (
                "name" => Lit::Str("sayan".into()),
                "verified" => Lit::Bool(true),
                "burgers" => Lit::Num(152),
            );
            multi_assert_eq!(d1, d2 => r);
        }
        #[test]
        fn dict_read_pro() {
            let (d1, d2, d3) = fold_dict! {
                br#"
                {
                    name: "sayan",
                    notes: {
                        burgers: "all the time, extra mayo",
                        taco: true,
                        pretzels: 1
                    }
                }
            "#,
                br#"
                {
                    name: "sayan",
                    notes: {
                        burgers: "all the time, extra mayo",
                        taco: true,
                        pretzels: 1,
                    }
                }
            "#,
                br#"
                {
                    name: "sayan",
                    notes: {
                        burgers: "all the time, extra mayo",
                        taco: true,
                        pretzels: 1,
                },
            }"#
            };
            multi_assert_eq!(
                d1, d2, d3 => dict! {
                    "name" => Lit::Str("sayan".into()),
                    "notes" => dict! {
                        "burgers" => Lit::Str("all the time, extra mayo".into()),
                        "taco" => Lit::Bool(true),
                        "pretzels" => Lit::Num(1),
                    }
                }
            );
        }

        #[test]
        fn dict_read_pro_max() {
            let (d1, d2, d3) = fold_dict! {
                br#"
                {
                    well: {
                        now: {
                            this: {
                                is: {
                                    ridiculous: true
                                }
                            }
                        }
                    }
                }
            "#,
                br#"
                {
                    well: {
                        now: {
                            this: {
                                is: {
                                    ridiculous: true,
                                }
                            }
                        }
                    }
                }
            "#,
                br#"
                {
                    well: {
                        now: {
                            this: {
                                is: {
                                    ridiculous: true,
                                },
                            },
                        },
                    },
                }
            }"#
            };
            multi_assert_eq!(
                d1, d2, d3 => dict! {
                    "well" => dict! {
                        "now" => dict! {
                            "this" => dict! {
                                "is" => dict! {
                                    "ridiculous" => Lit::Bool(true),
                                }
                            }
                        }
                    }
                }
            );
        }

        #[test]
        fn fuzz_dict() {
            let ret = lex(b"
                {
                    the_tradition_is: \"hello, world\",
                    could_have_been: {
                        this: true,
                        or_maybe_this: 100,
                        even_this: \"hello, universe!\"\r
                    },
                    but_oh_well: \"it continues to be the 'annoying' phrase\",
                    lorem: {
                        ipsum: {
                            dolor: \"sit amet\"\r
                        }\r
                    }\r
                }
            ")
            .unwrap();
            let ret_dict = dict! {
                "the_tradition_is" => Lit::Str("hello, world".into()),
                "could_have_been" => dict! {
                    "this" => Lit::Bool(true),
                    "or_maybe_this" => Lit::Num(100),
                    "even_this" => Lit::Str("hello, universe!".into()),
                },
                "but_oh_well" => Lit::Str("it continues to be the 'annoying' phrase".into()),
                "lorem" => dict! {
                    "ipsum" => dict! {
                        "dolor" => Lit::Str("sit amet".into())
                    }
                }
            };
            fuzz_tokens(&ret, |should_pass, new_src| {
                let r = schema::fold_dict(&new_src);
                if should_pass {
                    assert_eq!(r.unwrap(), ret_dict)
                } else if r.is_some() {
                    panic!(
                        "expected failure, but passed for token stream: `{:?}`",
                        new_src
                    );
                }
            });
        }
    }
    mod tymeta {
        use super::*;
        use crate::engine::ql::lexer::{Keyword, Type};
        #[test]
        fn tymeta_mini() {
            let tok = lex(b"}").unwrap();
            let (res, ret) = schema::fold_tymeta(&tok);
            assert!(res.is_okay());
            assert!(!res.has_more());
            assert_eq!(res.pos(), 1);
            assert_eq!(ret, dict!());
        }
        #[test]
        fn tymeta_mini_fail() {
            let tok = lex(b",}").unwrap();
            let (res, ret) = schema::fold_tymeta(&tok);
            assert!(!res.is_okay());
            assert!(!res.has_more());
            assert_eq!(res.pos(), 0);
            assert_eq!(ret, dict!());
        }
        #[test]
        fn tymeta() {
            let tok = lex(br#"hello: "world", loading: true, size: 100 }"#).unwrap();
            let (res, ret) = schema::fold_tymeta(&tok);
            assert!(res.is_okay());
            assert!(!res.has_more());
            assert_eq!(res.pos(), tok.len());
            assert_eq!(
                ret,
                dict! {
                    "hello" => Lit::Str("world".into()),
                    "loading" => Lit::Bool(true),
                    "size" => Lit::Num(100)
                }
            );
        }
        #[test]
        fn tymeta_pro() {
            // list { maxlen: 100, type string, unique: true }
            //        ^^^^^^^^^^^^^^^^^^ cursor should be at string
            let tok = lex(br#"maxlen: 100, type string, unique: true }"#).unwrap();
            let (res1, ret1) = schema::fold_tymeta(&tok);
            assert!(res1.is_okay());
            assert!(res1.has_more());
            assert_eq!(res1.pos(), 5);
            let remslice = &tok[res1.pos() + 2..];
            let (res2, ret2) = schema::fold_tymeta(remslice);
            assert!(res2.is_okay());
            assert!(!res2.has_more());
            assert_eq!(res2.pos() + res1.pos() + 2, tok.len());
            let mut final_ret = ret1;
            final_ret.extend(ret2);
            assert_eq!(
                final_ret,
                dict! {
                    "maxlen" => Lit::Num(100),
                    "unique" => Lit::Bool(true)
                }
            )
        }
        #[test]
        fn tymeta_pro_max() {
            // list { maxlen: 100, this: { is: "cool" }, type string, unique: true }
            //        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ cursor should be at string
            let tok =
                lex(br#"maxlen: 100, this: { is: "cool" }, type string, unique: true }"#).unwrap();
            let (res1, ret1) = schema::fold_tymeta(&tok);
            assert!(res1.is_okay());
            assert!(res1.has_more());
            assert_eq!(res1.pos(), 13);
            let remslice = &tok[res1.pos() + 2..];
            let (res2, ret2) = schema::fold_tymeta(remslice);
            assert!(res2.is_okay());
            assert!(!res2.has_more());
            assert_eq!(res2.pos() + res1.pos() + 2, tok.len());
            let mut final_ret = ret1;
            final_ret.extend(ret2);
            assert_eq!(
                final_ret,
                dict! {
                    "maxlen" => Lit::Num(100),
                    "unique" => Lit::Bool(true),
                    "this" => dict! {
                        "is" => Lit::Str("cool".into())
                    }
                }
            )
        }
        #[test]
        fn fuzz_tymeta_normal() {
            // { maxlen: 10, unique: true, user: "sayan" }
            //   ^start
            let tok = lex(b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\r
                    },
                    user: \"sayan\"\r
                }
            ")
            .unwrap();
            let expected = dict! {
                "maxlen" => Lit::Num(10),
                "unique" => Lit::Bool(true),
                "auth" => dict! {
                    "maybe" => Lit::Bool(true),
                },
                "user" => Lit::Str("sayan".into())
            };
            fuzz_tokens(&tok, |should_pass, new_src| {
                let (ret, dict) = schema::fold_tymeta(&tok);
                if should_pass {
                    assert!(ret.is_okay());
                    assert!(!ret.has_more());
                    assert_eq!(ret.pos(), new_src.len());
                    assert_eq!(dict, expected);
                } else if ret.is_okay() {
                    panic!(
                        "Expected failure but passed for token stream: `{:?}`",
                        new_src
                    );
                }
            });
        }
        #[test]
        fn fuzz_tymeta_with_ty() {
            // list { maxlen: 10, unique: true, type string, user: "sayan" }
            //   ^start
            let tok = lex(b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\r
                    },
                    type string,
                    user: \"sayan\"\r
                }
            ")
            .unwrap();
            let expected = dict! {
                "maxlen" => Lit::Num(10),
                "unique" => Lit::Bool(true),
                "auth" => dict! {
                    "maybe" => Lit::Bool(true),
                },
            };
            fuzz_tokens(&tok, |should_pass, new_src| {
                let (ret, dict) = schema::fold_tymeta(&tok);
                if should_pass {
                    assert!(ret.is_okay());
                    assert!(ret.has_more());
                    assert!(new_src[ret.pos()] == Token::Keyword(Keyword::TypeId(Type::String)));
                    assert_eq!(dict, expected);
                } else if ret.is_okay() {
                    panic!("Expected failure but passed for token stream: `{:?}`", tok);
                }
            });
        }
    }
    mod layer {
        use super::*;
        use crate::engine::ql::{lexer::Type, schema::Layer};
        #[test]
        fn layer_mini() {
            let tok = lex(b"string)").unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len() - 1);
            assert!(okay);
            assert_eq!(layers, vec![Layer::new(Type::String, dict! {})]);
        }
        #[test]
        fn layer() {
            let tok = lex(b"string { maxlen: 100 }").unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len());
            assert!(okay);
            assert_eq!(
                layers,
                vec![Layer::new(
                    Type::String,
                    dict! {
                        "maxlen" => Lit::Num(100)
                    }
                )]
            );
        }
        #[test]
        fn layer_plus() {
            let tok = lex(b"list { type string }").unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len());
            assert!(okay);
            assert_eq!(
                layers,
                vec![
                    Layer::new(Type::String, dict! {}),
                    Layer::new(Type::List, dict! {})
                ]
            );
        }
        #[test]
        fn layer_pro() {
            let tok = lex(b"list { unique: true, type string, maxlen: 10 }").unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len());
            assert!(okay);
            assert_eq!(
                layers,
                vec![
                    Layer::new(Type::String, dict! {}),
                    Layer::new(
                        Type::List,
                        dict! {
                            "unique" => Lit::Bool(true),
                            "maxlen" => Lit::Num(10),
                        }
                    )
                ]
            );
        }
        #[test]
        fn layer_pro_max() {
            let tok = lex(
                b"list { unique: true, type string { ascii_only: true, maxlen: 255 }, maxlen: 10 }",
            )
            .unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len());
            assert!(okay);
            assert_eq!(
                layers,
                vec![
                    Layer::new(
                        Type::String,
                        dict! {
                            "ascii_only" => Lit::Bool(true),
                            "maxlen" => Lit::Num(255)
                        }
                    ),
                    Layer::new(
                        Type::List,
                        dict! {
                            "unique" => Lit::Bool(true),
                            "maxlen" => Lit::Num(10),
                        }
                    )
                ]
            );
        }

        #[test]
        fn fuzz_layer() {
            let tok = lex(b"
            list {
                type list {
                    maxlen: 100,
                    type string\r
                },
                unique: true\r
            }
        ")
            .unwrap();
            let expected = vec![
                Layer::new(Type::String, dict!()),
                Layer::new(
                    Type::List,
                    dict! {
                        "maxlen" => Lit::Num(100),
                    },
                ),
                Layer::new(Type::List, dict!("unique" => Lit::Bool(true))),
            ];
            fuzz_tokens(&tok, |should_pass, new_tok| {
                let (layers, c, okay) = schema::fold_layers(&new_tok);
                if should_pass {
                    assert!(okay);
                    assert_eq!(c, new_tok.len());
                    assert_eq!(layers, expected);
                } else if okay {
                    panic!(
                        "expected failure but passed for token stream: `{:?}`",
                        new_tok
                    );
                }
            });
        }
    }
    mod field_properties {
        use {super::*, crate::engine::ql::schema::FieldProperties};

        #[test]
        fn field_properties_empty() {
            let tok = lex(b"myfield:").unwrap();
            let (props, c, okay) = schema::parse_field_properties(&tok);
            assert!(okay);
            assert_eq!(c, 0);
            assert_eq!(props, FieldProperties::default());
        }
        #[test]
        fn field_properties_full() {
            let tok = lex(b"primary null myfield:").unwrap();
            let (props, c, okay) = schema::parse_field_properties(&tok);
            assert_eq!(c, 2);
            assert_eq!(tok[c], Token::Ident("myfield".into()));
            assert!(okay);
            assert_eq!(
                props,
                FieldProperties {
                    properties: set!["primary", "null"],
                }
            )
        }
    }
    mod fields {
        use {
            super::*,
            crate::engine::ql::{
                lexer::Type,
                schema::{Field, Layer},
            },
        };
        #[test]
        fn field_mini() {
            let tok = lex(b"
                username: string,
            ")
            .unwrap();
            let (c, f) = schema::parse_field(&tok).unwrap();
            assert_eq!(c, tok.len() - 1);
            assert_eq!(
                f,
                Field {
                    field_name: "username".into(),
                    layers: [Layer::new(Type::String, dict! {})].into(),
                    props: set![],
                }
            )
        }
        #[test]
        fn field() {
            let tok = lex(b"
                primary username: string,    
            ")
            .unwrap();
            let (c, f) = schema::parse_field(&tok).unwrap();
            assert_eq!(c, tok.len() - 1);
            assert_eq!(
                f,
                Field {
                    field_name: "username".into(),
                    layers: [Layer::new(Type::String, dict! {})].into(),
                    props: set!["primary"],
                }
            )
        }
        #[test]
        fn field_pro() {
            let tok = lex(b"
                primary username: string {
                    maxlen: 10,
                    ascii_only: true,
                }
            ")
            .unwrap();
            let (c, f) = schema::parse_field(&tok).unwrap();
            assert_eq!(c, tok.len());
            assert_eq!(
                f,
                Field {
                    field_name: "username".into(),
                    layers: [Layer::new(
                        Type::String,
                        dict! {
                            "maxlen" => Lit::Num(10),
                            "ascii_only" => Lit::Bool(true),
                        }
                    )]
                    .into(),
                    props: set!["primary"],
                }
            )
        }
        #[test]
        fn field_pro_max() {
            let tok = lex(b"
                null notes: list {
                    type string {
                        maxlen: 255,
                        ascii_only: true,
                    },
                    unique: true,
                }
            ")
            .unwrap();
            let (c, f) = schema::parse_field(&tok).unwrap();
            assert_eq!(c, tok.len());
            assert_eq!(
                f,
                Field {
                    field_name: "notes".into(),
                    layers: [
                        Layer::new(
                            Type::String,
                            dict! {
                                "maxlen" => Lit::Num(255),
                                "ascii_only" => Lit::Bool(true),
                            }
                        ),
                        Layer::new(
                            Type::List,
                            dict! {
                                "unique" => Lit::Bool(true)
                            }
                        ),
                    ]
                    .into(),
                    props: set!["null"],
                }
            )
        }
    }
}
