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
        lexer::{InsecureLexer, Symbol, Token},
        LangResult,
    },
    crate::{
        engine::memory::DataType,
        util::{test_utils, Life},
    },
    rand::{self, Rng},
};

pub(super) fn lex(src: &[u8]) -> LangResult<Life<Vec<Token>>> {
    InsecureLexer::lex(src)
}

pub trait NullableData<T> {
    fn data(self) -> Option<T>;
}

impl<T> NullableData<DataType> for T
where
    T: Into<DataType>,
{
    fn data(self) -> Option<DataType> {
        Some(self.into())
    }
}

struct Null;

impl NullableData<DataType> for Null {
    fn data(self) -> Option<DataType> {
        None
    }
}

fn nullable_datatype(v: impl NullableData<DataType>) -> Option<DataType> {
    v.data()
}

pub trait NullableMapEntry {
    fn data(self) -> Option<super::schema::DictEntry>;
}

impl NullableMapEntry for Null {
    fn data(self) -> Option<super::schema::DictEntry> {
        None
    }
}

impl NullableMapEntry for super::lexer::Lit {
    fn data(self) -> Option<super::schema::DictEntry> {
        Some(super::schema::DictEntry::Lit(self))
    }
}

impl NullableMapEntry for super::schema::Dict {
    fn data(self) -> Option<super::schema::DictEntry> {
        Some(super::schema::DictEntry::Map(self))
    }
}

macro_rules! fold_dict {
    ($($input:expr),* $(,)?) => {
        ($({$crate::engine::ql::schema::fold_dict(&super::lex($input).unwrap()).unwrap()}),*)
    }
}

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
                let added = inject(&mut new_src, &mut rng);
                should_pass &= added <= 1;
            }
            Token::Symbol(Symbol::SymComma) => {
                let added = inject(&mut new_src, &mut rng);
                should_pass &= added == 1;
            }
            tok => new_src.push(tok.clone()),
        });
        assert!(
            new_src.iter().all(|tok| tok != &Token::IgnorableComma),
            "found ignorable comma in rectified source"
        );
        fuzzwith(should_pass, &new_src);
    }
}

mod lexer_tests {
    use {
        super::{
            super::lexer::{Lit, Token},
            lex,
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
        assert_eq!(lex(&src).unwrap(), vec![Token::Ident("hello".into())]);
    }

    // literals
    #[test]
    fn lex_unsigned_int() {
        let number = v!("123456");
        assert_eq!(
            lex(&number).unwrap(),
            vec![Token::Lit(Lit::UnsignedInt(123456))]
        );
    }
    #[test]
    fn lex_signed_int() {
        let number = v!("-123456");
        assert_eq!(
            lex(&number).unwrap(),
            vec![Token::Lit(Lit::SignedInt(-123456))]
        );
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
    #[test]
    fn lex_unsafe_literal_mini() {
        let usl = lex("\r0\n".as_bytes()).unwrap();
        assert_eq!(usl.len(), 1);
        assert_eq!(Token::Lit(Lit::UnsafeLit("".into())), usl[0]);
    }
    #[test]
    fn lex_unsafe_literal() {
        let usl = lex("\r9\nabcdefghi".as_bytes()).unwrap();
        assert_eq!(usl.len(), 1);
        assert_eq!(Token::Lit(Lit::UnsafeLit("abcdefghi".into())), usl[0]);
    }
    #[test]
    fn lex_unsafe_literal_pro() {
        let usl = lex("\r18\nabcdefghi123456789".as_bytes()).unwrap();
        assert_eq!(usl.len(), 1);
        assert_eq!(
            Token::Lit(Lit::UnsafeLit("abcdefghi123456789".into())),
            usl[0]
        );
    }
}

mod entity {
    use super::*;
    use crate::engine::ql::ast::{Compiler, Entity};
    #[test]
    fn entity_current() {
        let t = lex(b"hello").unwrap();
        let mut c = Compiler::new(&t);
        let r = Entity::parse(&mut c).unwrap();
        assert_eq!(r, Entity::Single("hello".into()))
    }
    #[test]
    fn entity_partial() {
        let t = lex(b":hello").unwrap();
        let mut c = Compiler::new(&t);
        let r = Entity::parse(&mut c).unwrap();
        assert_eq!(r, Entity::Partial("hello".into()))
    }
    #[test]
    fn entity_full() {
        let t = lex(b"hello.world").unwrap();
        let mut c = Compiler::new(&t);
        let r = Entity::parse(&mut c).unwrap();
        assert_eq!(r, Entity::Full("hello".into(), "world".into()))
    }
}

mod ddl_other_query_tests {
    use {
        super::*,
        crate::engine::ql::{
            ast::{Entity, Statement},
            ddl::{self, DropModel, DropSpace},
        },
    };
    #[test]
    fn drop_space() {
        let src = lex(br"drop space myspace").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new("myspace".into(), false))
        );
    }
    #[test]
    fn drop_space_force() {
        let src = lex(br"drop space myspace force").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new("myspace".into(), true))
        );
    }
    #[test]
    fn drop_model() {
        let src = lex(br"drop model mymodel").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single("mymodel".into()), false))
        );
    }
    #[test]
    fn drop_model_force() {
        let src = lex(br"drop model mymodel force").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single("mymodel".into()), true))
        );
    }
}

mod dict_tests {
    use {
        super::*,
        crate::engine::ql::{lexer::Lit, schema},
    };
    mod dict {
        use super::*;

        #[test]
        fn dict_read_mini() {
            let (d1, d2) = fold_dict! {
                br#"{name: "sayan"}"#,
                br#"{name: "sayan",}"#,
            };
            let r = nullable_dict!("name" => Lit::Str("sayan".into()));
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
            let r = nullable_dict! (
                "name" => Lit::Str("sayan".into()),
                "verified" => Lit::Bool(true),
                "burgers" => Lit::UnsignedInt(152),
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
                d1, d2, d3 => nullable_dict! {
                    "name" => Lit::Str("sayan".into()),
                    "notes" => nullable_dict! {
                        "burgers" => Lit::Str("all the time, extra mayo".into()),
                        "taco" => Lit::Bool(true),
                        "pretzels" => Lit::UnsignedInt(1),
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
                d1, d2, d3 => nullable_dict! {
                    "well" => nullable_dict! {
                        "now" => nullable_dict! {
                            "this" => nullable_dict! {
                                "is" => nullable_dict! {
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
                        even_this: \"hello, universe!\"\x01
                    },
                    but_oh_well: \"it continues to be the 'annoying' phrase\",
                    lorem: {
                        ipsum: {
                            dolor: \"sit amet\"\x01
                        }\x01
                    }\x01
                }
            ")
            .unwrap();
            let ret_dict = nullable_dict! {
                "the_tradition_is" => Lit::Str("hello, world".into()),
                "could_have_been" => nullable_dict! {
                    "this" => Lit::Bool(true),
                    "or_maybe_this" => Lit::UnsignedInt(100),
                    "even_this" => Lit::Str("hello, universe!".into()),
                },
                "but_oh_well" => Lit::Str("it continues to be the 'annoying' phrase".into()),
                "lorem" => nullable_dict! {
                    "ipsum" => nullable_dict! {
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
    mod nullable_dict_tests {
        use super::*;
        mod dict {
            use {super::*, crate::engine::ql::lexer::Lit};

            #[test]
            fn null_mini() {
                let d = fold_dict!(br"{ x: null }");
                assert_eq!(
                    d,
                    nullable_dict! {
                        "x" => Null,
                    }
                );
            }
            #[test]
            fn null() {
                let d = fold_dict! {
                    br#"
                        {
                            this_is_non_null: "hello",
                            but_this_is_null: null,
                        }
                    "#
                };
                assert_eq!(
                    d,
                    nullable_dict! {
                        "this_is_non_null" => Lit::Str("hello".into()),
                        "but_this_is_null" => Null,
                    }
                )
            }
            #[test]
            fn null_pro() {
                let d = fold_dict! {
                    br#"
                        {
                            a_string: "this is a string",
                            num: 1234,
                            a_dict: {
                                a_null: null,
                            }
                        }
                    "#
                };
                assert_eq!(
                    d,
                    nullable_dict! {
                        "a_string" => Lit::Str("this is a string".into()),
                        "num" => Lit::UnsignedInt(1234),
                        "a_dict" => nullable_dict! {
                            "a_null" => Null,
                        }
                    }
                )
            }
            #[test]
            fn null_pro_max() {
                let d = fold_dict! {
                    br#"
                        {
                            a_string: "this is a string",
                            num: 1234,
                            a_dict: {
                                a_null: null,
                            },
                            another_null: null,
                        }
                    "#
                };
                assert_eq!(
                    d,
                    nullable_dict! {
                        "a_string" => Lit::Str("this is a string".into()),
                        "num" => Lit::UnsignedInt(1234),
                        "a_dict" => nullable_dict! {
                            "a_null" => Null,
                        },
                        "another_null" => Null,
                    }
                )
            }
        }
        // TODO(@ohsayan): Add null tests
    }
}

mod schema_tests {
    use super::{
        super::{
            lexer::{Lit, Token},
            schema,
        },
        lex, *,
    };
    mod inspect {
        use {
            super::*,
            crate::engine::ql::{
                ast::{Entity, Statement},
                ddl,
            },
        };
        #[test]
        fn inspect_space() {
            let tok = lex(b"inspect space myspace").unwrap();
            assert_eq!(
                ddl::parse_inspect_full(&tok[1..]).unwrap(),
                Statement::InspectSpace("myspace".into())
            );
        }
        #[test]
        fn inspect_model() {
            let tok = lex(b"inspect model users").unwrap();
            assert_eq!(
                ddl::parse_inspect_full(&tok[1..]).unwrap(),
                Statement::InspectModel(Entity::Single("users".into()))
            );
            let tok = lex(b"inspect model tweeter.users").unwrap();
            assert_eq!(
                ddl::parse_inspect_full(&tok[1..]).unwrap(),
                Statement::InspectModel(("tweeter", "users").into())
            );
        }
        #[test]
        fn inspect_spaces() {
            let tok = lex(b"inspect spaces").unwrap();
            assert_eq!(
                ddl::parse_inspect_full(&tok[1..]).unwrap(),
                Statement::InspectSpaces
            );
        }
    }

    mod alter_space {
        use {
            super::*,
            crate::engine::ql::{
                lexer::Lit,
                schema::{self, AlterSpace},
            },
        };
        #[test]
        fn alter_space_mini() {
            let tok = lex(b"alter model mymodel with {}").unwrap();
            let r = schema::alter_space_full(&tok[2..]).unwrap();
            assert_eq!(
                r,
                AlterSpace {
                    space_name: "mymodel".into(),
                    updated_props: nullable_dict! {}
                }
            );
        }
        #[test]
        fn alter_space() {
            let tok = lex(br#"
                alter model mymodel with {
                    max_entry: 1000,
                    driver: "ts-0.8"
                }
            "#)
            .unwrap();
            let r = schema::alter_space_full(&tok[2..]).unwrap();
            assert_eq!(
                r,
                AlterSpace {
                    space_name: "mymodel".into(),
                    updated_props: nullable_dict! {
                        "max_entry" => Lit::UnsignedInt(1000),
                        "driver" => Lit::Str("ts-0.8".into())
                    }
                }
            );
        }
    }
    mod tymeta {
        use super::*;
        #[test]
        fn tymeta_mini() {
            let tok = lex(b"}").unwrap();
            let (res, ret) = schema::fold_tymeta(&tok);
            assert!(res.is_okay());
            assert!(!res.has_more());
            assert_eq!(res.pos(), 1);
            assert_eq!(ret, nullable_dict!());
        }
        #[test]
        fn tymeta_mini_fail() {
            let tok = lex(b",}").unwrap();
            let (res, ret) = schema::fold_tymeta(&tok);
            assert!(!res.is_okay());
            assert!(!res.has_more());
            assert_eq!(res.pos(), 0);
            assert_eq!(ret, nullable_dict!());
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
                nullable_dict! {
                    "hello" => Lit::Str("world".into()),
                    "loading" => Lit::Bool(true),
                    "size" => Lit::UnsignedInt(100)
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
                nullable_dict! {
                    "maxlen" => Lit::UnsignedInt(100),
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
                nullable_dict! {
                    "maxlen" => Lit::UnsignedInt(100),
                    "unique" => Lit::Bool(true),
                    "this" => nullable_dict! {
                        "is" => Lit::Str("cool".into())
                    }
                }
            )
        }
        #[test]
        fn fuzz_tymeta_normal() {
            // { maxlen: 10, unique: true, users: "sayan" }
            //   ^start
            let tok = lex(b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\x01
                    },
                    users: \"sayan\"\x01
                }
            ")
            .unwrap();
            let expected = nullable_dict! {
                "maxlen" => Lit::UnsignedInt(10),
                "unique" => Lit::Bool(true),
                "auth" => nullable_dict! {
                    "maybe" => Lit::Bool(true),
                },
                "users" => Lit::Str("sayan".into())
            };
            fuzz_tokens(&tok, |should_pass, new_src| {
                let (ret, dict) = schema::fold_tymeta(&new_src);
                if should_pass {
                    assert!(ret.is_okay(), "{:?}", &new_src);
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
            // list { maxlen: 10, unique: true, type string, users: "sayan" }
            //   ^start
            let tok = lex(b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\x01
                    },
                    type string,
                    users: \"sayan\"\x01
                }
            ")
            .unwrap();
            let expected = nullable_dict! {
                "maxlen" => Lit::UnsignedInt(10),
                "unique" => Lit::Bool(true),
                "auth" => nullable_dict! {
                    "maybe" => Lit::Bool(true),
                },
            };
            fuzz_tokens(&tok, |should_pass, new_src| {
                let (ret, dict) = schema::fold_tymeta(&new_src);
                if should_pass {
                    assert!(ret.is_okay());
                    assert!(ret.has_more());
                    assert!(new_src[ret.pos()] == Token::Ident("string".into()));
                    assert_eq!(dict, expected);
                } else if ret.is_okay() {
                    panic!("Expected failure but passed for token stream: `{:?}`", tok);
                }
            });
        }
    }
    mod layer {
        use super::*;
        use crate::engine::ql::schema::Layer;
        #[test]
        fn layer_mini() {
            let tok = lex(b"string)").unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len() - 1);
            assert!(okay);
            assert_eq!(
                layers,
                vec![Layer::new_noreset("string".into(), nullable_dict! {})]
            );
        }
        #[test]
        fn layer() {
            let tok = lex(b"string { maxlen: 100 }").unwrap();
            let (layers, c, okay) = schema::fold_layers(&tok);
            assert_eq!(c, tok.len());
            assert!(okay);
            assert_eq!(
                layers,
                vec![Layer::new_noreset(
                    "string".into(),
                    nullable_dict! {
                        "maxlen" => Lit::UnsignedInt(100)
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
                    Layer::new_noreset("string".into(), nullable_dict! {}),
                    Layer::new_noreset("list".into(), nullable_dict! {})
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
                    Layer::new_noreset("string".into(), nullable_dict! {}),
                    Layer::new_noreset(
                        "list".into(),
                        nullable_dict! {
                            "unique" => Lit::Bool(true),
                            "maxlen" => Lit::UnsignedInt(10),
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
                    Layer::new_noreset(
                        "string".into(),
                        nullable_dict! {
                            "ascii_only" => Lit::Bool(true),
                            "maxlen" => Lit::UnsignedInt(255)
                        }
                    ),
                    Layer::new_noreset(
                        "list".into(),
                        nullable_dict! {
                            "unique" => Lit::Bool(true),
                            "maxlen" => Lit::UnsignedInt(10),
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
                    type string\x01
                },
                unique: true\x01
            }
        ")
            .unwrap();
            let expected = vec![
                Layer::new_noreset("string".into(), nullable_dict!()),
                Layer::new_noreset(
                    "list".into(),
                    nullable_dict! {
                        "maxlen" => Lit::UnsignedInt(100),
                    },
                ),
                Layer::new_noreset("list".into(), nullable_dict!("unique" => Lit::Bool(true))),
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
            crate::engine::ql::schema::{Field, Layer},
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
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
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
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
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
                    layers: [Layer::new_noreset(
                        "string".into(),
                        nullable_dict! {
                            "maxlen" => Lit::UnsignedInt(10),
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
                        Layer::new_noreset(
                            "string".into(),
                            nullable_dict! {
                                "maxlen" => Lit::UnsignedInt(255),
                                "ascii_only" => Lit::Bool(true),
                            }
                        ),
                        Layer::new_noreset(
                            "list".into(),
                            nullable_dict! {
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
    mod schemas {
        use crate::engine::ql::schema::{Field, Layer, Model};

        use super::*;
        #[test]
        fn schema_mini() {
            let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                )
            ")
            .unwrap();
            let tok = &tok[2..];

            // parse model
            let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
            assert_eq!(c, tok.len());
            assert_eq!(
                model,
                Model {
                    model_name: "mymodel".into(),
                    fields: vec![
                        Field {
                            field_name: "username".into(),
                            layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                            props: set!["primary"]
                        },
                        Field {
                            field_name: "password".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set![]
                        }
                    ],
                    props: nullable_dict! {}
                }
            )
        }
        #[test]
        fn schema() {
            let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                )
            ")
            .unwrap();
            let tok = &tok[2..];

            // parse model
            let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
            assert_eq!(c, tok.len());
            assert_eq!(
                model,
                Model {
                    model_name: "mymodel".into(),
                    fields: vec![
                        Field {
                            field_name: "username".into(),
                            layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                            props: set!["primary"]
                        },
                        Field {
                            field_name: "password".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set![]
                        },
                        Field {
                            field_name: "profile_pic".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set!["null"]
                        }
                    ],
                    props: nullable_dict! {}
                }
            )
        }

        #[test]
        fn schema_pro() {
            let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                    null notes: list {
                        type string,
                        unique: true,
                    },
                )
            ")
            .unwrap();
            let tok = &tok[2..];

            // parse model
            let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
            assert_eq!(c, tok.len());
            assert_eq!(
                model,
                Model {
                    model_name: "mymodel".into(),
                    fields: vec![
                        Field {
                            field_name: "username".into(),
                            layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                            props: set!["primary"]
                        },
                        Field {
                            field_name: "password".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set![]
                        },
                        Field {
                            field_name: "profile_pic".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set!["null"]
                        },
                        Field {
                            field_name: "notes".into(),
                            layers: vec![
                                Layer::new_noreset("string".into(), nullable_dict! {}),
                                Layer::new_noreset(
                                    "list".into(),
                                    nullable_dict! {
                                        "unique" => Lit::Bool(true)
                                    }
                                )
                            ],
                            props: set!["null"]
                        }
                    ],
                    props: nullable_dict! {}
                }
            )
        }

        #[test]
        fn schema_pro_max() {
            let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                    null notes: list {
                        type string,
                        unique: true,
                    },
                ) with {
                    env: {
                        free_user_limit: 100,
                    },
                    storage_driver: \"skyheap\"
                }
            ")
            .unwrap();
            let tok = &tok[2..];

            // parse model
            let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
            assert_eq!(c, tok.len());
            assert_eq!(
                model,
                Model {
                    model_name: "mymodel".into(),
                    fields: vec![
                        Field {
                            field_name: "username".into(),
                            layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                            props: set!["primary"]
                        },
                        Field {
                            field_name: "password".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set![]
                        },
                        Field {
                            field_name: "profile_pic".into(),
                            layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                            props: set!["null"]
                        },
                        Field {
                            field_name: "notes".into(),
                            layers: vec![
                                Layer::new_noreset("string".into(), nullable_dict! {}),
                                Layer::new_noreset(
                                    "list".into(),
                                    nullable_dict! {
                                        "unique" => Lit::Bool(true)
                                    }
                                )
                            ],
                            props: set!["null"]
                        }
                    ],
                    props: nullable_dict! {
                        "env" => nullable_dict! {
                            "free_user_limit" => Lit::UnsignedInt(100),
                        },
                        "storage_driver" => Lit::Str("skyheap".into()),
                    }
                }
            )
        }
    }
    mod dict_field_syntax {
        use super::*;
        use crate::engine::ql::schema::{ExpandedField, Layer};
        #[test]
        fn field_syn_mini() {
            let tok = lex(b"username { type string }").unwrap();
            let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                ef,
                ExpandedField {
                    field_name: "username".into(),
                    layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                    props: nullable_dict! {},
                    reset: false
                }
            )
        }
        #[test]
        fn field_syn() {
            let tok = lex(b"
                username {
                    nullable: false,
                    type string,
                }
            ")
            .unwrap();
            let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                ef,
                ExpandedField {
                    field_name: "username".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(false),
                    },
                    layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                    reset: false
                }
            );
        }
        #[test]
        fn field_syn_pro() {
            let tok = lex(b"
                username {
                    nullable: false,
                    type string {
                        minlen: 6,
                        maxlen: 255,
                    },
                    jingle_bells: \"snow\"
                }
            ")
            .unwrap();
            let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                ef,
                ExpandedField {
                    field_name: "username".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(false),
                        "jingle_bells" => Lit::Str("snow".into()),
                    },
                    layers: vec![Layer::new_noreset(
                        "string".into(),
                        nullable_dict! {
                            "minlen" => Lit::UnsignedInt(6),
                            "maxlen" => Lit::UnsignedInt(255),
                        }
                    )],
                    reset: false
                }
            );
        }
        #[test]
        fn field_syn_pro_max() {
            let tok = lex(b"
                notes {
                    nullable: true,
                    type list {
                        type string {
                            ascii_only: true,
                        },
                        unique: true,
                    },
                    jingle_bells: \"snow\"
                }
            ")
            .unwrap();
            let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                ef,
                ExpandedField {
                    field_name: "notes".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true),
                        "jingle_bells" => Lit::Str("snow".into()),
                    },
                    layers: vec![
                        Layer::new_noreset(
                            "string".into(),
                            nullable_dict! {
                                "ascii_only" => Lit::Bool(true),
                            }
                        ),
                        Layer::new_noreset(
                            "list".into(),
                            nullable_dict! {
                                "unique" => Lit::Bool(true),
                            }
                        )
                    ],
                    reset: false
                }
            );
        }
    }
    mod alter_model_remove {
        use super::*;
        use crate::engine::ql::RawSlice;
        #[test]
        fn alter_mini() {
            let tok = lex(b"alter model mymodel remove myfield").unwrap();
            let mut i = 4;
            let remove = schema::alter_remove(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(remove, [RawSlice::from("myfield")].into());
        }
        #[test]
        fn alter_mini_2() {
            let tok = lex(b"alter model mymodel remove (myfield)").unwrap();
            let mut i = 4;
            let remove = schema::alter_remove(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(remove, [RawSlice::from("myfield")].into());
        }
        #[test]
        fn alter() {
            let tok = lex(b"alter model mymodel remove (myfield1, myfield2, myfield3, myfield4)")
                .unwrap();
            let mut i = 4;
            let remove = schema::alter_remove(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                remove,
                [
                    RawSlice::from("myfield1"),
                    RawSlice::from("myfield2"),
                    RawSlice::from("myfield3"),
                    RawSlice::from("myfield4")
                ]
                .into()
            );
        }
    }
    mod alter_model_add {
        use super::*;
        use crate::engine::ql::schema::{ExpandedField, Layer};
        #[test]
        fn add_mini() {
            let tok = lex(b"
                alter model mymodel add myfield { type string }
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_add(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {},
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: false
                }]
            );
        }
        #[test]
        fn add() {
            let tok = lex(b"
                alter model mymodel add myfield { type string, nullable: true }
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_add(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: false
                }]
            );
        }
        #[test]
        fn add_pro() {
            let tok = lex(b"
                alter model mymodel add (myfield { type string, nullable: true })
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_add(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: false
                }]
            );
        }
        #[test]
        fn add_pro_max() {
            let tok = lex(b"
                alter model mymodel add (
                    myfield {
                        type string,
                        nullable: true
                    },
                    another {
                        type list {
                            type string {
                                maxlen: 255
                            },
                            unique: true
                        },
                        nullable: false,
                    }
                )
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_add(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [
                    ExpandedField {
                        field_name: "myfield".into(),
                        props: nullable_dict! {
                            "nullable" => Lit::Bool(true)
                        },
                        layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                        reset: false
                    },
                    ExpandedField {
                        field_name: "another".into(),
                        props: nullable_dict! {
                            "nullable" => Lit::Bool(false)
                        },
                        layers: [
                            Layer::new_noreset(
                                "string".into(),
                                nullable_dict! {
                                    "maxlen" => Lit::UnsignedInt(255)
                                }
                            ),
                            Layer::new_noreset(
                                "list".into(),
                                nullable_dict! {
                                   "unique" => Lit::Bool(true)
                                },
                            )
                        ]
                        .into(),
                        reset: false
                    }
                ]
            );
        }
    }
    mod alter_model_update {
        use super::*;
        use crate::engine::ql::schema::{ExpandedField, Layer};

        #[test]
        fn alter_mini() {
            let tok = lex(b"
                alter model mymodel update myfield { type string, .. }
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_update(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {},
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: true
                }]
            );
        }
        #[test]
        fn alter_mini_2() {
            let tok = lex(b"
                alter model mymodel update (myfield { type string, .. })
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_update(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {},
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: true
                }]
            );
        }
        #[test]
        fn alter() {
            let tok = lex(b"
                alter model mymodel update (
                    myfield {
                        type string,
                        nullable: true,
                        ..
                    }
                )
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_update(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: true
                }]
            );
        }
        #[test]
        fn alter_pro() {
            let tok = lex(b"
                alter model mymodel update (
                    myfield {
                        type string,
                        nullable: true,
                        ..
                    },
                    myfield2 {
                        type string,
                        ..
                    }
                )
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_update(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [
                    ExpandedField {
                        field_name: "myfield".into(),
                        props: nullable_dict! {
                            "nullable" => Lit::Bool(true)
                        },
                        layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                        reset: true
                    },
                    ExpandedField {
                        field_name: "myfield2".into(),
                        props: nullable_dict! {},
                        layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                        reset: true
                    }
                ]
            );
        }
        #[test]
        fn alter_pro_max() {
            let tok = lex(b"
                alter model mymodel update (
                    myfield {
                        type string {..},
                        nullable: true,
                        ..
                    },
                    myfield2 {
                        type string {
                            maxlen: 255,
                            ..
                        },
                        ..
                    }
                )
            ")
            .unwrap();
            let mut i = 4;
            let r = schema::alter_update(&tok[i..], &mut i).unwrap();
            assert_eq!(i, tok.len());
            assert_eq!(
                r.as_ref(),
                [
                    ExpandedField {
                        field_name: "myfield".into(),
                        props: nullable_dict! {
                            "nullable" => Lit::Bool(true)
                        },
                        layers: [Layer::new_reset("string".into(), nullable_dict! {})].into(),
                        reset: true
                    },
                    ExpandedField {
                        field_name: "myfield2".into(),
                        props: nullable_dict! {},
                        layers: [Layer::new_reset(
                            "string".into(),
                            nullable_dict! {"maxlen" => Lit::UnsignedInt(255)}
                        )]
                        .into(),
                        reset: true
                    }
                ]
            );
        }
    }
}

mod dml_tests {
    use super::*;
    mod list_parse {
        use super::*;
        use crate::engine::ql::dml::parse_list_full;

        #[test]
        fn list_mini() {
            let tok = lex(b"
                []
            ")
            .unwrap();
            let r = parse_list_full(&tok[1..]).unwrap();
            assert_eq!(r, vec![])
        }

        #[test]
        fn list() {
            let tok = lex(b"
                [1, 2, 3, 4]
            ")
            .unwrap();
            let r = parse_list_full(&tok[1..]).unwrap();
            assert_eq!(r.as_slice(), into_array![1, 2, 3, 4])
        }

        #[test]
        fn list_pro() {
            let tok = lex(b"
                [
                    [1, 2],
                    [3, 4],
                    [5, 6],
                    []
                ]
            ")
            .unwrap();
            let r = parse_list_full(&tok[1..]).unwrap();
            assert_eq!(
                r.as_slice(),
                into_array![
                    into_array![1, 2],
                    into_array![3, 4],
                    into_array![5, 6],
                    into_array![]
                ]
            )
        }

        #[test]
        fn list_pro_max() {
            let tok = lex(b"
                [
                    [[1, 1], [2, 2]],
                    [[], [4, 4]],
                    [[5, 5], [6, 6]],
                    [[7, 7], []]
                ]
            ")
            .unwrap();
            let r = parse_list_full(&tok[1..]).unwrap();
            assert_eq!(
                r.as_slice(),
                into_array![
                    into_array![into_array![1, 1], into_array![2, 2]],
                    into_array![into_array![], into_array![4, 4]],
                    into_array![into_array![5, 5], into_array![6, 6]],
                    into_array![into_array![7, 7], into_array![]],
                ]
            )
        }
    }
    mod tuple_syntax {
        use super::*;
        use crate::engine::ql::dml::parse_data_tuple_syntax_full;

        #[test]
        fn tuple_mini() {
            let tok = lex(b"()").unwrap();
            let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
            assert_eq!(r, vec![]);
        }

        #[test]
        fn tuple() {
            let tok = lex(br#"
                (1234, "email@example.com", true)
            "#)
            .unwrap();
            let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
            assert_eq!(
                r.as_slice(),
                into_array_nullable![1234, "email@example.com", true]
            );
        }

        #[test]
        fn tuple_pro() {
            let tok = lex(br#"
                (
                    1234,
                    "email@example.com",
                    true,
                    ["hello", "world", "and", "the", "universe"]
                )
            "#)
            .unwrap();
            let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
            assert_eq!(
                r.as_slice(),
                into_array_nullable![
                    1234,
                    "email@example.com",
                    true,
                    into_array!["hello", "world", "and", "the", "universe"]
                ]
            );
        }

        #[test]
        fn tuple_pro_max() {
            let tok = lex(br#"
                (
                    1234,
                    "email@example.com",
                    true,
                    [
                        ["h", "hello"],
                        ["w", "world"],
                        ["a", "and"],
                        ["the"],
                        ["universe"],
                        []
                    ]
                )
            "#)
            .unwrap();
            let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
            assert_eq!(
                r.as_slice(),
                into_array_nullable![
                    1234,
                    "email@example.com",
                    true,
                    into_array![
                        into_array!["h", "hello"],
                        into_array!["w", "world"],
                        into_array!["a", "and"],
                        into_array!["the"],
                        into_array!["universe"],
                        into_array![],
                    ]
                ]
            );
        }
    }
    mod map_syntax {
        use super::*;
        use crate::engine::ql::dml::parse_data_map_syntax_full;

        #[test]
        fn map_mini() {
            let tok = lex(b"{}").unwrap();
            let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
            assert_eq!(r, nullable_dict! {})
        }

        #[test]
        fn map() {
            let tok = lex(br#"
                {
                    name: "John Appletree",
                    email: "john@example.com",
                    verified: false,
                    followers: 12345
                }
            "#)
            .unwrap();
            let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
            assert_eq!(
                r,
                dict_nullable! {
                    "name" => "John Appletree",
                    "email" => "john@example.com",
                    "verified" => false,
                    "followers" => 12345,
                }
            )
        }

        #[test]
        fn map_pro() {
            let tok = lex(br#"
                {
                    name: "John Appletree",
                    email: "john@example.com",
                    verified: false,
                    followers: 12345,
                    tweets_by_day: []
                }
            "#)
            .unwrap();
            let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
            assert_eq!(
                r,
                dict_nullable! {
                    "name" => "John Appletree",
                    "email" => "john@example.com",
                    "verified" => false,
                    "followers" => 12345,
                    "tweets_by_day" => []
                }
            )
        }

        #[test]
        fn map_pro_max() {
            let tok = lex(br#"
                {
                    name: "John Appletree",
                    email: "john@example.com",
                    verified: false,
                    followers: 12345,
                    tweets_by_day: [
                        ["it's a fresh monday", "monday was tiring"],
                        ["already bored with tuesday", "nope. gotta change stuff, life's getting boring"],
                        ["sunday, going to bed"]
                    ]
                }
            "#)
            .unwrap();
            let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
            assert_eq!(
                r,
                dict_nullable! {
                    "name" => "John Appletree",
                    "email" => "john@example.com",
                    "verified" => false,
                    "followers" => 12345,
                    "tweets_by_day" => into_array![
                        into_array![
                            "it's a fresh monday", "monday was tiring"
                        ],
                        into_array![
                            "already bored with tuesday", "nope. gotta change stuff, life's getting boring"
                        ],
                        into_array!["sunday, going to bed"]
                    ]
                }
            )
        }
    }
    mod stmt_insert {
        use {
            super::*,
            crate::engine::ql::{
                ast::Entity,
                dml::{self, InsertStatement},
            },
        };

        #[test]
        fn insert_tuple_mini() {
            let x = lex(br#"
                insert into twitter.users ("sayan")
            "#)
            .unwrap();
            let r = dml::parse_insert_full(&x[1..]).unwrap();
            let e = InsertStatement {
                entity: Entity::Full("twitter".into(), "users".into()),
                data: into_array_nullable!["sayan"].to_vec().into(),
            };
            assert_eq!(e, r);
        }
        #[test]
        fn insert_tuple() {
            let x = lex(br#"
                insert into twitter.users (
                    "sayan",
                    "Sayan",
                    "sayan@example.com",
                    true,
                    12345,
                    67890
                )
            "#)
            .unwrap();
            let r = dml::parse_insert_full(&x[1..]).unwrap();
            let e = InsertStatement {
                entity: Entity::Full("twitter".into(), "users".into()),
                data: into_array_nullable![
                    "sayan",
                    "Sayan",
                    "sayan@example.com",
                    true,
                    12345,
                    67890
                ]
                .to_vec()
                .into(),
            };
            assert_eq!(e, r);
        }
        #[test]
        fn insert_tuple_pro() {
            let x = lex(br#"
                insert into twitter.users (
                    "sayan",
                    "Sayan",
                    "sayan@example.com",
                    true,
                    12345,
                    67890,
                    null,
                    12345,
                    null
                )
            "#)
            .unwrap();
            let r = dml::parse_insert_full(&x[1..]).unwrap();
            let e = InsertStatement {
                entity: Entity::Full("twitter".into(), "users".into()),
                data: into_array_nullable![
                    "sayan",
                    "Sayan",
                    "sayan@example.com",
                    true,
                    12345,
                    67890,
                    Null,
                    12345,
                    Null
                ]
                .to_vec()
                .into(),
            };
            assert_eq!(e, r);
        }
        #[test]
        fn insert_map_mini() {
            let tok = lex(br#"
                insert into jotsy.app { username: "sayan" }
            "#)
            .unwrap();
            let r = dml::parse_insert_full(&tok[1..]).unwrap();
            let e = InsertStatement {
                entity: Entity::Full("jotsy".into(), "app".into()),
                data: dict_nullable! {
                    "username".as_bytes() => "sayan"
                }
                .into(),
            };
            assert_eq!(e, r);
        }
        #[test]
        fn insert_map() {
            let tok = lex(br#"
                insert into jotsy.app {
                    username: "sayan",
                    name: "Sayan",
                    email: "sayan@example.com",
                    verified: true,
                    following: 12345,
                    followers: 67890
                }
            "#)
            .unwrap();
            let r = dml::parse_insert_full(&tok[1..]).unwrap();
            let e = InsertStatement {
                entity: Entity::Full("jotsy".into(), "app".into()),
                data: dict_nullable! {
                    "username".as_bytes() => "sayan",
                    "name".as_bytes() => "Sayan",
                    "email".as_bytes() => "sayan@example.com",
                    "verified".as_bytes() => true,
                    "following".as_bytes() => 12345,
                    "followers".as_bytes() => 67890
                }
                .into(),
            };
            assert_eq!(e, r);
        }
        #[test]
        fn insert_map_pro() {
            let tok = lex(br#"
                insert into jotsy.app {
                    username: "sayan",
                    password: "pass123",
                    email: "sayan@example.com",
                    verified: true,
                    following: 12345,
                    followers: 67890,
                    linked_smart_devices: null,
                    bookmarks: 12345,
                    other_linked_accounts: null
                }
            "#)
            .unwrap();
            let r = dml::parse_insert_full(&tok[1..]).unwrap();
            let e = InsertStatement {
                entity: Entity::Full("jotsy".into(), "app".into()),
                data: dict_nullable! {
                    "username".as_bytes() => "sayan",
                    "password".as_bytes() => "pass123",
                    "email".as_bytes() => "sayan@example.com",
                    "verified".as_bytes() => true,
                    "following".as_bytes() => 12345,
                    "followers".as_bytes() => 67890,
                    "linked_smart_devices".as_bytes() => Null,
                    "bookmarks".as_bytes() => 12345,
                    "other_linked_accounts".as_bytes() => Null
                }
                .into(),
            };
            assert_eq!(r, e);
        }
    }

    mod stmt_select {
        use crate::engine::ql::dml::RelationalExpr;

        use {
            super::*,
            crate::engine::ql::{
                ast::Entity,
                dml::{self, SelectStatement},
            },
        };
        #[test]
        fn select_mini() {
            let tok = lex(br#"
                select * from users where username = "sayan"
            "#)
            .unwrap();
            let r = dml::parse_select_full(&tok[1..]).unwrap();
            let username_where = "sayan".into();
            let e = SelectStatement::new_test(
                Entity::Single("users".into()),
                [].to_vec(),
                true,
                dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(), &username_where, RelationalExpr::OP_EQ
                    ),
                },
            );
            assert_eq!(r, e);
        }
        #[test]
        fn select() {
            let tok = lex(br#"
                select field1 from users where username = "sayan"
            "#)
            .unwrap();
            let r = dml::parse_select_full(&tok[1..]).unwrap();
            let username_where = "sayan".into();
            let e = SelectStatement::new_test(
                Entity::Single("users".into()),
                ["field1".into()].to_vec(),
                false,
                dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(), &username_where, RelationalExpr::OP_EQ
                    ),
                },
            );
            assert_eq!(r, e);
        }
        #[test]
        fn select_pro() {
            let tok = lex(br#"
                select field1 from twitter.users where username = "sayan"
            "#)
            .unwrap();
            let r = dml::parse_select_full(&tok[1..]).unwrap();
            let username_where = "sayan".into();
            let e = SelectStatement::new_test(
                Entity::Full("twitter".into(), "users".into()),
                ["field1".into()].to_vec(),
                false,
                dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(), &username_where, RelationalExpr::OP_EQ
                    ),
                },
            );
            assert_eq!(r, e);
        }
        #[test]
        fn select_pro_max() {
            let tok = lex(br#"
                select field1, field2 from twitter.users where username = "sayan"
            "#)
            .unwrap();
            let r = dml::parse_select_full(&tok[1..]).unwrap();
            let username_where = "sayan".into();
            let e = SelectStatement::new_test(
                Entity::Full("twitter".into(), "users".into()),
                ["field1".into(), "field2".into()].to_vec(),
                false,
                dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(), &username_where, RelationalExpr::OP_EQ
                    ),
                },
            );
            assert_eq!(r, e);
        }
    }
    mod expression_tests {
        use {
            super::*,
            crate::engine::ql::{
                dml::{self, AssignmentExpression, Operator},
                lexer::Lit,
            },
        };
        #[test]
        fn expr_assign() {
            let src = lex(b"username = 'sayan'").unwrap();
            let r = dml::parse_expression_full(&src).unwrap();
            assert_eq!(
                r,
                AssignmentExpression {
                    lhs: "username".into(),
                    rhs: &Lit::Str("sayan".into()),
                    operator_fn: Operator::Assign
                }
            );
        }
        #[test]
        fn expr_add_assign() {
            let src = lex(b"followers += 100").unwrap();
            let r = dml::parse_expression_full(&src).unwrap();
            assert_eq!(
                r,
                AssignmentExpression {
                    lhs: "followers".into(),
                    rhs: &(100.into()),
                    operator_fn: Operator::AddAssign
                }
            );
        }
        #[test]
        fn expr_sub_assign() {
            let src = lex(b"following -= 150").unwrap();
            let r = dml::parse_expression_full(&src).unwrap();
            assert_eq!(
                r,
                AssignmentExpression {
                    lhs: "following".into(),
                    rhs: &(150.into()),
                    operator_fn: Operator::SubAssign
                }
            );
        }
        #[test]
        fn expr_mul_assign() {
            let src = lex(b"product_qty *= 2").unwrap();
            let r = dml::parse_expression_full(&src).unwrap();
            assert_eq!(
                r,
                AssignmentExpression {
                    lhs: "product_qty".into(),
                    rhs: &(2.into()),
                    operator_fn: Operator::MulAssign
                }
            );
        }
        #[test]
        fn expr_div_assign() {
            let src = lex(b"image_crop_factor /= 2").unwrap();
            let r = dml::parse_expression_full(&src).unwrap();
            assert_eq!(
                r,
                AssignmentExpression {
                    lhs: "image_crop_factor".into(),
                    rhs: &(2.into()),
                    operator_fn: Operator::DivAssign
                }
            );
        }
    }
    mod update_statement {
        use {
            super::*,
            crate::engine::ql::{
                ast::Entity,
                dml::{
                    self, AssignmentExpression, Operator, RelationalExpr, UpdateStatement,
                    WhereClause,
                },
            },
        };
        #[test]
        fn update_mini() {
            let tok = lex(br#"
                update app SET notes += "this is my new note" where username = "sayan"
            "#)
            .unwrap();
            let where_username = "sayan".into();
            let note = "this is my new note".to_string().into();
            let r = dml::parse_update_full(&tok[1..]).unwrap();
            let e = UpdateStatement {
                entity: Entity::Single("app".into()),
                expressions: vec![AssignmentExpression {
                    lhs: "notes".into(),
                    rhs: &note,
                    operator_fn: Operator::AddAssign,
                }],
                wc: WhereClause::new(dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(),
                        &where_username,
                        RelationalExpr::OP_EQ
                    )
                }),
            };
            assert_eq!(r, e);
        }
        #[test]
        fn update() {
            let tok = lex(br#"
                update
                    jotsy.app
                SET
                    notes += "this is my new note",
                    email = "sayan@example.com"
                WHERE
                    username = "sayan"
            "#)
            .unwrap();
            let r = dml::parse_update_full(&tok[1..]).unwrap();
            let where_username = "sayan".into();
            let field_note = "this is my new note".into();
            let field_email = "sayan@example.com".into();
            let e = UpdateStatement {
                entity: ("jotsy", "app").into(),
                expressions: vec![
                    AssignmentExpression::new("notes".into(), &field_note, Operator::AddAssign),
                    AssignmentExpression::new("email".into(), &field_email, Operator::Assign),
                ],
                wc: WhereClause::new(dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(),
                        &where_username,
                        RelationalExpr::OP_EQ
                    )
                }),
            };

            assert_eq!(r, e);
        }
    }
    mod delete_stmt {
        use {
            super::*,
            crate::engine::ql::{
                ast::Entity,
                dml::{self, DeleteStatement, RelationalExpr},
            },
        };

        #[test]
        fn delete_mini() {
            let tok = lex(br#"
                delete from users where username = "sayan"
            "#)
            .unwrap();
            let primary_key = "sayan".into();
            let e = DeleteStatement::new_test(
                Entity::Single("users".into()),
                dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(),
                        &primary_key,
                        RelationalExpr::OP_EQ
                    )
                },
            );
            let r = dml::parse_delete_full(&tok[1..]).unwrap();
            assert_eq!(r, e);
        }
        #[test]
        fn delete() {
            let tok = lex(br#"
                delete from twitter.users where username = "sayan"
            "#)
            .unwrap();
            let primary_key = "sayan".into();
            let e = DeleteStatement::new_test(
                ("twitter", "users").into(),
                dict! {
                    "username".as_bytes() => RelationalExpr::new(
                        "username".as_bytes(),
                        &primary_key,
                        RelationalExpr::OP_EQ
                    )
                },
            );
            let r = dml::parse_delete_full(&tok[1..]).unwrap();
            assert_eq!(r, e);
        }
    }
    mod relational_expr {
        use {
            super::*,
            crate::engine::ql::dml::{self, RelationalExpr},
        };

        #[test]
        fn expr_eq() {
            let expr = lex(b"primary_key = 10").unwrap();
            let r = dml::parse_relexpr_full(&expr).unwrap();
            assert_eq!(
                r,
                RelationalExpr {
                    rhs: &(10.into()),
                    lhs: "primary_key".as_bytes(),
                    opc: RelationalExpr::OP_EQ
                }
            );
        }
        #[test]
        fn expr_ne() {
            let expr = lex(b"primary_key != 10").unwrap();
            let r = dml::parse_relexpr_full(&expr).unwrap();
            assert_eq!(
                r,
                RelationalExpr {
                    rhs: &(10.into()),
                    lhs: "primary_key".as_bytes(),
                    opc: RelationalExpr::OP_NE
                }
            );
        }
        #[test]
        fn expr_gt() {
            let expr = lex(b"primary_key > 10").unwrap();
            let r = dml::parse_relexpr_full(&expr).unwrap();
            assert_eq!(
                r,
                RelationalExpr {
                    rhs: &(10.into()),
                    lhs: "primary_key".as_bytes(),
                    opc: RelationalExpr::OP_GT
                }
            );
        }
        #[test]
        fn expr_ge() {
            let expr = lex(b"primary_key >= 10").unwrap();
            let r = dml::parse_relexpr_full(&expr).unwrap();
            assert_eq!(
                r,
                RelationalExpr {
                    rhs: &(10.into()),
                    lhs: "primary_key".as_bytes(),
                    opc: RelationalExpr::OP_GE
                }
            );
        }
        #[test]
        fn expr_lt() {
            let expr = lex(b"primary_key < 10").unwrap();
            let r = dml::parse_relexpr_full(&expr).unwrap();
            assert_eq!(
                r,
                RelationalExpr {
                    rhs: &(10.into()),
                    lhs: "primary_key".as_bytes(),
                    opc: RelationalExpr::OP_LT
                }
            );
        }
        #[test]
        fn expr_le() {
            let expr = lex(b"primary_key <= 10").unwrap();
            let r = dml::parse_relexpr_full(&expr).unwrap();
            assert_eq!(
                r,
                RelationalExpr {
                    rhs: &(10.into()),
                    lhs: "primary_key".as_bytes(),
                    opc: RelationalExpr::OP_LE
                }
            );
        }
    }
    mod where_clause {
        use {
            super::*,
            crate::engine::ql::dml::{self, RelationalExpr, WhereClause},
        };
        #[test]
        fn where_single() {
            let tok = lex(br#"
                x = 100
            "#)
            .unwrap();
            let rhs_hundred = 100.into();
            let expected = WhereClause::new(dict! {
                "x".as_bytes() => RelationalExpr {
                    rhs: &rhs_hundred,
                    lhs: "x".as_bytes(),
                    opc: RelationalExpr::OP_EQ
                }
            });
            assert_eq!(expected, dml::parse_where_clause_full(&tok).unwrap());
        }
        #[test]
        fn where_double() {
            let tok = lex(br#"
                userid = 100 and pass = "password"
            "#)
            .unwrap();
            let rhs_hundred = 100.into();
            let rhs_password = "password".into();
            let expected = WhereClause::new(dict! {
                "userid".as_bytes() => RelationalExpr {
                    rhs: &rhs_hundred,
                    lhs: "userid".as_bytes(),
                    opc: RelationalExpr::OP_EQ
                },
                "pass".as_bytes() => RelationalExpr {
                    rhs: &rhs_password,
                    lhs: "pass".as_bytes(),
                    opc: RelationalExpr::OP_EQ
                }
            });
            assert_eq!(expected, dml::parse_where_clause_full(&tok).unwrap());
        }
        #[test]
        fn where_duplicate_condition() {
            let tok = lex(br#"
                userid = 100 and userid > 200
            "#)
            .unwrap();
            assert!(dml::parse_where_clause_full(&tok).is_none());
        }
    }
}
