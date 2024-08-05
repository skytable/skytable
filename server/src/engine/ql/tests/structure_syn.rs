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
    super::*,
    crate::engine::{
        data::{lit::Lit, DictGeneric},
        error::QueryError,
        ql::{
            ast::parse_ast_node_full,
            ddl::syn::{self, DictBasic, LayerSpec},
        },
    },
    ast::State,
};

macro_rules! fold_dict {
    ($($dict:expr),+ $(,)?) => {
        ($(
            fold_dict($dict).unwrap()
        ),+)
    }
}

fn fold_dict(raw: &[u8]) -> Option<DictGeneric> {
    let lexed = lex_insecure(raw).unwrap();
    parse_ast_node_full::<DictBasic>(&lexed)
        .map(|v| v.into_inner())
        .ok()
}

mod dict {
    use super::*;

    #[sky_macros::test]
    fn dict_read_mini() {
        let (d1, d2) = fold_dict! {
            br#"{name: "sayan"}"#,
            br#"{name: "sayan",}"#,
        };
        let r = null_dict!("name" => Lit::new_string("sayan".into()));
        multi_assert_eq!(d1, d2 => r);
    }
    #[sky_macros::test]
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
        let r = null_dict! (
            "name" => Lit::new_string("sayan".into()),
            "verified" => Lit::new_bool(true),
            "burgers" => Lit::new_uint(152),
        );
        multi_assert_eq!(d1, d2 => r);
    }
    #[sky_macros::test]
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
            d1, d2, d3 => null_dict! {
                "name" => Lit::new_string("sayan".into()),
                "notes" => null_dict! {
                    "burgers" => Lit::new_string("all the time, extra mayo".into()),
                    "taco" => Lit::new_bool(true),
                    "pretzels" => Lit::new_uint(1),
                }
            }
        );
    }

    #[sky_macros::test]
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
            "#
        };
        multi_assert_eq!(
            d1, d2, d3 => null_dict! {
                "well" => null_dict! {
                    "now" => null_dict! {
                        "this" => null_dict! {
                            "is" => null_dict! {
                                "ridiculous" => Lit::new_bool(true),
                            }
                        }
                    }
                }
            }
        );
    }

    #[sky_macros::non_miri_test] // FIXME(@ohsayan): massive slowdown with miri..not sure why
    fn fuzz_dict() {
        let tok = b"
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
        ";
        let ret_dict = null_dict! {
            "the_tradition_is" => Lit::new_string("hello, world".into()),
            "could_have_been" => null_dict! {
                "this" => Lit::new_bool(true),
                "or_maybe_this" => Lit::new_uint(100),
                "even_this" => Lit::new_string("hello, universe!".into()),
            },
            "but_oh_well" => Lit::new_string("it continues to be the 'annoying' phrase".into()),
            "lorem" => null_dict! {
                "ipsum" => null_dict! {
                    "dolor" => Lit::new_string("sit amet".into())
                }
            }
        };
        fuzz_tokens(&tok[..], |should_pass, new_src| {
            let r = parse_ast_node_full::<DictBasic>(new_src);
            let okay = r.is_ok();
            if should_pass {
                assert_eq!(r.unwrap(), ret_dict)
            }
            okay
        });
    }
}

#[sky_macros::test]
fn list_syn_parse() {
    let tokens = b"[[[string]]]";
    let tokens = lex_insecure(tokens).unwrap();
    let mut state = State::new_inplace(&tokens[1..]);
    let lspec = syn::parse_list_decl_syntax(&mut state).unwrap();
    assert_eq!(
        lspec,
        vec![
            LayerSpec::new("string".into(), into_dict!()),
            LayerSpec::new("list".into(), into_dict!()),
            LayerSpec::new("list".into(), into_dict!()),
            LayerSpec::new("list".into(), into_dict!()),
        ]
    )
}

#[sky_macros::test]
fn list_syn_parse_fail() {
    let failure_cases = ["[string", "[[string]", "[string string]", "]"];
    for failure_case in failure_cases {
        let tokens = lex_insecure(failure_case.as_bytes()).unwrap();
        let mut state = State::new_inplace(&tokens[1..]);
        assert_eq!(
            syn::parse_list_decl_syntax(&mut state).unwrap_err(),
            QueryError::QLInvalidTypeDefinitionSyntax
        );
    }
}

mod null_dict_tests {
    use super::*;
    mod dict {
        use super::*;

        #[sky_macros::test]
        fn null_mini() {
            let d = fold_dict!(br"{ x: null }");
            assert_eq!(
                d,
                null_dict! {
                    "x" => Null,
                }
            );
        }
        #[sky_macros::test]
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
                null_dict! {
                    "this_is_non_null" => Lit::new_string("hello".into()),
                    "but_this_is_null" => Null,
                }
            )
        }
        #[sky_macros::test]
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
                null_dict! {
                    "a_string" => Lit::new_string("this is a string".into()),
                    "num" => Lit::new_uint(1234),
                    "a_dict" => null_dict! {
                        "a_null" => Null,
                    }
                }
            )
        }
        #[sky_macros::test]
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
                null_dict! {
                    "a_string" => Lit::new_string("this is a string".into()),
                    "num" => Lit::new_uint(1234),
                    "a_dict" => null_dict! {
                        "a_null" => Null,
                    },
                    "another_null" => Null,
                }
            )
        }
    }
    // TODO(@ohsayan): Add null tests
}
