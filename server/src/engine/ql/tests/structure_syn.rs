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
    crate::engine::ql::{lex::Lit, schema},
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
        let ret = lex_insecure(
            b"
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
            ",
        )
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
        use {super::*, crate::engine::ql::lex::Lit};

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