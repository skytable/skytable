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

#[cfg(test)]
macro_rules! dict {
    () => {
        <::std::collections::HashMap<_, _> as ::core::default::Default>::default()
    };
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut hm: ::std::collections::HashMap<_, _> = ::core::default::Default::default();
        $(hm.insert($key.into(), $value.into());)*
        hm
    }};
}

macro_rules! multi_assert_eq {
    ($($lhs:expr),* => $rhs:expr) => {
        $(assert_eq!($lhs, $rhs);)*
    };
}

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
            vec![Token::Lit(Lit::Str("\\".repeat(5)))],
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
    use super::super::{lexer::Lit, schema};

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
}
