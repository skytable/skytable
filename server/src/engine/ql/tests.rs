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

use super::lexer;

mod lexer_tests {
    use crate::engine::ql::LangError;

    use super::lexer::{Lexer, Lit, Token};

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
            Lexer::lex(&src).unwrap(),
            vec![Token::Ident("hello".into())]
        );
    }

    // literals
    #[test]
    fn lex_number() {
        let number = v!("123456");
        assert_eq!(
            Lexer::lex(&number).unwrap(),
            vec![Token::Lit(Lit::Num(123456))]
        );
    }
    #[test]
    fn lex_bool() {
        let (t, f) = v!("true", "false");
        assert_eq!(Lexer::lex(&t).unwrap(), vec![Token::Lit(Lit::Bool(true))]);
        assert_eq!(Lexer::lex(&f).unwrap(), vec![Token::Lit(Lit::Bool(false))]);
    }
    #[test]
    fn lex_string() {
        let s = br#" "hello, world" "#;
        assert_eq!(
            Lexer::lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("hello, world".into()))]
        );
        let s = br#" 'hello, world' "#;
        assert_eq!(
            Lexer::lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("hello, world".into()))]
        );
    }
    #[test]
    fn lex_string_test_escape_quote() {
        let s = br#" "\"hello world\"" "#; // == "hello world"
        assert_eq!(
            Lexer::lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("\"hello world\"".into()))]
        );
        let s = br#" '\'hello world\'' "#; // == 'hello world'
        assert_eq!(
            Lexer::lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("'hello world'".into()))]
        );
    }
    #[test]
    fn lex_string_use_different_quote_style() {
        let s = br#" "he's on it" "#;
        assert_eq!(
            Lexer::lex(s).unwrap(),
            vec![Token::Lit(Lit::Str("he's on it".into()))]
        );
        let s = br#" 'he thinks that "that girl" fixed it' "#;
        assert_eq!(
            Lexer::lex(s).unwrap(),
            vec![Token::Lit(Lit::Str(
                "he thinks that \"that girl\" fixed it".into()
            ))]
        )
    }
    #[test]
    fn lex_string_escape_bs() {
        let s = v!(r#" "windows has c:\\" "#);
        assert_eq!(
            Lexer::lex(&s).unwrap(),
            vec![Token::Lit(Lit::Str("windows has c:\\".into()))]
        );
        let s = v!(r#" 'windows has c:\\' "#);
        assert_eq!(
            Lexer::lex(&s).unwrap(),
            vec![Token::Lit(Lit::Str("windows has c:\\".into()))]
        );
        let lol = v!(r#"'\\\\\\\\\\'"#);
        assert_eq!(
            Lexer::lex(&lol).unwrap(),
            vec![Token::Lit(Lit::Str("\\".repeat(5)))],
            "lol"
        )
    }
    #[test]
    fn lex_string_bad_escape() {
        let wth = br#" '\a should be an alert on windows apparently' "#;
        assert_eq!(
            Lexer::lex(wth).unwrap_err(),
            LangError::InvalidStringLiteral
        );
    }
    #[test]
    fn lex_string_unclosed() {
        let wth = br#" 'omg where did the end go "#;
        assert_eq!(
            Lexer::lex(wth).unwrap_err(),
            LangError::InvalidStringLiteral
        );
        let wth = br#" 'see, we escaped the end\' "#;
        assert_eq!(
            Lexer::lex(wth).unwrap_err(),
            LangError::InvalidStringLiteral
        );
    }
}
