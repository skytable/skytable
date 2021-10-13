/*
 * Created on Sun Oct 10 2021
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

use crate::tokenizer::{get_query, TokenizerError};

fn query_from(input: &[u8]) -> Result<Vec<String>, TokenizerError> {
    get_query(input)
}

#[test]
fn test_basic_tokenization() {
    let input = "set x 100".as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec!["set".to_owned(), "x".to_owned(), "100".to_owned()]
    );
}

#[test]
fn test_single_quote_tokens() {
    let input = "set 'x with a whitespace' 100".as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec![
            "set".to_owned(),
            "x with a whitespace".to_owned(),
            "100".to_owned()
        ]
    );
}

#[test]
fn test_double_quote_tokens() {
    let input = r#"set "x with a whitespace" 100"#.as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec![
            "set".to_owned(),
            "x with a whitespace".to_owned(),
            "100".to_owned()
        ]
    );
}

#[test]
fn test_single_and_double_quote_tokens() {
    let input = r#"set "x with a whitespace" 'y with a whitespace'"#.as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec![
            "set".to_owned(),
            "x with a whitespace".to_owned(),
            "y with a whitespace".to_owned()
        ]
    );
}

#[test]
fn test_multiple_single_quote_tokens() {
    let input = r#"'set' 'x with a whitespace' 'y with a whitespace'"#.as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec![
            "set".to_owned(),
            "x with a whitespace".to_owned(),
            "y with a whitespace".to_owned()
        ]
    );
}

#[test]
fn test_multiple_double_quote_tokens() {
    let input = r#""set" "x with a whitespace" "y with a whitespace""#.as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec![
            "set".to_owned(),
            "x with a whitespace".to_owned(),
            "y with a whitespace".to_owned()
        ]
    );
}

#[test]
fn test_missing_single_quote() {
    let input = r#"'get' 'x with a whitespace"#.as_bytes();
    let ret = format!("{}", query_from(input).unwrap_err());
    assert_eq!(ret, "mismatched quotes near end of: `x with a whitespace`");
}

#[test]
fn test_missing_double_quote() {
    let input = r#"'get' "x with a whitespace"#.as_bytes();
    let ret = format!("{}", query_from(input).unwrap_err());
    assert_eq!(ret, "mismatched quotes near end of: `x with a whitespace`");
}

#[test]
fn test_extra_whitespace() {
    let input = "set  x  '100'".as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec!["set".to_owned(), "x".to_owned(), "100".to_owned()]
    );
}

#[test]
fn test_singly_quoted() {
    let input = "set tables' wth".as_bytes();
    let ret = query_from(input).unwrap_err();
    assert_eq!(ret, TokenizerError::ExpectedWhitespace("tables".to_owned()));
}

#[test]
fn test_text_after_quote_nospace() {
    let input = "get 'rust'ferris".as_bytes();
    let ret = query_from(input).unwrap_err();
    assert_eq!(ret, TokenizerError::ExpectedWhitespace("rust'".to_owned()));
}

#[test]
fn test_text_after_double_quote_nospace() {
    let input = r#"get "rust"ferris"#.as_bytes();
    let ret = query_from(input).unwrap_err();
    assert_eq!(ret, TokenizerError::ExpectedWhitespace("rust\"".to_owned()));
}

#[test]
fn test_inline_comment() {
    let input = "set x 100 # sets x to 100".as_bytes();
    let ret = query_from(input).unwrap();
    assert_eq!(
        ret,
        vec!["set".to_owned(), "x".to_owned(), "100".to_owned()]
    )
}

#[test]
fn test_full_comment() {
    let input = "# what is going on?".as_bytes();
    let ret = query_from(input).unwrap();
    assert!(ret.is_empty());
}

#[test]
fn test_ignore_comment() {
    let input = "set x \"# ooh la la\"".as_bytes();
    assert_eq!(
        query_from(input).unwrap(),
        vec!["set".to_owned(), "x".to_owned(), "# ooh la la".to_owned()]
    );
    let input = "set x \"#\"".as_bytes();
    assert_eq!(
        query_from(input).unwrap(),
        vec!["set".to_owned(), "x".to_owned(), "#".to_owned()]
    );
}
