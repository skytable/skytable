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

use crate::tokenizer::get_query;

#[test]
fn test_basic_tokenization() {
    let input = "set x 100".as_bytes();
    let ret: Vec<String> = get_query(input).unwrap();
    assert_eq!(
        ret,
        vec!["set".to_owned(), "x".to_owned(), "100".to_owned()]
    );
}

#[test]
fn test_single_quote_tokens() {
    let input = "set 'x with a whitespace' 100".as_bytes();
    let ret: Vec<String> = get_query(input).unwrap();
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
    let ret: Vec<String> = get_query(input).unwrap();
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
    let ret: Vec<String> = get_query(input).unwrap();
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
    let ret: Vec<String> = get_query(input).unwrap();
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
    let ret: Vec<String> = get_query(input).unwrap();
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
    let ret = format!("{}", get_query::<Vec<String>>(input).unwrap_err());
    assert_eq!(ret, "mismatched quotes near end of: `x with a whitespace`");
}

#[test]
fn test_missing_double_quote() {
    let input = r#"'get' "x with a whitespace"#.as_bytes();
    let ret = format!("{}", get_query::<Vec<String>>(input).unwrap_err());
    assert_eq!(ret, "mismatched quotes near end of: `x with a whitespace`");
}

#[test]
fn test_extra_whitespace() {
    let input = "set  x  '100'".as_bytes();
    let ret = get_query::<Vec<String>>(input).unwrap();
    assert_eq!(
        ret,
        vec!["set".to_owned(), "x".to_owned(), "100".to_owned()]
    );
}
