/*
 * Created on Thu Jun 09 2022
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

use super::{
    lex::{Ident, LitNum, LitString, LitStringEscaped},
    Scanner,
};

#[test]
fn scanner_tokenize() {
    let tokens = b"create space app".to_vec();
    let scanned_tokens = Scanner::parse_into_tokens(&tokens);
    let scanned_tokens: Vec<String> = scanned_tokens
        .into_iter()
        .map(|tok| unsafe { String::from_utf8_lossy(tok.as_slice()).to_string() })
        .collect();
    assert_eq!(scanned_tokens, ["create", "space", "app"]);
}

#[test]
fn scanner_step_by_step_tokenize() {
    let tokens = b"create space app".to_vec();
    let mut scanner = Scanner::new(&tokens);
    unsafe {
        assert_eq!(scanner.next_token_tl().as_slice(), b"create");
        assert_eq!(scanner.next_token_tl().as_slice(), b"space");
        assert_eq!(scanner.next_token_tl().as_slice(), b"app");
        assert!(scanner.exhausted());
        assert_eq!(scanner.next_token_tl().as_slice(), b"");
        assert_eq!(scanner.next_token_tl().as_slice(), b"");
        assert_eq!(scanner.next_token_tl().as_slice(), b"");
    }
    assert!(scanner.exhausted());
}

// lexing
#[test]
fn lex_ident() {
    let src = b"hello ".to_vec();
    let mut scanner = Scanner::new(&src);
    let ident: Ident = scanner.next().unwrap();
    assert_eq!(unsafe { ident.as_slice() }, b"hello");
    assert!(scanner.exhausted());
    let src = b"hello:world".to_vec();
    let mut scanner = Scanner::new(&src);
    let ident: Ident = scanner.next().unwrap();
    assert_eq!(unsafe { ident.as_slice() }, b"hello");
    assert!(scanner.not_exhausted());
}

#[test]
fn lex_lit_num() {
    let src = b"123456".to_vec();
    let mut scanner = Scanner::new(&src);
    let num: LitNum = scanner.next().unwrap();
    assert_eq!(num.0, 123456);
    let src = b"123456 ".to_vec();
    let mut scanner = Scanner::new(&src);
    let num: LitNum = scanner.next().unwrap();
    assert_eq!(num.0, 123456);
}

#[test]
fn lex_lit_string() {
    let src = br#""hello, world""#.to_vec();
    assert_eq!(
        Scanner::new(&src).next::<LitString>().unwrap().0,
        "hello, world"
    );
    let src = br#""hello, world" "#.to_vec();
    assert_eq!(
        Scanner::new(&src).next::<LitString>().unwrap().0,
        "hello, world"
    );
}

#[test]
fn lex_lit_string_escaped() {
    let src = br#""hello\\world""#.to_vec();
    let litstr = Scanner::new(&src).next::<LitStringEscaped>().unwrap().0;
    assert_eq!(litstr, "hello\\world");
}
