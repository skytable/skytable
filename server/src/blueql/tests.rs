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
    lex::{
        CloseAngular, CloseParen, Colon, DoubleQuote, Ident, LitNum, LitString, LitStringEscaped,
        OpenAngular, OpenParen, Semicolon, SingleQuote, Type, TypeExpression,
    },
    QueryProcessor,
};

#[test]
fn qp_tokenize() {
    let tokens = b"create space app".to_vec();
    let scanned_tokens = QueryProcessor::parse_into_tokens(&tokens);
    let scanned_tokens: Vec<String> = scanned_tokens
        .into_iter()
        .map(|tok| unsafe { String::from_utf8_lossy(tok.as_slice()).to_string() })
        .collect();
    assert_eq!(scanned_tokens, ["create", "space", "app"]);
}

#[test]
fn qp_step_by_step_tokenize() {
    let tokens = b"create space app".to_vec();
    let mut qp = QueryProcessor::new(&tokens);
    unsafe {
        assert_eq!(qp.next_token_tl().as_slice(), b"create");
        assert_eq!(qp.next_token_tl().as_slice(), b"space");
        assert_eq!(qp.next_token_tl().as_slice(), b"app");
        assert!(qp.exhausted());
        assert_eq!(qp.next_token_tl().as_slice(), b"");
        assert_eq!(qp.next_token_tl().as_slice(), b"");
        assert_eq!(qp.next_token_tl().as_slice(), b"");
    }
    assert!(qp.exhausted());
}

// lexing
#[test]
fn lex_ident() {
    let src = b"hello ".to_vec();
    let mut qp = QueryProcessor::new(&src);
    let ident: Ident = qp.next().unwrap();
    assert_eq!(unsafe { ident.as_slice() }, b"hello");
    assert!(qp.exhausted());
    let src = b"hello:world".to_vec();
    let mut qp = QueryProcessor::new(&src);
    let ident: Ident = qp.next().unwrap();
    assert_eq!(unsafe { ident.as_slice() }, b"hello");
    assert!(qp.not_exhausted());
}

#[test]
fn lex_lit_num() {
    let src = b"123456".to_vec();
    let mut qp = QueryProcessor::new(&src);
    let num: LitNum = qp.next().unwrap();
    assert_eq!(num.0, 123456);
    let src = b"123456 ".to_vec();
    let mut qp = QueryProcessor::new(&src);
    let num: LitNum = qp.next().unwrap();
    assert_eq!(num.0, 123456);
}

#[test]
fn lex_lit_string() {
    let src = br#""hello, world""#.to_vec();
    assert_eq!(
        QueryProcessor::new(&src).next::<LitString>().unwrap().0,
        "hello, world"
    );
    let src = br#""hello, world" "#.to_vec();
    assert_eq!(
        QueryProcessor::new(&src).next::<LitString>().unwrap().0,
        "hello, world"
    );
}

#[test]
fn lex_lit_string_escaped() {
    let src = br#""hello\\world\"""#.to_vec();
    let litstr = QueryProcessor::new(&src)
        .next::<LitStringEscaped>()
        .unwrap()
        .0;
    assert_eq!(litstr, "hello\\world\"");
}

#[test]
fn lex_punctutation() {
    let src = br#"()<>:;'""#.to_vec();
    let mut qp = QueryProcessor::new(&src);
    qp.next::<OpenParen>().unwrap();
    qp.next::<CloseParen>().unwrap();
    qp.next::<OpenAngular>().unwrap();
    qp.next::<CloseAngular>().unwrap();
    qp.next::<Colon>().unwrap();
    qp.next::<Semicolon>().unwrap();
    qp.next::<SingleQuote>().unwrap();
    qp.next::<DoubleQuote>().unwrap();
    assert!(qp.exhausted());
}

#[test]
fn lex_type() {
    let src = b"string binary list".to_vec();
    let mut qp = QueryProcessor::new(&src);
    assert_eq!(qp.next::<Type>().unwrap(), Type::String);
    assert_eq!(qp.next::<Type>().unwrap(), Type::Binary);
    assert_eq!(qp.next::<Type>().unwrap(), Type::List);
    assert!(qp.exhausted());
}

#[test]
fn lex_type_expression() {
    let ty_expr = b"list<list<list<string>>>".to_vec();
    let ty = QueryProcessor::new(&ty_expr)
        .next::<TypeExpression>()
        .unwrap();
    assert_eq!(ty.0, vec![Type::List, Type::List, Type::List, Type::String])
}
