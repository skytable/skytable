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
    super::lex::{InsecureLexer, SafeLexer, Symbol, Token},
    crate::{
        engine::{data::HSData, error::LexResult},
        util::test_utils,
    },
    rand::{self, Rng},
};

mod dml_tests;
mod entity;
mod lexer_tests;
mod schema_tests;
mod structure_syn;

#[inline(always)]
/// Uses the [`InsecureLexer`] to lex the given input
pub fn lex_insecure<'a>(src: &'a [u8]) -> LexResult<Vec<Token<'a>>> {
    InsecureLexer::lex(src)
}
#[inline(always)]
/// Uses the [`SafeLexer`] to lex the given input
pub fn lex_secure(src: &[u8]) -> LexResult<Vec<Token>> {
    SafeLexer::lex(src)
}

pub trait NullableData<T> {
    fn data(self) -> Option<T>;
}

impl<T> NullableData<HSData> for T
where
    T: Into<HSData>,
{
    fn data(self) -> Option<HSData> {
        Some(self.into())
    }
}

struct Null;

impl NullableData<HSData> for Null {
    fn data(self) -> Option<HSData> {
        None
    }
}

fn nullable_datatype(v: impl NullableData<HSData>) -> Option<HSData> {
    v.data()
}

pub trait NullableDictEntry {
    fn data(self) -> Option<crate::engine::data::DictEntryGeneric>;
}

impl NullableDictEntry for Null {
    fn data(self) -> Option<crate::engine::data::DictEntryGeneric> {
        None
    }
}

impl<'a> NullableDictEntry for super::lex::Lit<'a> {
    fn data(self) -> Option<crate::engine::data::DictEntryGeneric> {
        Some(crate::engine::data::DictEntryGeneric::from(self.as_ir()))
    }
}

impl NullableDictEntry for crate::engine::data::DictGeneric {
    fn data(self) -> Option<crate::engine::data::DictEntryGeneric> {
        Some(crate::engine::data::DictEntryGeneric::Map(self))
    }
}

/// A very "basic" fuzzer that will randomly inject tokens wherever applicable
fn fuzz_tokens(src: &[u8], fuzzverify: impl Fn(bool, &[Token]) -> bool) {
    let src_tokens = lex_insecure(src).unwrap();
    static FUZZ_TARGETS: [Token; 2] = [Token::Symbol(Symbol::SymComma), Token::IgnorableComma];
    let mut rng = rand::thread_rng();
    #[inline(always)]
    fn inject(new_src: &mut Vec<Token>, rng: &mut impl Rng) -> usize {
        let start = new_src.len();
        (0..test_utils::random_number(0, 5, rng))
            .for_each(|_| new_src.push(Token::Symbol(Symbol::SymComma)));
        new_src.len() - start
    }
    let fuzz_amount = src_tokens
        .iter()
        .filter(|tok| FUZZ_TARGETS.contains(tok))
        .count();
    for _ in 0..(fuzz_amount.pow(3)) {
        let mut new_src = Vec::with_capacity(src_tokens.len());
        let mut should_pass = true;
        src_tokens.iter().for_each(|tok| match tok {
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
        if fuzzverify(should_pass, &new_src) && !should_pass {
            panic!(
                "expected failure for `{}`, but it passed",
                new_src
                    .iter()
                    .map(|tok| format!("{} ", tok.to_string()).into_bytes())
                    .flatten()
                    .map(char::from)
                    .collect::<String>()
            )
        }
    }
}
