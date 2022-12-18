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

macro_rules! fold_dict {
    ($($input:expr),* $(,)?) => {
        ($({$crate::engine::ql::schema::fold_dict(&super::lex($input).unwrap()).unwrap()}),*)
    }
}

mod dml_tests;
mod entity;
mod lexer_tests;
mod schema_tests;
mod structure_syn;

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
