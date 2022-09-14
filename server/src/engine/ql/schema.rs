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

/*
    Most grammar tools are pretty much "off the shelf" which makes some things incredible hard to achieve (such
    as custom error injection logic). To make things icier, Rust's integration with these tools (like lex) is not
    very "refined." Hence, it is best for us to implement our own parsers. In the future, I plan to optimize our
    rule checkers but that's not a concern at the moment.

    -- Sayan (Sept. 14, 2022)
*/

use {
    super::{
        ast::{Compiler, Statement},
        lexer::{Lit, Token, Ty},
        LangError, LangResult, RawSlice,
    },
    std::{collections::HashMap, mem::MaybeUninit, str},
};

macro_rules! boxed {
    ([] $ty:ty) => {
        ::std::boxed::Box::<[$ty]>
    };
}

/*
    Meta
*/

#[derive(Debug)]
struct FieldMeta {
    field_name: Option<RawSlice>,
    unprocessed_layers: boxed![[] TypeConfig],
}

#[derive(Debug)]
struct TypeConfig {
    ty: Ty,
    dict: Dict,
}

/*
    Dictionary
*/

pub type Dict = HashMap<String, DictEntry>;

#[derive(Debug, PartialEq)]
pub enum DictEntry {
    Lit(Lit),
    Map(HashMap<String, Self>),
}

type NxFlag = u8;
const EXP_COLON: NxFlag = 0x00;
const EXP_LIT_OR_OBRC: NxFlag = 0x01;
const EXP_COMMA_OR_CBRC: NxFlag = 0x02;
const EXP_IDENT_OR_CBRC: NxFlag = 0x03;
const EXP_EOF: NxFlag = 0x04;
const EXP_START: NxFlag = 0x05;
const HIBIT: u64 = 1 << 63;

pub(super) fn parse_dictionary(c: &mut Compiler) -> LangResult<Dict> {
    let mut dict = Dict::new();
    let r = self::fold_dict(EXP_START, c.remslice(), &mut dict);
    unsafe {
        // IFEVERBROKEN: When the silicon guys decide to build a new chip with > 48 AS, lmk
        c.incr_cursor_by((r & !HIBIT) as _);
    }
    if r & HIBIT == HIBIT {
        Ok(dict)
    } else {
        Err(LangError::InvalidDictionaryExpression)
    }
}

fn fold_dict<'a>(mut next: NxFlag, src: &'a [Token], dict: &mut Dict) -> u64 {
    /*
        NOTE: Assume respective <lit> validity and other character set rules

        <lbrace> ::= "{"
        <rbrace> ::= "}"
        <colon> ::= ":"
        <ident> ::= (<sym_us> | <alpha>) (<alphanum> | <sym_us>)*
        <dict> ::= <lbrace> (<ident> <colon> (<lit> | <dict> ) <comma>)* <rbrace>
    */
    let mut i = 0;
    let mut okay = true;
    let mut tmp = MaybeUninit::uninit();
    while i < src.len() && okay {
        match (&src[i], next) {
            // as a future optimization, note that this is just a single call
            (Token::OpenBrace, EXP_START) => {
                next = EXP_IDENT_OR_CBRC;
            }
            (Token::Ident(id), EXP_IDENT_OR_CBRC) => {
                next = EXP_COLON;
                tmp = MaybeUninit::new(unsafe {
                    // UNSAFE(@ohsayan): If the token is an ident, the lexer guarantees that is valid unicode
                    str::from_utf8_unchecked(id.as_slice())
                });
            }
            (Token::Colon, EXP_COLON) => next = EXP_LIT_OR_OBRC,
            (Token::Lit(lit), EXP_LIT_OR_OBRC) => {
                okay &= dict
                    .insert(
                        unsafe {
                            // UNSAFE(@ohsayan): This is completely safe because the transition and correctness
                            // of this function makes it a consequence
                            tmp.assume_init_ref()
                        }
                        .to_string(),
                        DictEntry::Lit(lit.clone()),
                    )
                    .is_none();
                next = EXP_COMMA_OR_CBRC;
            }
            (Token::OpenBrace, EXP_LIT_OR_OBRC) => {
                // fold tokens
                let mut this_dict = Dict::new();
                let read = self::fold_dict(EXP_IDENT_OR_CBRC, &src[i..], &mut this_dict);
                i += (read & !HIBIT) as usize;
                okay &= (read & HIBIT) == HIBIT;
                okay &= dict
                    .insert(
                        unsafe {
                            // UNSAFE(@ohsayan): See above comment for context to know why this is safe
                            tmp.assume_init_ref()
                        }
                        .to_string(),
                        DictEntry::Map(this_dict),
                    )
                    .is_none();
                next = EXP_COMMA_OR_CBRC;
            }
            (Token::Comma, EXP_COMMA_OR_CBRC) => {
                next = EXP_IDENT_OR_CBRC;
            }
            (Token::CloseBrace, EXP_COMMA_OR_CBRC | EXP_IDENT_OR_CBRC) => {
                // graceful exit
                next = EXP_EOF;
                i += 1;
                break;
            }
            _ => {
                okay = false;
                break;
            }
        }
        i += 1;
    }
    okay &= next == EXP_EOF;
    ((okay as u64) << 63) | i as u64
}

fn parse_type_definition(_c: &mut Compiler) -> LangResult<boxed![[] TypeConfig]> {
    /*
        NOTE: Assume correct rules in context

        <lbrace> ::= "{"
        <rbrace> ::= "}"
        <langle> ::= "<"
        <rangle> ::= ">"
        <colon> ::= ":"
        <comma> ::= ","
        <tydef_simple> ::= <type>
        <tydef_nest> ::= <type> (<langle> <tydef_nest> <rangle>)*
        <tydef_dict> ::= <type> <lbrace> ( ((<ident> <colon> <lit>) | <tydef_dict>) <comma>)* <rbrace>
        <tydef> ::= <tydef_simple> | <tydef_nest> | <tydef_dict>
    */
    todo!()
}

fn parse_field(_c: &mut Compiler) -> LangResult<FieldMeta> {
    todo!()
}

pub(super) fn parse_schema(_c: &mut Compiler, _model: RawSlice) -> LangResult<Statement> {
    todo!()
}
