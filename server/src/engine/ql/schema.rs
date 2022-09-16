/*
 * Created on Fri Sep 16 2022
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

    This module makes use of DFAs with additional flags, accepting a token stream as input to generate appropriate
    structures, and have provable correctness. Hence, the unsafe code used here is correct, because the states are
    only transitioned to if the input is accepted. If you do find otherwise, please file a bug report. The
    transitions are currently very inefficient but can be made much faster.

    TODO: The SMs can be reduced greatly, enocded to fixed-sized structures even, so do that
    FIXME: For now, try and reduce reliance on additional flags (encoded into state?)

    --
    Sayan (@ohsayan)
    Sept. 15, 2022
*/

use {
    super::{
        ast::{Compiler, Statement},
        lexer::{Kw, Lit, Token},
        LangResult, RawSlice,
    },
    std::{
        collections::HashMap,
        mem::{transmute, MaybeUninit},
    },
};

/*
    Meta
*/

const HIBIT: u64 = 1 << 63;
const TRAIL_COMMA: bool = true;

#[derive(Debug, PartialEq)]
pub enum DictEntry {
    Lit(Lit),
    Map(HashMap<String, DictEntry>),
}

impl From<Lit> for DictEntry {
    fn from(l: Lit) -> Self {
        Self::Lit(l)
    }
}

impl From<Dict> for DictEntry {
    fn from(m: Dict) -> Self {
        Self::Map(m)
    }
}

pub type Dict = HashMap<String, DictEntry>;

/*
    Non-contextual dict
*/

type DictFoldState = u8;
const DICT_STATE_FINAL: DictFoldState = 0xFF;
const DICT_STATE_ACCEPT_OB: DictFoldState = 0x00;
const DICT_STATE_ACCEPT_IDENT_OR_CB: DictFoldState = 0x01;
const DICT_STATE_ACCEPT_COLON: DictFoldState = 0x02;
const DICT_STATE_ACCEPT_LIT_OR_OB: DictFoldState = 0x03;
const DICT_STATE_ACCEPT_COMMA_OR_CB: DictFoldState = 0x04;
const DICT_STATE_ACCEPT_IDENT: DictFoldState = 0x05;

fn rfold_dict(mut state: DictFoldState, tok: &[Token], dict: &mut Dict) -> u64 {
    /*
        NOTE: Assume appropriate rule definitions wherever applicable

        <lbrace> ::= "{"
        <rbrace> ::= "}"
        <colon> ::= ":"
        <comma> ::= ","
        <dict> ::= <lbrace> (<ident> <colon> (<lit> | <dict>) <comma> )* <comma>0*1 <rbace>
    */
    let mut i = 0;
    let l = tok.len();
    let mut okay = true;
    let mut tmp = MaybeUninit::<&str>::uninit();
    while i < l && okay {
        match (&tok[i], state) {
            (Token::OpenBrace, DICT_STATE_ACCEPT_OB) => {
                i += 1;
                // next state is literal
                state = DICT_STATE_ACCEPT_IDENT_OR_CB;
            }
            (Token::CloseBrace, DICT_STATE_ACCEPT_IDENT_OR_CB | DICT_STATE_ACCEPT_COMMA_OR_CB) => {
                i += 1;
                // since someone closed the brace, we're done processing this type
                state = DICT_STATE_FINAL;
                break;
            }
            (Token::Ident(key), DICT_STATE_ACCEPT_IDENT_OR_CB | DICT_STATE_ACCEPT_IDENT) => {
                i += 1;
                tmp = MaybeUninit::new(unsafe { transmute(key.as_slice()) });
                state = DICT_STATE_ACCEPT_COLON;
            }
            (Token::Colon, DICT_STATE_ACCEPT_COLON) => {
                i += 1;
                state = DICT_STATE_ACCEPT_LIT_OR_OB;
            }
            (Token::Lit(l), DICT_STATE_ACCEPT_LIT_OR_OB) => {
                i += 1;
                // insert this key/value pair
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        l.clone().into(),
                    )
                    .is_none();
                state = DICT_STATE_ACCEPT_COMMA_OR_CB;
            }
            (Token::Comma, DICT_STATE_ACCEPT_COMMA_OR_CB) => {
                i += 1; // since there is a comma, expect an ident
                if TRAIL_COMMA {
                    state = DICT_STATE_ACCEPT_IDENT_OR_CB;
                } else {
                    state = DICT_STATE_ACCEPT_IDENT;
                }
            }
            (Token::OpenBrace, DICT_STATE_ACCEPT_LIT_OR_OB) => {
                i += 1;
                // there is another dictionary in here. let's parse it
                let mut this_dict = Dict::new();
                let r = rfold_dict(DICT_STATE_ACCEPT_IDENT_OR_CB, &tok[i..], &mut this_dict);
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        DictEntry::Map(this_dict),
                    )
                    .is_none();
                okay &= r & HIBIT == HIBIT;
                i += (r & !HIBIT) as usize;
                // at the end of a dictionary, we expect a comma or brace close
                state = DICT_STATE_ACCEPT_COMMA_OR_CB;
            }
            _ => {
                okay = false;
                break;
            }
        }
    }
    okay &= state == DICT_STATE_FINAL;
    i as u64 | ((okay as u64) << 63)
}

pub fn fold_dict(tok: &[Token]) -> Option<Dict> {
    let mut dict = Dict::new();
    let r = rfold_dict(DICT_STATE_ACCEPT_OB, tok, &mut dict);
    if r & HIBIT == HIBIT {
        Some(dict)
    } else {
        None
    }
}

/*
    Type metadata
*/

#[derive(Debug, PartialEq)]
pub struct TypeMetaFoldResult {
    c: usize,
    m: [bool; 2],
}

impl TypeMetaFoldResult {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            c: 0,
            m: [true, false],
        }
    }
    #[inline(always)]
    fn incr(&mut self) {
        self.incr_by(1);
    }
    #[inline(always)]
    fn set_has_more(&mut self) {
        self.m[1] = true;
    }
    #[inline(always)]
    fn set_fail(&mut self) {
        self.m[0] = false;
    }
    #[inline(always)]
    pub fn pos(&self) -> usize {
        self.c
    }
    #[inline(always)]
    pub fn is_okay(&self) -> bool {
        self.m[0]
    }
    #[inline(always)]
    pub fn has_more(&self) -> bool {
        self.m[1]
    }
    #[inline(always)]
    fn record(&mut self, cond: bool) {
        self.m[0] &= cond;
    }
    #[inline(always)]
    fn incr_by(&mut self, pos: usize) {
        self.c += pos;
    }
}

type TypeMetaFoldState = u8;
const TYMETA_STATE_FINAL: TypeMetaFoldState = 0xFF;
const TYMETA_STATE_ACCEPT_IDENT: TypeMetaFoldState = 0x00;
const TYMETA_STATE_ACCEPT_IDENT_OR_CB: TypeMetaFoldState = 0x01;
const TYMETA_STATE_ACCEPT_COLON: TypeMetaFoldState = 0x02;
const TYMETA_STATE_ACCEPT_LIT_OR_OB: TypeMetaFoldState = 0x03;
const TYMETA_STATE_ACCEPT_COMMA_OR_CB: TypeMetaFoldState = 0x04;
const TYMETA_STATE_ACCEPT_CB_OR_COMMA: TypeMetaFoldState = 0x05;

pub(super) fn rfold_tymeta(
    mut state: TypeMetaFoldState,
    tok: &[Token],
    dict: &mut Dict,
) -> TypeMetaFoldResult {
    let mut r = TypeMetaFoldResult::new();
    let l = tok.len();
    let mut tmp = MaybeUninit::<&str>::uninit();
    while r.pos() < l && r.is_okay() {
        match (&tok[r.pos()], state) {
            (Token::Ident(id), TYMETA_STATE_ACCEPT_IDENT | TYMETA_STATE_ACCEPT_IDENT_OR_CB) => {
                r.incr();
                state = TYMETA_STATE_ACCEPT_COLON;
                tmp = MaybeUninit::new(unsafe { transmute(id.as_slice()) });
            }
            (Token::Colon, TYMETA_STATE_ACCEPT_COLON) => {
                r.incr();
                state = TYMETA_STATE_ACCEPT_LIT_OR_OB;
            }
            (Token::Comma, TYMETA_STATE_ACCEPT_CB_OR_COMMA) => {
                // we got a comma, so this should have more entries
                r.incr();
                state = TYMETA_STATE_ACCEPT_IDENT_OR_CB;
            }
            (Token::Lit(l), TYMETA_STATE_ACCEPT_LIT_OR_OB | TYMETA_STATE_ACCEPT_IDENT_OR_CB) => {
                r.incr();
                r.record(
                    dict.insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        l.clone().into(),
                    )
                    .is_none(),
                );
                state = TYMETA_STATE_ACCEPT_COMMA_OR_CB;
            }
            (Token::OpenBrace, TYMETA_STATE_ACCEPT_LIT_OR_OB) => {
                // found a nested dict. fold it
                r.incr();
                let this_ret = rfold_tymeta(TYMETA_STATE_ACCEPT_IDENT_OR_CB, &tok[r.pos()..], dict);
                r.incr_by(this_ret.pos());
                r.record(this_ret.is_okay());
                if r.has_more() {
                    // that's broken because L2 can NEVER have a typdef
                    r.set_fail();
                    break;
                }
                state = TYMETA_STATE_ACCEPT_COMMA_OR_CB;
            }
            (
                Token::Keyword(Kw::Type),
                TYMETA_STATE_ACCEPT_IDENT | TYMETA_STATE_ACCEPT_IDENT_OR_CB,
            ) => {
                // we found the type keyword inplace of a colon! increase depth
                r.incr();
                r.set_has_more();
                state = TYMETA_STATE_FINAL;
                break;
            }
            (
                Token::CloseBrace,
                TYMETA_STATE_ACCEPT_COMMA_OR_CB
                | TYMETA_STATE_ACCEPT_IDENT_OR_CB
                | TYMETA_STATE_ACCEPT_CB_OR_COMMA,
            ) => {
                r.incr();
                // brace closed, so it's time to exit
                state = TYMETA_STATE_FINAL;
                break;
            }
            (Token::Comma, TYMETA_STATE_ACCEPT_COMMA_OR_CB | TYMETA_STATE_ACCEPT_IDENT_OR_CB) => {
                r.incr();
                if TRAIL_COMMA {
                    state = TYMETA_STATE_ACCEPT_IDENT_OR_CB;
                } else {
                    // comma, so expect something ahead
                    state = TYMETA_STATE_ACCEPT_IDENT;
                }
            }
            _ => {
                // in any other case, that is broken
                r.set_fail();
                break;
            }
        }
    }
    r.record(state == TYMETA_STATE_FINAL);
    r
}

pub(crate) fn parse_schema(_c: &mut Compiler, _m: RawSlice) -> LangResult<Statement> {
    todo!()
}
