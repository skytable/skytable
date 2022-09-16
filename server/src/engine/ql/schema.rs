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
        lexer::{Kw, Lit, Token, Ty},
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
/// Start with this stat when you have already read an OB
const DICTFOLD_STATE_INIT_IDENT_OR_CB: DictFoldState = 0x00;
const DICTFOLD_STATE_FINAL: DictFoldState = 0xFF;
const DICTFOLD_STATE_ACCEPT_OB: DictFoldState = 0x01;
const DICTFOLD_STATE_ACCEPT_COLON: DictFoldState = 0x02;
const DICTFOLD_STATE_ACCEPT_LIT_OR_OB: DictFoldState = 0x03;
const DICTFOLD_STATE_ACCEPT_COMMA_OR_CB: DictFoldState = 0x04;
const DICTFOLD_STATE_ACCEPT_IDENT: DictFoldState = 0x05;

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
            (Token::OpenBrace, DICTFOLD_STATE_ACCEPT_OB) => {
                i += 1;
                // next state is literal
                state = DICTFOLD_STATE_INIT_IDENT_OR_CB;
            }
            (Token::CloseBrace, DICTFOLD_STATE_INIT_IDENT_OR_CB) => {
                i += 1;
                // since someone closed the brace, we're done processing this type
                state = DICTFOLD_STATE_FINAL;
                break;
            }
            (Token::Ident(key), DICTFOLD_STATE_INIT_IDENT_OR_CB | DICTFOLD_STATE_ACCEPT_IDENT) => {
                i += 1;
                tmp = MaybeUninit::new(unsafe { transmute(key.as_slice()) });
                state = DICTFOLD_STATE_ACCEPT_COLON;
            }
            (Token::Colon, DICTFOLD_STATE_ACCEPT_COLON) => {
                i += 1;
                state = DICTFOLD_STATE_ACCEPT_LIT_OR_OB;
            }
            (Token::Lit(l), DICTFOLD_STATE_ACCEPT_LIT_OR_OB) => {
                i += 1;
                // insert this key/value pair
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        l.clone().into(),
                    )
                    .is_none();
                state = DICTFOLD_STATE_ACCEPT_COMMA_OR_CB;
            }
            (Token::Comma, DICTFOLD_STATE_ACCEPT_COMMA_OR_CB) => {
                i += 1; // since there is a comma, expect an ident
                if TRAIL_COMMA {
                    state = DICTFOLD_STATE_INIT_IDENT_OR_CB;
                } else {
                    state = DICTFOLD_STATE_ACCEPT_IDENT;
                }
            }
            (Token::OpenBrace, DICTFOLD_STATE_ACCEPT_LIT_OR_OB) => {
                i += 1;
                // there is another dictionary in here. let's parse it
                let mut this_dict = Dict::new();
                let r = rfold_dict(DICTFOLD_STATE_INIT_IDENT_OR_CB, &tok[i..], &mut this_dict);
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        DictEntry::Map(this_dict),
                    )
                    .is_none();
                okay &= r & HIBIT == HIBIT;
                i += (r & !HIBIT) as usize;
                // at the end of a dictionary, we expect a comma or brace close
                state = DICTFOLD_STATE_ACCEPT_COMMA_OR_CB;
            }
            _ => {
                okay = false;
                break;
            }
        }
    }
    okay &= state == DICTFOLD_STATE_FINAL;
    i as u64 | ((okay as u64) << 63)
}

pub fn fold_dict(tok: &[Token]) -> Option<Dict> {
    let mut dict = Dict::new();
    let r = rfold_dict(DICTFOLD_STATE_ACCEPT_OB, tok, &mut dict);
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

/*
    Layer
*/

#[derive(Debug, PartialEq)]
pub struct Layer {
    ty: Ty,
    props: Dict,
}

type LayerFoldState = u8;
const LAYERFOLD_STATE_FINAL: LayerFoldState = 0xFF;
const LAYERFOLD_STATE_ACCEPT_TYPE: LayerFoldState = 0x00;
const LAYERFOLD_STATE_ACCEPT_OB_OR_END_ANY: LayerFoldState = 0x01;

fn rfold_layers(tok: &[Token], layers: &mut Vec<Layer>) -> u64 {
    let mut i = 0;
    let l = tok.len();
    let mut okay = true;
    let mut state = LAYERFOLD_STATE_ACCEPT_TYPE;
    let mut meta = Dict::new();
    let mut tmp = MaybeUninit::uninit();
    while i < l && okay {
        match (&tok[i], state) {
            (Token::Keyword(Kw::TypeId(t)), LAYERFOLD_STATE_ACCEPT_TYPE) => {
                i += 1;
                tmp = MaybeUninit::new(*t);
                state = LAYERFOLD_STATE_ACCEPT_OB_OR_END_ANY;
            }
            (Token::OpenBrace, LAYERFOLD_STATE_ACCEPT_OB_OR_END_ANY) => {
                i += 1;
                // get ty meta
                let r = rfold_tymeta(TYMETA_STATE_ACCEPT_IDENT_OR_CB, &tok[i..], &mut meta);
                okay &= r.is_okay();
                i += r.pos();
                if r.has_more() {
                    // mmm, more layers
                    let r = rfold_layers(&tok[i..], layers);
                    okay &= r & HIBIT == HIBIT;
                    i += (r & !HIBIT) as usize;
                    // fold remaining meta (if this has a closebrace great; if not it *has* to have a comma to provide other meta)
                    let ret = rfold_tymeta(TYMETA_STATE_ACCEPT_CB_OR_COMMA, &tok[i..], &mut meta);
                    okay &= ret.is_okay();
                    okay &= !ret.has_more(); // can't have two kinds
                    i += ret.pos();
                }
                // since we ended a dictionary parse, the exact valid token after this could be anything
                // since this is CFG, we don't care
                state = LAYERFOLD_STATE_FINAL;
                // push in this layer
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() },
                    props: meta,
                });
                break;
            }
            (_, LAYERFOLD_STATE_ACCEPT_OB_OR_END_ANY) => {
                // we don't care what token is here. we want this to end. also, DO NOT incr pos since we haven't
                // seen this token
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() },
                    props: meta,
                });
                state = LAYERFOLD_STATE_FINAL;
                break;
            }
            _ => {
                // in any other case, that is broken
                okay = false;
                break;
            }
        }
    }
    if state == LAYERFOLD_STATE_ACCEPT_OB_OR_END_ANY {
        // if we've exited at this state, there was only one possible exit
        layers.push(Layer {
            ty: unsafe { tmp.assume_init() },
            props: dict!(),
        });
        // safe exit
    } else {
        okay &= state == LAYERFOLD_STATE_FINAL;
    }
    (i as u64) | ((okay as u64) << 63)
}

pub fn fold_layers(tok: &[Token]) -> Option<Vec<Layer>> {
    let mut l = Vec::new();
    let r = rfold_layers(tok, &mut l);
    if r & HIBIT == HIBIT {
        Some(l)
    } else {
        None
    }
}

pub(crate) fn parse_schema(_c: &mut Compiler, _m: RawSlice) -> LangResult<Statement> {
    todo!()
}
