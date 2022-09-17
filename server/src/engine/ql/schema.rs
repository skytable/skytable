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

/// This macro constructs states for our machine
///
/// **DO NOT** construct states manually
macro_rules! states {
    ($(#[$attr:meta])+$vis:vis struct $stateid:ident: $statebase:ty {$($(#[$tyattr:meta])*$v:vis$state:ident = $statexp:expr),* $(,)?}) => {
        #[::core::prelude::v1::derive(::core::cmp::PartialEq, ::core::cmp::Eq, ::core::clone::Clone, ::core::marker::Copy)]
        $(#[$attr])+$vis struct $stateid {__base: $statebase}
        impl $stateid {$($(#[$tyattr])*$v const $state:Self=$stateid{__base: $statexp,};)*}
        impl ::core::fmt::Debug for $stateid {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                let r = match self.__base {$($statexp => ::core::stringify!($state),)* _ => panic!("invalid state"),};
                ::core::write!(f, "{}::{}", ::core::stringify!($stateid), r)
            }
        }
    }
}

const HIBIT: u64 = 1 << 63;
const TRAIL_COMMA: bool = true;

#[derive(Debug, PartialEq)]
pub enum DictEntry {
    Lit(Lit),
    Map(Dict),
}

impl From<Lit> for DictEntry {
    fn from(l: Lit) -> Self {
        Self::Lit(l)
    }
}

impl From<Dict> for DictEntry {
    fn from(d: Dict) -> Self {
        Self::Map(d)
    }
}

pub type Dict = HashMap<String, DictEntry>;

#[derive(Debug, PartialEq)]
pub struct Layer {
    ty: Ty,
    props: Dict,
}

impl Layer {
    pub(super) const fn new(ty: Ty, props: Dict) -> Self {
        Self { ty, props }
    }
}

/*
    Context-free dict
*/

states! {
    /// The dict fold state
    pub struct DictFoldState: u8 {
        FINAL = 0xFF,
        OB = 0x00,
        CB_OR_IDENT = 0x01,
        COLON = 0x02,
        LIT_OR_OB = 0x03,
        COMMA_OR_CB = 0x04,
    }
}

pub(super) fn rfold_dict(mut state: DictFoldState, tok: &[Token], dict: &mut Dict) -> u64 {
    let l = tok.len();
    let mut i = 0;
    let mut okay = true;
    let mut tmp = MaybeUninit::<&str>::uninit();

    while i < l {
        match (&tok[i], state) {
            (Token::OpenBrace, DictFoldState::OB) => {
                i += 1;
                // we found a brace, expect a close brace or an ident
                state = DictFoldState::CB_OR_IDENT;
            }
            (Token::CloseBrace, DictFoldState::CB_OR_IDENT | DictFoldState::COMMA_OR_CB) => {
                // end of stream
                i += 1;
                state = DictFoldState::FINAL;
                break;
            }
            (Token::Ident(id), DictFoldState::CB_OR_IDENT) => {
                // found ident, so expect colon
                i += 1;
                tmp = MaybeUninit::new(unsafe { transmute(id.as_slice()) });
                state = DictFoldState::COLON;
            }
            (Token::Colon, DictFoldState::COLON) => {
                // found colon, expect literal or openbrace
                i += 1;
                state = DictFoldState::LIT_OR_OB;
            }
            (Token::Lit(l), DictFoldState::LIT_OR_OB) => {
                i += 1;
                // found literal; so push in k/v pair and then expect a comma or close brace
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        l.clone().into(),
                    )
                    .is_none();
                state = DictFoldState::COMMA_OR_CB;
            }
            // ONLY COMMA CAPTURE
            (Token::Comma, DictFoldState::COMMA_OR_CB) => {
                i += 1;
                // we found a comma, expect a *strict* brace close or ident
                state = DictFoldState::CB_OR_IDENT;
            }
            (Token::OpenBrace, DictFoldState::LIT_OR_OB) => {
                i += 1;
                // we found an open brace, so this is a dict
                let mut new_dict = Dict::new();
                let ret = rfold_dict(DictFoldState::CB_OR_IDENT, &tok[i..], &mut new_dict);
                okay &= ret & HIBIT == HIBIT;
                i += (ret & !HIBIT) as usize;
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        new_dict.into(),
                    )
                    .is_none();
                // at the end of a dict we either expect a comma or close brace
                state = DictFoldState::COMMA_OR_CB;
            }
            _ => {
                okay = false;
                break;
            }
        }
    }
    okay &= state == DictFoldState::FINAL;
    i as u64 | ((okay as u64) << 63)
}

pub fn fold_dict(tok: &[Token]) -> Option<Dict> {
    let mut d = Dict::new();
    let r = rfold_dict(DictFoldState::OB, tok, &mut d);
    if r & HIBIT == HIBIT {
        Some(d)
    } else {
        None
    }
}

/*
    Contextual dict (tymeta)
*/

states! {
    /// Type metadata fold state
    pub struct TyMetaFoldState: u8 {
        IDENT_OR_CB = 0x00,
        COLON = 0x01,
        LIT_OR_OB = 0x02,
        COMMA_OR_CB = 0x03,
        FINAL = 0xFF,
    }
}

pub struct TyMetaFoldResult {
    c: usize,
    b: [bool; 2],
}

impl TyMetaFoldResult {
    const fn new() -> Self {
        Self {
            c: 0,
            b: [true, false],
        }
    }
    fn incr(&mut self) {
        self.incr_by(1)
    }
    fn incr_by(&mut self, by: usize) {
        self.c += by;
    }
    fn set_fail(&mut self) {
        self.b[0] = false;
    }
    fn set_has_more(&mut self) {
        self.b[1] = true;
    }
    pub fn pos(&self) -> usize {
        self.c
    }
    pub fn has_more(&self) -> bool {
        self.b[1]
    }
    pub fn is_okay(&self) -> bool {
        self.b[0]
    }
    fn record(&mut self, c: bool) {
        self.b[0] &= c;
    }
}

pub(super) fn rfold_tymeta(
    mut state: TyMetaFoldState,
    tok: &[Token],
    dict: &mut Dict,
) -> TyMetaFoldResult {
    let l = tok.len();
    let mut r = TyMetaFoldResult::new();
    let mut tmp = MaybeUninit::<&str>::uninit();
    while r.pos() < l && r.is_okay() {
        match (&tok[r.pos()], state) {
            (Token::Keyword(Kw::Type), TyMetaFoldState::IDENT_OR_CB) => {
                // we were expecting an ident but found the type keyword! increase depth
                r.incr();
                r.set_has_more();
                state = TyMetaFoldState::FINAL;
                break;
            }
            (Token::CloseBrace, TyMetaFoldState::IDENT_OR_CB | TyMetaFoldState::COMMA_OR_CB) => {
                r.incr();
                // found close brace. end of stream
                state = TyMetaFoldState::FINAL;
                break;
            }
            (Token::Ident(ident), TyMetaFoldState::IDENT_OR_CB) => {
                r.incr();
                tmp = MaybeUninit::new(unsafe { transmute(ident.as_slice()) });
                // we just saw an ident, so we expect to see a colon
                state = TyMetaFoldState::COLON;
            }
            (Token::Colon, TyMetaFoldState::COLON) => {
                r.incr();
                // we just saw a colon. now we want a literal or openbrace
                state = TyMetaFoldState::LIT_OR_OB;
            }
            (Token::Lit(lit), TyMetaFoldState::LIT_OR_OB) => {
                r.incr();
                r.record(
                    dict.insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        lit.clone().into(),
                    )
                    .is_none(),
                );
                // saw a literal. next is either comma or close brace
                state = TyMetaFoldState::COMMA_OR_CB;
            }
            (Token::Comma, TyMetaFoldState::COMMA_OR_CB) => {
                r.incr();
                // next is strictly a close brace or ident
                state = TyMetaFoldState::IDENT_OR_CB;
            }
            (Token::OpenBrace, TyMetaFoldState::LIT_OR_OB) => {
                r.incr();
                // another dict in here
                let mut d = Dict::new();
                let ret = rfold_tymeta(TyMetaFoldState::IDENT_OR_CB, &tok[r.pos()..], &mut d);
                r.incr_by(ret.pos());
                r.record(ret.is_okay());
                r.record(!ret.has_more()); // L2 cannot have type definitions
                                           // end of definition or comma followed by something
                r.record(
                    dict.insert(unsafe { tmp.assume_init_ref() }.to_string(), d.into())
                        .is_none(),
                );
                state = TyMetaFoldState::COMMA_OR_CB;
            }
            _ => {
                r.set_fail();
                break;
            }
        }
    }
    r.record(state == TyMetaFoldState::FINAL);
    r
}

pub(super) fn fold_tymeta(tok: &[Token]) -> (TyMetaFoldResult, Dict) {
    let mut d = Dict::new();
    let r = rfold_tymeta(TyMetaFoldState::IDENT_OR_CB, tok, &mut d);
    (r, d)
}

/*
    Layer
*/

states! {
    /// Layer fold state
    pub struct LayerFoldState: u8 {
        TY = 0x00,
        END_OR_OB = 0x01,
        FOLD_DICT_INCOMPLETE = 0x02,
        FOLD_COMPLETED = 0xFF
    }
}

pub(super) fn rfold_layers(tok: &[Token], layers: &mut Vec<Layer>) -> u64 {
    let l = tok.len();
    let mut i = 0;
    let mut okay = true;
    let mut state = LayerFoldState::TY;
    let mut tmp = MaybeUninit::uninit();
    let mut dict = Dict::new();
    while i < l && okay {
        match (&tok[i], state) {
            (Token::Keyword(Kw::TypeId(ty)), LayerFoldState::TY) => {
                i += 1;
                // expecting type, and found type. next is either end or an open brace or some arbitrary token
                tmp = MaybeUninit::new(ty);
                state = LayerFoldState::END_OR_OB;
            }
            (Token::OpenBrace, LayerFoldState::END_OR_OB) => {
                i += 1;
                // since we found an open brace, this type has some meta
                let ret = rfold_tymeta(TyMetaFoldState::IDENT_OR_CB, &tok[i..], &mut dict);
                i += ret.pos();
                okay &= ret.is_okay();
                if ret.has_more() {
                    // more layers
                    let ret = rfold_layers(&tok[i..], layers);
                    okay &= ret & HIBIT == HIBIT;
                    i += (ret & !HIBIT) as usize;
                    state = LayerFoldState::FOLD_DICT_INCOMPLETE;
                } else if okay {
                    // done folding dictionary. nothing more expected. break
                    state = LayerFoldState::FOLD_COMPLETED;
                    layers.push(Layer {
                        ty: unsafe { tmp.assume_init() }.clone(),
                        props: dict,
                    });
                    break;
                }
            }
            (Token::Comma, LayerFoldState::FOLD_DICT_INCOMPLETE) => {
                // there is a comma at the end of this
                i += 1;
                let ret = rfold_tymeta(TyMetaFoldState::IDENT_OR_CB, &tok[i..], &mut dict);
                i += ret.pos();
                okay &= ret.is_okay();
                okay &= !ret.has_more(); // not more than one type depth
                if okay {
                    // done folding dict successfully. nothing more expected. break.
                    state = LayerFoldState::FOLD_COMPLETED;
                    layers.push(Layer {
                        ty: unsafe { tmp.assume_init() }.clone(),
                        props: dict,
                    });
                    break;
                }
            }
            (Token::CloseBrace, LayerFoldState::FOLD_DICT_INCOMPLETE) => {
                // end of stream
                i += 1;
                state = LayerFoldState::FOLD_COMPLETED;
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() }.clone(),
                    props: dict,
                });
                break;
            }
            (_, LayerFoldState::END_OR_OB) => {
                // random arbitrary byte. finish append
                state = LayerFoldState::FOLD_COMPLETED;
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() }.clone(),
                    props: dict,
                });
                break;
            }
            _ => {
                okay = false;
                break;
            }
        }
    }
    okay &= state == LayerFoldState::FOLD_COMPLETED;
    i as u64 | ((okay as u64) << 63)
}

pub(super) fn fold_layers(tok: &[Token]) -> (Vec<Layer>, usize, bool) {
    let mut l = Vec::new();
    let r = rfold_layers(tok, &mut l);
    (l, (r & !HIBIT) as _, r & HIBIT == HIBIT)
}

pub(crate) fn parse_schema(_c: &mut Compiler, _m: RawSlice) -> LangResult<Statement> {
    todo!()
}
