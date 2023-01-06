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
    Most grammar tools are pretty much "off the shelf" which makes some things incredibly hard to achieve (such
    as custom error injection logic). To make things icier, Rust's integration with these tools (like lex) is not
    very "refined." Hence, it is best for us to implement our own parsers. In the future, I plan to optimize our
    rule checkers but that's not a concern at the moment.

    This module makes use of DFAs with additional flags, accepting a token stream as input to generate appropriate
    structures, and have provable correctness. Hence, the unsafe code used here is correct, because the states are
    only transitioned to if the input is accepted. If you do find otherwise, please file a bug report. The
    transitions are currently very inefficient but can be made much faster.

    TODO: The SMs can be reduced greatly, enocded to fixed-sized structures even, so do that
    FIXME: For now, try and reduce reliance on additional flags (encoded into state?)
    FIXME: The returns are awfully large right now. Do something about it

    --
    Sayan (@ohsayan)
    Sept. 15, 2022
*/

use {
    super::{
        ast::QueryData,
        lexer::{LitIR, LitIROwned, Slice, Symbol, Token},
        LangError, LangResult,
    },
    crate::util::MaybeInit,
    core::str,
    std::collections::{HashMap, HashSet},
};

#[cfg(test)]
use crate::engine::ql::ast::InplaceData;

/*
    Meta
*/

/// This macro constructs states for our machine
///
/// **DO NOT** construct states manually
macro_rules! states {
    ($(#[$attr:meta])+$vis:vis struct $stateid:ident: $statebase:ty {$($(#[$tyattr:meta])*$v:vis$state:ident = $statexp:expr),+ $(,)?}) => {
        #[::core::prelude::v1::derive(::core::cmp::PartialEq,::core::cmp::Eq,::core::clone::Clone,::core::marker::Copy)]
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

/// A static string slice
type StaticStr = &'static str;

const HIBIT: u64 = 1 << 63;
/// Flag for disallowing the `..` syntax
const DISALLOW_RESET_SYNTAX: bool = false;
/// Flag for allowing the `..` syntax
const ALLOW_RESET_SYNTAX: bool = true;

#[derive(Debug, PartialEq)]
/// A dictionary entry type. Either a literal or another dictionary
pub enum DictEntry {
    Lit(LitIROwned),
    Map(Dict),
}

impl<'a> From<LitIR<'a>> for DictEntry {
    fn from(l: LitIR<'a>) -> Self {
        Self::Lit(l.to_litir_owned())
    }
}

impl From<Dict> for DictEntry {
    fn from(d: Dict) -> Self {
        Self::Map(d)
    }
}

/// A metadata dictionary
pub type Dict = HashMap<String, Option<DictEntry>>;

#[derive(Debug, PartialEq)]
/// A layer contains a type and corresponding metadata
pub struct Layer<'a> {
    ty: Slice<'a>,
    props: Dict,
    reset: bool,
}

impl<'a> Layer<'a> {
    //// Create a new layer
    pub(super) const fn new(ty: Slice<'a>, props: Dict, reset: bool) -> Self {
        Self { ty, props, reset }
    }
    /// Create a new layer that doesn't have any reset
    pub(super) const fn new_noreset(ty: Slice<'a>, props: Dict) -> Self {
        Self::new(ty, props, false)
    }
    /// Create a new layer that adds a reset
    pub(super) const fn new_reset(ty: Slice<'a>, props: Dict) -> Self {
        Self::new(ty, props, true)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
/// Field properties
pub struct FieldProperties {
    pub(super) properties: HashSet<StaticStr>,
}

impl FieldProperties {
    const NULL: StaticStr = "null";
    const PRIMARY: StaticStr = "primary";
    pub fn new() -> Self {
        Self {
            properties: HashSet::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
/// A field definition
pub struct Field<'a> {
    /// the field name
    pub(super) field_name: Slice<'a>,
    /// layers
    pub(super) layers: Vec<Layer<'a>>,
    /// properties
    pub(super) props: HashSet<StaticStr>,
}

impl<'a> Field<'a> {
    #[inline(always)]
    pub fn new(field_name: Slice<'a>, layers: Vec<Layer<'a>>, props: HashSet<StaticStr>) -> Self {
        Self {
            field_name,
            layers,
            props,
        }
    }
}

#[derive(Debug, PartialEq)]
/// A model definition
pub struct Model<'a> {
    /// the model name
    pub(super) model_name: Slice<'a>,
    /// the fields
    pub(super) fields: Vec<Field<'a>>,
    /// properties
    pub(super) props: Dict,
}

impl<'a> Model<'a> {
    #[inline(always)]
    pub fn new(model_name: Slice<'a>, fields: Vec<Field<'a>>, props: Dict) -> Self {
        Self {
            model_name,
            fields,
            props,
        }
    }
}

#[derive(Debug, PartialEq)]
/// A space
pub struct Space<'a> {
    /// the space name
    pub(super) space_name: Slice<'a>,
    /// properties
    pub(super) props: Dict,
}

#[derive(Debug, PartialEq)]
/// An alter space query with corresponding data
pub struct AlterSpace<'a> {
    pub(super) space_name: Slice<'a>,
    pub(super) updated_props: Dict,
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

/// Fold a dictionary
pub(super) fn rfold_dict<'a, Qd: QueryData<'a>>(
    mut state: DictFoldState,
    tok: &'a [Token],
    d: &mut Qd,
    dict: &mut Dict,
) -> u64 {
    /*
        NOTE: Assume rules wherever applicable

        <openbrace> ::= "{"
        <closebrace> ::= "}"
        <comma> ::= ","
        <colon> ::= ":"
        <dict> ::= <openbrace> (<ident> <colon> (<lit> | <dict>) <comma>)* <comma>* <closebrace>
    */
    let l = tok.len();
    let mut i = 0;
    let mut okay = true;
    let mut tmp = MaybeInit::uninit();

    while i < l {
        match (&tok[i], state) {
            (Token::Symbol(Symbol::TtOpenBrace), DictFoldState::OB) => {
                i += 1;
                // we found a brace, expect a close brace or an ident
                state = DictFoldState::CB_OR_IDENT;
            }
            (
                Token::Symbol(Symbol::TtCloseBrace),
                DictFoldState::CB_OR_IDENT | DictFoldState::COMMA_OR_CB,
            ) => {
                // end of stream
                i += 1;
                state = DictFoldState::FINAL;
                break;
            }
            (Token::Ident(id), DictFoldState::CB_OR_IDENT) => {
                // found ident, so expect colon
                i += 1;
                tmp = MaybeInit::new(unsafe { str::from_utf8_unchecked(id) });
                state = DictFoldState::COLON;
            }
            (Token::Symbol(Symbol::SymColon), DictFoldState::COLON) => {
                // found colon, expect literal or openbrace
                i += 1;
                state = DictFoldState::LIT_OR_OB;
            }
            (tok, DictFoldState::LIT_OR_OB) if Qd::can_read_lit_from(d, tok) => {
                i += 1;
                // found literal; so push in k/v pair and then expect a comma or close brace
                unsafe {
                    okay &= dict
                        .insert(
                            tmp.assume_init_ref().to_string(),
                            Some(Qd::read_lit(d, tok).into()),
                        )
                        .is_none();
                }
                state = DictFoldState::COMMA_OR_CB;
            }
            (Token![null], DictFoldState::LIT_OR_OB) => {
                // null
                i += 1;
                okay &= dict
                    .insert(unsafe { tmp.assume_init_ref() }.to_string(), None)
                    .is_none();
                state = DictFoldState::COMMA_OR_CB;
            }
            // ONLY COMMA CAPTURE
            (Token::Symbol(Symbol::SymComma), DictFoldState::COMMA_OR_CB) => {
                i += 1;
                // we found a comma, expect a *strict* brace close or ident
                state = DictFoldState::CB_OR_IDENT;
            }
            (Token::Symbol(Symbol::TtOpenBrace), DictFoldState::LIT_OR_OB) => {
                i += 1;
                // we found an open brace, so this is a dict
                let mut new_dict = Dict::new();
                let ret = rfold_dict(DictFoldState::CB_OR_IDENT, &tok[i..], d, &mut new_dict);
                okay &= ret & HIBIT == HIBIT;
                i += (ret & !HIBIT) as usize;
                okay &= dict
                    .insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        Some(new_dict.into()),
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

#[cfg(test)]
/// Fold a dictionary (**test-only**)
pub fn fold_dict(tok: &[Token]) -> Option<Dict> {
    let mut d = Dict::new();
    let r = rfold_dict(DictFoldState::OB, tok, &mut InplaceData::new(), &mut d);
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
        CB = 0x04,
        FINAL = 0xFF,
    }
}

#[derive(Debug, PartialEq)]
/// The result of a type metadata fold
pub struct TyMetaFoldResult {
    cnt: usize,
    reset: bool,
    more: bool,
    okay: bool,
}

impl TyMetaFoldResult {
    #[inline(always)]
    /// Create a new [`TyMetaFoldResult`] with the default settings:
    /// - reset: false
    /// - more: false
    /// - okay: true
    const fn new() -> Self {
        Self {
            cnt: 0,
            reset: false,
            more: false,
            okay: true,
        }
    }
    #[inline(always)]
    /// Increment the position
    fn incr(&mut self) {
        self.incr_by(1)
    }
    #[inline(always)]
    /// Increment the position by `by`
    fn incr_by(&mut self, by: usize) {
        self.cnt += by;
    }
    #[inline(always)]
    /// Set fail
    fn set_fail(&mut self) {
        self.okay = false;
    }
    #[inline(always)]
    /// Set has more
    fn set_has_more(&mut self) {
        self.more = true;
    }
    #[inline(always)]
    /// Set reset
    fn set_reset(&mut self) {
        self.reset = true;
    }
    #[inline(always)]
    /// Should the meta be reset?
    pub fn should_reset(&self) -> bool {
        self.reset
    }
    #[inline(always)]
    /// Returns the cursor
    pub fn pos(&self) -> usize {
        self.cnt
    }
    #[inline(always)]
    /// Returns if more layers are expected
    pub fn has_more(&self) -> bool {
        self.more
    }
    #[inline(always)]
    /// Returns if the internal state is okay
    pub fn is_okay(&self) -> bool {
        self.okay
    }
    #[inline(always)]
    /// Records an expression
    fn record(&mut self, c: bool) {
        self.okay &= c;
    }
}

/// Fold type metadata (flag setup dependent on caller)
pub(super) fn rfold_tymeta<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    mut state: TyMetaFoldState,
    tok: &'a [Token],
    d: &mut Qd,
    dict: &mut Dict,
) -> TyMetaFoldResult {
    let l = tok.len();
    let mut r = TyMetaFoldResult::new();
    let mut tmp = MaybeInit::uninit();
    while r.pos() < l && r.is_okay() {
        match (&tok[r.pos()], state) {
            (Token![type], TyMetaFoldState::IDENT_OR_CB) => {
                // we were expecting an ident but found the type keyword! increase depth
                r.incr();
                r.set_has_more();
                state = TyMetaFoldState::FINAL;
                break;
            }
            (Token::Symbol(Symbol::SymPeriod), TyMetaFoldState::IDENT_OR_CB) if ALLOW_RESET => {
                r.incr();
                let reset = r.pos() < l && tok[r.pos()] == Token::Symbol(Symbol::SymPeriod);
                r.incr_by(reset as _);
                r.record(reset);
                r.set_reset();
                state = TyMetaFoldState::CB;
            }
            (
                Token::Symbol(Symbol::TtCloseBrace),
                TyMetaFoldState::IDENT_OR_CB | TyMetaFoldState::COMMA_OR_CB | TyMetaFoldState::CB,
            ) => {
                r.incr();
                // found close brace. end of stream
                state = TyMetaFoldState::FINAL;
                break;
            }
            (Token::Ident(ident), TyMetaFoldState::IDENT_OR_CB) => {
                r.incr();
                tmp = MaybeInit::new(unsafe { str::from_utf8_unchecked(ident) });
                // we just saw an ident, so we expect to see a colon
                state = TyMetaFoldState::COLON;
            }
            (Token::Symbol(Symbol::SymColon), TyMetaFoldState::COLON) => {
                r.incr();
                // we just saw a colon. now we want a literal or openbrace
                state = TyMetaFoldState::LIT_OR_OB;
            }
            (tok, TyMetaFoldState::LIT_OR_OB) if Qd::can_read_lit_from(d, tok) => {
                r.incr();
                unsafe {
                    r.record(
                        dict.insert(
                            tmp.assume_init_ref().to_string(),
                            Some(Qd::read_lit(d, tok).into()),
                        )
                        .is_none(),
                    );
                }
                // saw a literal. next is either comma or close brace
                state = TyMetaFoldState::COMMA_OR_CB;
            }
            (Token![null], TyMetaFoldState::LIT_OR_OB) => {
                r.incr();
                r.record(
                    dict.insert(unsafe { tmp.assume_init_ref() }.to_string(), None)
                        .is_none(),
                );
                // saw null, start parsing another entry
                state = TyMetaFoldState::COMMA_OR_CB;
            }
            (Token::Symbol(Symbol::SymComma), TyMetaFoldState::COMMA_OR_CB) => {
                r.incr();
                // next is strictly a close brace or ident
                state = TyMetaFoldState::IDENT_OR_CB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), TyMetaFoldState::LIT_OR_OB) => {
                r.incr();
                // another dict in here
                let mut nd = Dict::new();
                let ret = rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    &tok[r.pos()..],
                    d,
                    &mut nd,
                );
                r.incr_by(ret.pos());
                r.record(ret.is_okay());
                // L2 cannot have type definitions
                r.record(!ret.has_more());
                // end of definition or comma followed by something
                r.record(
                    dict.insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        Some(nd.into()),
                    )
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

#[cfg(test)]
/// (**test-only**) fold type metadata
pub(super) fn fold_tymeta(tok: &[Token]) -> (TyMetaFoldResult, Dict) {
    let mut d = Dict::new();
    let r = rfold_tymeta::<InplaceData, DISALLOW_RESET_SYNTAX>(
        TyMetaFoldState::IDENT_OR_CB,
        tok,
        &mut InplaceData::new(),
        &mut d,
    );
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

/// Fold layers
pub(super) fn rfold_layers<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    start: LayerFoldState,
    tok: &'a [Token],
    qd: &mut Qd,
    layers: &mut Vec<Layer<'a>>,
) -> u64 {
    /*
        NOTE: Assume rules wherever applicable

        <openbrace> ::= "{"
        <closebrace> ::= "}"
        <comma> ::= ","
        <colon> ::= ":"
        <kw_type> ::= "type"
        <layer> ::= <openbrace>
            (<kw_type> <kw_typeid> <layer> <comma>)*1
            (<ident> <colon> (<lit> | <dict>) <comma>)*
            <comma>* <closebrace>
    */
    let l = tok.len();
    let mut i = 0;
    let mut okay = true;
    let mut state = start;
    let mut tmp = MaybeInit::uninit();
    let mut dict = Dict::new();
    while i < l && okay {
        match (&tok[i], state) {
            (Token::Ident(ty), LayerFoldState::TY) => {
                i += 1;
                // expecting type, and found type. next is either end or an open brace or some arbitrary token
                tmp = MaybeInit::new(ty.clone());
                state = LayerFoldState::END_OR_OB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), LayerFoldState::END_OR_OB) => {
                i += 1;
                // since we found an open brace, this type has some meta
                let ret = rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    &tok[i..],
                    qd,
                    &mut dict,
                );
                i += ret.pos();
                okay &= ret.is_okay();
                if ret.has_more() {
                    // more layers
                    let ret =
                        rfold_layers::<Qd, ALLOW_RESET>(LayerFoldState::TY, &tok[i..], qd, layers);
                    okay &= ret & HIBIT == HIBIT;
                    i += (ret & !HIBIT) as usize;
                    state = LayerFoldState::FOLD_DICT_INCOMPLETE;
                } else if okay {
                    // done folding dictionary. nothing more expected. break
                    state = LayerFoldState::FOLD_COMPLETED;
                    layers.push(Layer {
                        ty: unsafe { tmp.assume_init() }.clone(),
                        props: dict,
                        reset: ret.should_reset(),
                    });
                    break;
                }
            }
            (Token::Symbol(Symbol::SymComma), LayerFoldState::FOLD_DICT_INCOMPLETE) => {
                // there is a comma at the end of this
                i += 1;
                let ret = rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    &tok[i..],
                    qd,
                    &mut dict,
                );
                i += ret.pos();
                okay &= ret.is_okay();
                okay &= !ret.has_more(); // not more than one type depth
                if okay {
                    // done folding dict successfully. nothing more expected. break.
                    state = LayerFoldState::FOLD_COMPLETED;
                    layers.push(Layer {
                        ty: unsafe { tmp.assume_init() }.clone(),
                        props: dict,
                        reset: ret.should_reset(),
                    });
                    break;
                }
            }
            (Token::Symbol(Symbol::TtCloseBrace), LayerFoldState::FOLD_DICT_INCOMPLETE) => {
                // end of stream
                i += 1;
                state = LayerFoldState::FOLD_COMPLETED;
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() }.clone(),
                    props: dict,
                    reset: false,
                });
                break;
            }
            (_, LayerFoldState::END_OR_OB) => {
                // random arbitrary byte. finish append
                state = LayerFoldState::FOLD_COMPLETED;
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() }.clone(),
                    props: dict,
                    reset: false,
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

#[cfg(test)]
#[inline(always)]
/// (**test-only**) fold layers
pub(super) fn fold_layers<'a>(tok: &'a [Token]) -> (Vec<Layer<'a>>, usize, bool) {
    let mut l = Vec::new();
    let r = rfold_layers::<InplaceData, DISALLOW_RESET_SYNTAX>(
        LayerFoldState::TY,
        tok,
        &mut InplaceData::new(),
        &mut l,
    );
    (l, (r & !HIBIT) as _, r & HIBIT == HIBIT)
}

#[inline(always)]
/// Collect field properties
pub(super) fn collect_field_properties(tok: &[Token]) -> (FieldProperties, u64) {
    let mut props = FieldProperties::default();
    let mut i = 0;
    let mut okay = true;
    while i < tok.len() {
        match &tok[i] {
            Token![primary] => okay &= props.properties.insert(FieldProperties::PRIMARY),
            Token![null] => okay &= props.properties.insert(FieldProperties::NULL),
            Token::Ident(_) => break,
            _ => {
                // we could pass this over to the caller, but it's better if we do it since we're doing
                // a linear scan anyways
                okay = false;
                break;
            }
        }
        i += 1;
    }
    (props, i as u64 | ((okay as u64) << 63))
}

#[cfg(test)]
#[inline(always)]
/// (**test-only**) parse field properties
pub(super) fn parse_field_properties(tok: &[Token]) -> (FieldProperties, usize, bool) {
    let (p, r) = collect_field_properties(tok);
    (p, (r & !HIBIT) as _, r & HIBIT == HIBIT)
}

#[cfg(test)]
pub(super) fn parse_field_full<'a>(tok: &'a [Token]) -> LangResult<(usize, Field<'a>)> {
    self::parse_field(tok, &mut InplaceData::new())
}

#[inline(always)]
/// Parse a field using the declaration-syntax (not field syntax)
pub(super) fn parse_field<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
) -> LangResult<(usize, Field<'a>)> {
    let l = tok.len();
    let mut i = 0;
    let mut okay = true;
    // parse field properties
    let (props, r) = collect_field_properties(tok);
    okay &= r & HIBIT == HIBIT;
    i += (r & !HIBIT) as usize;
    // if exhauted or broken, simply return
    if i == l || !okay || (l - i) == 1 {
        return Err(LangError::UnexpectedEndofStatement);
    }

    // field name
    let field_name = match (&tok[i], &tok[i + 1]) {
        (Token::Ident(id), Token::Symbol(Symbol::SymColon)) => id,
        _ => return Err(LangError::UnexpectedToken),
    };
    i += 2;

    // layers
    let mut layers = Vec::new();
    let r =
        rfold_layers::<Qd, DISALLOW_RESET_SYNTAX>(LayerFoldState::TY, &tok[i..], qd, &mut layers);
    okay &= r & HIBIT == HIBIT;
    i += (r & !HIBIT) as usize;

    if okay {
        Ok((
            i,
            Field {
                field_name: field_name.clone(),
                layers,
                props: props.properties,
            },
        ))
    } else {
        Err(LangError::UnexpectedToken)
    }
}

states! {
    /// Accept state for a schema parse
    pub struct SchemaParseState: u8 {
        OPEN_PAREN = 0x00,
        FIELD = 0x01,
        COMMA_OR_END = 0x02,
        END_OR_FIELD = 0x03,
    }
}

#[cfg(test)]
pub(super) fn parse_schema_from_tokens_full<'a>(
    tok: &'a [Token],
) -> LangResult<(Model<'a>, usize)> {
    self::parse_schema_from_tokens::<InplaceData>(tok, &mut InplaceData::new())
}

#[inline(always)]
/// Parse a fresh schema with declaration-syntax fields
pub(super) fn parse_schema_from_tokens<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
) -> LangResult<(Model<'a>, usize)> {
    // parse fields
    let l = tok.len();
    let mut i = 0;
    // check if we have our model name
    let mut okay = i < l && tok[i].is_ident();
    i += okay as usize;
    let mut fields = Vec::with_capacity(2);
    let mut state = SchemaParseState::OPEN_PAREN;

    while i < l && okay {
        match (&tok[i], state) {
            (Token::Symbol(Symbol::TtOpenParen), SchemaParseState::OPEN_PAREN) => {
                i += 1;
                state = SchemaParseState::FIELD;
            }
            (
                Token![primary] | Token![null] | Token::Ident(_),
                SchemaParseState::FIELD | SchemaParseState::END_OR_FIELD,
            ) => {
                // fine, we found a field. let's see what we've got
                let (c, f) = self::parse_field(&tok[i..], qd)?;
                fields.push(f);
                i += c;
                state = SchemaParseState::COMMA_OR_END;
            }
            (Token::Symbol(Symbol::SymComma), SchemaParseState::COMMA_OR_END) => {
                i += 1;
                // expect a field or close paren
                state = SchemaParseState::END_OR_FIELD;
            }
            (
                Token::Symbol(Symbol::TtCloseParen),
                SchemaParseState::COMMA_OR_END | SchemaParseState::END_OR_FIELD,
            ) => {
                i += 1;
                // end of stream
                break;
            }
            _ => {
                okay = false;
                break;
            }
        }
    }

    // model properties
    if !okay {
        return Err(LangError::UnexpectedToken);
    }

    let model_name = unsafe {
        // UNSAFE(@ohsayan): Now that we're sure that we have the model name ident, get it
        extract!(tok[0], Token::Ident(ref model_name) => model_name.clone())
    };

    if l > i && tok[i] == (Token![with]) {
        // we have some more input, and it should be a dict of properties
        i += 1; // +WITH

        // great, parse the dict
        let mut dict = Dict::new();
        let r = self::rfold_dict(DictFoldState::OB, &tok[i..], qd, &mut dict);
        i += (r & !HIBIT) as usize;

        if r & HIBIT == HIBIT {
            // sweet, so we got our dict
            Ok((
                Model {
                    model_name,
                    props: dict,
                    fields,
                },
                i,
            ))
        } else {
            Err(LangError::UnexpectedToken)
        }
    } else {
        // we've reached end of stream, so there's nothing more to parse
        Ok((
            Model {
                model_name,
                props: dict! {},
                fields,
            },
            i,
        ))
    }
}

#[inline(always)]
/// Parse space data from the given tokens
pub(super) fn parse_space_from_tokens<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
) -> LangResult<(Space<'a>, usize)> {
    let l = tok.len();
    let mut okay = !tok.is_empty() && tok[0].is_ident();
    let mut i = 0;
    i += okay as usize;
    // either we have `with` or nothing. don't be stupid
    let has_more_properties = i < l && tok[i] == Token![with];
    okay &= has_more_properties | (i == l);
    // properties
    let mut d = Dict::new();

    if has_more_properties && okay {
        let ret = self::rfold_dict(DictFoldState::OB, &tok[1..], qd, &mut d);
        i += (ret & !HIBIT) as usize;
        okay &= ret & HIBIT == HIBIT;
    }

    if okay {
        Ok((
            Space {
                space_name: unsafe { extract!(tok[0], Token::Ident(ref id) => id.clone()) },
                props: d,
            },
            i,
        ))
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[inline(always)]
/// Parse alter space from tokens
pub(super) fn parse_alter_space_from_tokens<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
) -> LangResult<(AlterSpace<'a>, usize)> {
    let mut i = 0;
    let l = tok.len();

    let okay = l > 3 && tok[0].is_ident() && tok[1] == Token![with] && tok[2] == Token![open {}];

    if !okay {
        return Err(LangError::UnexpectedToken);
    }

    let space_name = unsafe { extract!(tok[0], Token::Ident(ref space) => space.clone()) };

    i += 3;

    let mut d = Dict::new();
    let ret = rfold_dict(DictFoldState::CB_OR_IDENT, &tok[i..], qd, &mut d);
    i += (ret & !HIBIT) as usize;

    if ret & HIBIT == HIBIT {
        Ok((
            AlterSpace {
                space_name,
                updated_props: d,
            },
            i,
        ))
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[cfg(test)]
pub(super) fn alter_space_full<'a>(tok: &'a [Token]) -> LangResult<AlterSpace<'a>> {
    let (r, i) = self::parse_alter_space_from_tokens(tok, &mut InplaceData::new())?;
    assert_full_tt!(i, tok.len());
    Ok(r)
}

states! {
    /// The field syntax parse state
    pub struct FieldSyntaxParseState: u8 {
        IDENT = 0x00,
        OB = 0x01,
        FOLD_DICT_INCOMPLETE = 0x02,
        COMPLETED = 0xFF,
    }
}

#[derive(Debug, PartialEq)]
/// An [`ExpandedField`] is a full field definition with advanced metadata
pub struct ExpandedField<'a> {
    pub(super) field_name: Slice<'a>,
    pub(super) props: Dict,
    pub(super) layers: Vec<Layer<'a>>,
    pub(super) reset: bool,
}

#[cfg(test)]
pub fn parse_field_syntax_full<'a, const ALLOW_RESET: bool>(
    tok: &'a [Token],
) -> LangResult<(ExpandedField<'a>, usize)> {
    self::parse_field_syntax::<InplaceData, ALLOW_RESET>(tok, &mut InplaceData::new())
}

#[inline(always)]
/// Parse a field declared using the field syntax
pub(super) fn parse_field_syntax<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    tok: &'a [Token],
    qd: &mut Qd,
) -> LangResult<(ExpandedField<'a>, usize)> {
    let l = tok.len();
    let mut i = 0_usize;
    let mut state = FieldSyntaxParseState::IDENT;
    let mut okay = true;
    let mut tmp = MaybeInit::uninit();
    let mut props = Dict::new();
    let mut layers = vec![];
    let mut reset = false;
    while i < l && okay {
        match (&tok[i], state) {
            (Token::Ident(field), FieldSyntaxParseState::IDENT) => {
                i += 1;
                tmp = MaybeInit::new(field.clone());
                // expect open brace
                state = FieldSyntaxParseState::OB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), FieldSyntaxParseState::OB) => {
                i += 1;
                let r = self::rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    &tok[i..],
                    qd,
                    &mut props,
                );
                okay &= r.is_okay();
                i += r.pos();
                if r.has_more() && i < l {
                    // now parse layers
                    let r = self::rfold_layers::<Qd, ALLOW_RESET>(
                        LayerFoldState::TY,
                        &tok[i..],
                        qd,
                        &mut layers,
                    );
                    okay &= r & HIBIT == HIBIT;
                    i += (r & !HIBIT) as usize;
                    state = FieldSyntaxParseState::FOLD_DICT_INCOMPLETE;
                } else {
                    okay = false;
                    break;
                }
            }
            (Token::Symbol(Symbol::SymComma), FieldSyntaxParseState::FOLD_DICT_INCOMPLETE) => {
                i += 1;
                let r = self::rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    &tok[i..],
                    qd,
                    &mut props,
                );
                okay &= r.is_okay() && !r.has_more();
                i += r.pos();
                reset = ALLOW_RESET && r.should_reset();
                if okay {
                    state = FieldSyntaxParseState::COMPLETED;
                    break;
                }
            }
            (Token::Symbol(Symbol::TtCloseBrace), FieldSyntaxParseState::FOLD_DICT_INCOMPLETE) => {
                i += 1;
                // great, were done
                state = FieldSyntaxParseState::COMPLETED;
                break;
            }
            _ => {
                okay = false;
                break;
            }
        }
    }
    okay &= state == FieldSyntaxParseState::COMPLETED;
    if okay {
        Ok((
            ExpandedField {
                field_name: unsafe { tmp.assume_init() },
                layers,
                props,
                reset,
            },
            i,
        ))
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Alter<'a> {
    model: Slice<'a>,
    kind: AlterKind<'a>,
}

impl<'a> Alter<'a> {
    #[inline(always)]
    pub(super) fn new(model: Slice<'a>, kind: AlterKind<'a>) -> Self {
        Self { model, kind }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// The alter operation kind
pub enum AlterKind<'a> {
    Add(Box<[ExpandedField<'a>]>),
    Remove(Box<[Slice<'a>]>),
    Update(Box<[ExpandedField<'a>]>),
}

#[inline(always)]
/// Parse an [`AlterKind`] from the given token stream
pub(super) fn parse_alter_kind_from_tokens<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
    current: &mut usize,
) -> LangResult<Alter<'a>> {
    let l = tok.len();
    let okay = l > 2 && tok[0].is_ident();
    if !okay {
        return Err(LangError::UnexpectedEndofStatement);
    }
    *current += 2;
    let model_name = unsafe { extract!(tok[0], Token::Ident(ref l) => l.clone()) };
    match tok[1] {
        Token![add] => alter_add(&tok[1..], qd, current)
            .map(AlterKind::Add)
            .map(|kind| Alter::new(model_name, kind)),
        Token![remove] => alter_remove(&tok[1..], current)
            .map(AlterKind::Remove)
            .map(|kind| Alter::new(model_name, kind)),
        Token![update] => alter_update(&tok[1..], qd, current)
            .map(AlterKind::Update)
            .map(|kind| Alter::new(model_name, kind)),
        _ => return Err(LangError::ExpectedStatement),
    }
}

#[inline(always)]
/// Parse multiple fields declared using the field syntax. Flag setting allows or disallows reset syntax
pub(super) fn parse_multiple_field_syntax<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    tok: &'a [Token],
    qd: &mut Qd,
    current: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    const DEFAULT_ADD_COL_CNT: usize = 4;
    /*
        WARNING: No trailing commas allowed

        <add> ::= (<field_syntax> <comma>)*

        Smallest length:
        alter model add myfield { type string };
    */
    let l = tok.len();
    if l < 5 {
        return Err(LangError::UnexpectedEndofStatement);
    }
    match tok[0] {
        Token::Ident(_) => {
            let (r, i) = parse_field_syntax::<Qd, ALLOW_RESET>(&tok, qd)?;
            *current += i;
            Ok([r].into())
        }
        Token::Symbol(Symbol::TtOpenParen) => {
            let mut i = 1;
            let mut okay = true;
            let mut stop = false;
            let mut cols = Vec::with_capacity(DEFAULT_ADD_COL_CNT);
            while i < l && okay && !stop {
                match tok[i] {
                    Token::Ident(_) => {
                        let (r, cnt) = parse_field_syntax::<Qd, ALLOW_RESET>(&tok[i..], qd)?;
                        i += cnt;
                        cols.push(r);
                        let nx_comma = i < l && tok[i] == Token::Symbol(Symbol::SymComma);
                        let nx_close = i < l && tok[i] == Token::Symbol(Symbol::TtCloseParen);
                        stop = nx_close;
                        okay &= nx_comma | nx_close;
                        i += (nx_comma | nx_close) as usize;
                    }
                    _ => {
                        okay = false;
                        break;
                    }
                }
            }
            *current += i;
            if okay && stop {
                Ok(cols.into_boxed_slice())
            } else {
                Err(LangError::UnexpectedToken)
            }
        }
        _ => Err(LangError::ExpectedStatement),
    }
}

#[inline(always)]
/// Parse the expression for `alter model <> add (..)`
pub(super) fn alter_add<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
    current: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    self::parse_multiple_field_syntax::<Qd, DISALLOW_RESET_SYNTAX>(tok, qd, current)
}

#[cfg(test)]
pub(super) fn alter_add_full<'a>(
    tok: &'a [Token],
    current: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    self::alter_add(tok, &mut InplaceData::new(), current)
}

#[inline(always)]
/// Parse the expression for `alter model <> remove (..)`
pub(super) fn alter_remove<'a>(
    tok: &'a [Token],
    current: &mut usize,
) -> LangResult<Box<[Slice<'a>]>> {
    const DEFAULT_REMOVE_COL_CNT: usize = 4;
    /*
        WARNING: No trailing commas allowed
        <remove> ::= <ident> | <openparen> (<ident> <comma>)*<closeparen>
    */
    if tok.is_empty() {
        return Err(LangError::UnexpectedEndofStatement);
    }

    let r = match &tok[0] {
        Token::Ident(id) => {
            *current += 1;
            Box::new([id.clone()])
        }
        Token::Symbol(Symbol::TtOpenParen) => {
            let l = tok.len();
            let mut i = 1_usize;
            let mut okay = true;
            let mut stop = false;
            let mut cols = Vec::with_capacity(DEFAULT_REMOVE_COL_CNT);
            while i < tok.len() && okay && !stop {
                match tok[i] {
                    Token::Ident(ref ident) => {
                        cols.push(ident.clone());
                        i += 1;
                        let nx_comma = i < l && tok[i] == (Token::Symbol(Symbol::SymComma));
                        let nx_close = i < l && tok[i] == (Token::Symbol(Symbol::TtCloseParen));
                        okay &= nx_comma | nx_close;
                        stop = nx_close;
                        i += (nx_comma | nx_close) as usize;
                    }
                    _ => {
                        okay = false;
                        break;
                    }
                }
            }
            *current += i;
            if okay && stop {
                cols.into_boxed_slice()
            } else {
                return Err(LangError::UnexpectedToken);
            }
        }
        _ => return Err(LangError::ExpectedStatement),
    };
    Ok(r)
}

#[inline(always)]
/// Parse the expression for `alter model <> update (..)`
pub(super) fn alter_update<'a, Qd: QueryData<'a>>(
    tok: &'a [Token],
    qd: &mut Qd,
    current: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    self::parse_multiple_field_syntax::<Qd, ALLOW_RESET_SYNTAX>(tok, qd, current)
}

#[cfg(test)]
pub(super) fn alter_update_full<'a>(
    tok: &'a [Token],
    i: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    self::alter_update(tok, &mut InplaceData::new(), i)
}
