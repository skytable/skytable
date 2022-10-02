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

use super::lexer::DmlKeyword;

use {
    super::{
        lexer::{DdlKeyword, DdlMiscKeyword, Keyword, Lit, MiscKeyword, Symbol, Token, Type},
        LangError, LangResult, RawSlice,
    },
    std::{
        collections::{HashMap, HashSet},
        mem::MaybeUninit,
    },
};

/*
    Meta
*/

/// This macro constructs states for our machine
///
/// **DO NOT** construct states manually
macro_rules! states {
    ($(#[$attr:meta])+$vis:vis struct $stateid:ident: $statebase:ty {$($(#[$tyattr:meta])*$v:vis$state:ident = $statexp:expr),+ $(,)?}) => {
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

type StaticStr = &'static str;

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
    ty: Type,
    props: Dict,
}

impl Layer {
    pub(super) const fn new(ty: Type, props: Dict) -> Self {
        Self { ty, props }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
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
pub struct Field {
    pub(super) field_name: RawSlice,
    pub(super) layers: Vec<Layer>,
    pub(super) props: HashSet<StaticStr>,
}

#[derive(Debug, PartialEq)]
/// A model definition
pub struct Model {
    pub(super) model_name: RawSlice,
    pub(super) fields: Vec<Field>,
    pub(super) props: Dict,
}

#[derive(Debug, PartialEq)]
pub struct Space {
    pub(super) space_name: RawSlice,
    pub(super) props: Dict,
}

#[derive(Debug, PartialEq)]
pub struct AlterSpace {
    pub(super) space_name: RawSlice,
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

pub(super) fn rfold_dict(mut state: DictFoldState, tok: &[Token], dict: &mut Dict) -> u64 {
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
    let mut tmp = MaybeUninit::uninit();

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
                tmp = MaybeUninit::new(unsafe { id.as_str() });
                state = DictFoldState::COLON;
            }
            (Token::Symbol(Symbol::SymColon), DictFoldState::COLON) => {
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
            (Token::Symbol(Symbol::SymComma), DictFoldState::COMMA_OR_CB) => {
                i += 1;
                // we found a comma, expect a *strict* brace close or ident
                state = DictFoldState::CB_OR_IDENT;
            }
            (Token::Symbol(Symbol::TtOpenBrace), DictFoldState::LIT_OR_OB) => {
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

#[cfg(test)]
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
    #[inline(always)]
    const fn new() -> Self {
        Self {
            c: 0,
            b: [true, false],
        }
    }
    #[inline(always)]
    fn incr(&mut self) {
        self.incr_by(1)
    }
    #[inline(always)]
    fn incr_by(&mut self, by: usize) {
        self.c += by;
    }
    #[inline(always)]
    fn set_fail(&mut self) {
        self.b[0] = false;
    }
    #[inline(always)]
    fn set_has_more(&mut self) {
        self.b[1] = true;
    }
    #[inline(always)]
    pub fn pos(&self) -> usize {
        self.c
    }
    #[inline(always)]
    pub fn has_more(&self) -> bool {
        self.b[1]
    }
    #[inline(always)]
    pub fn is_okay(&self) -> bool {
        self.b[0]
    }
    #[inline(always)]
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
    let mut tmp = MaybeUninit::uninit();
    while r.pos() < l && r.is_okay() {
        match (&tok[r.pos()], state) {
            (
                Token::Keyword(Keyword::DdlMisc(DdlMiscKeyword::Type)),
                TyMetaFoldState::IDENT_OR_CB,
            ) => {
                // we were expecting an ident but found the type keyword! increase depth
                r.incr();
                r.set_has_more();
                state = TyMetaFoldState::FINAL;
                break;
            }
            (
                Token::Symbol(Symbol::TtCloseBrace),
                TyMetaFoldState::IDENT_OR_CB | TyMetaFoldState::COMMA_OR_CB,
            ) => {
                r.incr();
                // found close brace. end of stream
                state = TyMetaFoldState::FINAL;
                break;
            }
            (Token::Ident(ident), TyMetaFoldState::IDENT_OR_CB) => {
                r.incr();
                tmp = MaybeUninit::new(unsafe { ident.as_str() });
                // we just saw an ident, so we expect to see a colon
                state = TyMetaFoldState::COLON;
            }
            (Token::Symbol(Symbol::SymColon), TyMetaFoldState::COLON) => {
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
            (Token::Symbol(Symbol::SymComma), TyMetaFoldState::COMMA_OR_CB) => {
                r.incr();
                // next is strictly a close brace or ident
                state = TyMetaFoldState::IDENT_OR_CB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), TyMetaFoldState::LIT_OR_OB) => {
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

#[cfg(test)]
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

pub(super) fn rfold_layers(start: LayerFoldState, tok: &[Token], layers: &mut Vec<Layer>) -> u64 {
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
    let mut tmp = MaybeUninit::uninit();
    let mut dict = Dict::new();
    while i < l && okay {
        match (&tok[i], state) {
            (Token::Keyword(Keyword::TypeId(ty)), LayerFoldState::TY) => {
                i += 1;
                // expecting type, and found type. next is either end or an open brace or some arbitrary token
                tmp = MaybeUninit::new(ty);
                state = LayerFoldState::END_OR_OB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), LayerFoldState::END_OR_OB) => {
                i += 1;
                // since we found an open brace, this type has some meta
                let ret = rfold_tymeta(TyMetaFoldState::IDENT_OR_CB, &tok[i..], &mut dict);
                i += ret.pos();
                okay &= ret.is_okay();
                if ret.has_more() {
                    // more layers
                    let ret = rfold_layers(LayerFoldState::TY, &tok[i..], layers);
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
            (Token::Symbol(Symbol::SymComma), LayerFoldState::FOLD_DICT_INCOMPLETE) => {
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
            (Token::Symbol(Symbol::TtCloseBrace), LayerFoldState::FOLD_DICT_INCOMPLETE) => {
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

#[cfg(test)]
#[inline(always)]
pub(super) fn fold_layers(tok: &[Token]) -> (Vec<Layer>, usize, bool) {
    let mut l = Vec::new();
    let r = rfold_layers(LayerFoldState::TY, tok, &mut l);
    (l, (r & !HIBIT) as _, r & HIBIT == HIBIT)
}

#[inline(always)]
pub(super) fn collect_field_properties(tok: &[Token]) -> (FieldProperties, u64) {
    let mut props = FieldProperties::default();
    let mut i = 0;
    let mut okay = true;
    while i < tok.len() {
        match &tok[i] {
            Token::Keyword(Keyword::Ddl(DdlKeyword::Primary)) => {
                okay &= props.properties.insert(FieldProperties::PRIMARY)
            }
            Token::Keyword(Keyword::Misc(MiscKeyword::Null)) => {
                okay &= props.properties.insert(FieldProperties::NULL)
            }
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
pub(super) fn parse_field_properties(tok: &[Token]) -> (FieldProperties, usize, bool) {
    let (p, r) = collect_field_properties(tok);
    (p, (r & !HIBIT) as _, r & HIBIT == HIBIT)
}

#[inline(always)]
pub(super) fn parse_field(tok: &[Token]) -> LangResult<(usize, Field)> {
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
    let r = rfold_layers(LayerFoldState::TY, &tok[i..], &mut layers);
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

/*
    create model name(..) with { .. }
                     ^^^^
*/

states! {
    ///
    pub struct SchemaParseState: u8 {
        OPEN_PAREN = 0x00,
        FIELD = 0x01,
        COMMA_OR_END = 0x02,
        END_OR_FIELD = 0x03,
    }
}

#[inline(always)]
pub(super) fn parse_schema_from_tokens(
    tok: &[Token],
    model_name: RawSlice,
) -> LangResult<(Model, usize)> {
    // parse fields
    let l = tok.len();
    let mut i = 0;
    let mut state = SchemaParseState::OPEN_PAREN;
    let mut okay = true;
    let mut fields = Vec::with_capacity(2);

    while i < l && okay {
        match (&tok[i], state) {
            (Token::Symbol(Symbol::TtOpenParen), SchemaParseState::OPEN_PAREN) => {
                i += 1;
                state = SchemaParseState::FIELD;
            }
            (
                Token::Keyword(Keyword::Ddl(DdlKeyword::Primary))
                | Token::Keyword(Keyword::Misc(MiscKeyword::Null))
                | Token::Ident(_),
                SchemaParseState::FIELD | SchemaParseState::END_OR_FIELD,
            ) => {
                // fine, we found a field. let's see what we've got
                let (c, f) = self::parse_field(&tok[i..])?;
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

    if l > i && tok[i] == (Token::Keyword(Keyword::DdlMisc(DdlMiscKeyword::With))) {
        // we have some more input, and it should be a dict of properties
        i += 1; // +WITH

        // great, parse the dict
        let mut dict = Dict::new();
        let r = self::rfold_dict(DictFoldState::OB, &tok[i..], &mut dict);
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
pub(super) fn parse_space_from_tokens(tok: &[Token], s: RawSlice) -> LangResult<(Space, usize)> {
    // let's see if the cursor is at `with`. ignore other tokens because that's fine
    if !tok.is_empty() && tok[0] == (Token::Keyword(Keyword::DdlMisc(DdlMiscKeyword::With))) {
        // we have a dict
        let mut d = Dict::new();
        let ret = self::rfold_dict(DictFoldState::OB, &tok[1..], &mut d);
        if ret & HIBIT == HIBIT {
            Ok((
                Space {
                    space_name: s,
                    props: d,
                },
                (ret & !HIBIT) as _,
            ))
        } else {
            Err(LangError::UnexpectedToken)
        }
    } else {
        Ok((
            Space {
                space_name: s,
                props: dict! {},
            },
            0,
        ))
    }
}

pub(super) fn parse_alter_space_from_tokens(
    tok: &[Token],
    space_name: RawSlice,
) -> LangResult<(AlterSpace, usize)> {
    let mut i = 0;
    let l = tok.len();

    let invalid = l < 3
        || !(tok[i] == (Token::Keyword(Keyword::DdlMisc(DdlMiscKeyword::With)))
            && tok[i + 1] == (Token::Symbol(Symbol::TtOpenBrace)));

    if invalid {
        return Err(LangError::UnexpectedToken);
    }

    i += 2;

    let mut d = Dict::new();
    let ret = rfold_dict(DictFoldState::CB_OR_IDENT, &tok[i..], &mut d);
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
pub(super) struct ExpandedField {
    pub(super) field_name: RawSlice,
    pub(super) props: Dict,
    pub(super) layers: Vec<Layer>,
}

pub(super) fn parse_field_syntax(tok: &[Token]) -> LangResult<(ExpandedField, usize)> {
    let l = tok.len();
    let mut i = 0_usize;
    let mut state = FieldSyntaxParseState::IDENT;
    let mut okay = true;
    let mut tmp = MaybeUninit::uninit();
    let mut props = Dict::new();
    let mut layers = vec![];
    while i < l && okay {
        match (&tok[i], state) {
            (Token::Ident(field), FieldSyntaxParseState::IDENT) => {
                i += 1;
                tmp = MaybeUninit::new(field.clone());
                // expect open brace
                state = FieldSyntaxParseState::OB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), FieldSyntaxParseState::OB) => {
                i += 1;
                let r = self::rfold_tymeta(TyMetaFoldState::IDENT_OR_CB, &tok[i..], &mut props);
                okay &= r.is_okay();
                i += r.pos();
                if r.has_more() && i < l {
                    // now parse layers
                    let r = self::rfold_layers(LayerFoldState::TY, &tok[i..], &mut layers);
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
                let r = self::rfold_dict(DictFoldState::CB_OR_IDENT, &tok[i..], &mut props);
                okay &= r & HIBIT == HIBIT;
                i += (r & !HIBIT) as usize;
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
            },
            i,
        ))
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[derive(Debug)]
#[cfg_attr(debug_assertions, derive(PartialEq))]
pub(super) enum AlterKind {
    Add(Field),
    Remove(Box<[RawSlice]>),
    Update(ExpandedField),
}

#[inline(always)]
pub(super) fn parse_alter_kind_from_tokens(
    tok: &[Token],
    current: &mut usize,
) -> LangResult<AlterKind> {
    let l = tok.len();
    let mut i = 0;
    if l < 2 {
        return Err(LangError::UnexpectedEndofStatement);
    }
    *current += 1;
    let r = match tok[i] {
        Token::Keyword(Keyword::DdlMisc(DdlMiscKeyword::Add)) => {
            AlterKind::Add(alter_add(&tok[1..], &mut i))
        }
        Token::Keyword(Keyword::DdlMisc(DdlMiscKeyword::Remove)) => {
            AlterKind::Remove(alter_remove(&tok[1..], &mut i)?)
        }
        Token::Keyword(Keyword::Dml(DmlKeyword::Update)) => {
            AlterKind::Update(alter_update(&tok[1..], &mut i))
        }
        _ => return Err(LangError::ExpectedStatement),
    };
    *current += i;
    Ok(r)
}

#[inline(always)]
pub(super) fn alter_add(_tok: &[Token], _current: &mut usize) -> Field {
    todo!()
}

#[inline(always)]
pub(super) fn alter_remove(tok: &[Token], current: &mut usize) -> LangResult<Box<[RawSlice]>> {
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
pub(super) fn alter_update(_tok: &[Token], _current: &mut usize) -> ExpandedField {
    todo!()
}
