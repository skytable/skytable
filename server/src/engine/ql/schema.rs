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

#[cfg(test)]
use crate::engine::ql::ast::InplaceData;
use {
    super::{
        ast::{QueryData, State},
        lex::{LitIR, LitIROwned, Slice, Symbol, Token},
        LangError, LangResult,
    },
    crate::util::{compiler, MaybeInit},
    core::str,
    std::collections::{HashMap, HashSet},
};

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
    mut mstate: DictFoldState,
    state: &mut State<'a, Qd>,
    dict: &mut Dict,
) {
    /*
        NOTE: Assume rules wherever applicable

        <openbrace> ::= "{"
        <closebrace> ::= "}"
        <comma> ::= ","
        <colon> ::= ":"
        <dict> ::= <openbrace> (<ident> <colon> (<lit> | <dict>) <comma>)* <comma>* <closebrace>
    */
    let mut tmp = MaybeInit::uninit();

    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token::Symbol(Symbol::TtOpenBrace), DictFoldState::OB) => {
                // we found a brace, expect a close brace or an ident
                mstate = DictFoldState::CB_OR_IDENT;
            }
            (
                Token::Symbol(Symbol::TtCloseBrace),
                DictFoldState::CB_OR_IDENT | DictFoldState::COMMA_OR_CB,
            ) => {
                // end of stream
                mstate = DictFoldState::FINAL;
                break;
            }
            (Token::Ident(id), DictFoldState::CB_OR_IDENT) => {
                // found ident, so expect colon
                tmp = MaybeInit::new(unsafe { str::from_utf8_unchecked(id) });
                mstate = DictFoldState::COLON;
            }
            (Token::Symbol(Symbol::SymColon), DictFoldState::COLON) => {
                // found colon, expect literal or openbrace
                mstate = DictFoldState::LIT_OR_OB;
            }
            (tok, DictFoldState::LIT_OR_OB) if state.can_read_lit_from(tok) => {
                // found literal; so push in k/v pair and then expect a comma or close brace
                unsafe {
                    let v = Some(state.read_lit_unchecked_from(tok).into());
                    state
                        .poison_if_not(dict.insert(tmp.assume_init_ref().to_string(), v).is_none());
                }
                mstate = DictFoldState::COMMA_OR_CB;
            }
            (Token![null], DictFoldState::LIT_OR_OB) => {
                // null
                state.poison_if_not(
                    dict.insert(unsafe { tmp.assume_init_ref() }.to_string(), None)
                        .is_none(),
                );
                mstate = DictFoldState::COMMA_OR_CB;
            }
            // ONLY COMMA CAPTURE
            (Token::Symbol(Symbol::SymComma), DictFoldState::COMMA_OR_CB) => {
                // we found a comma, expect a *strict* brace close or ident
                mstate = DictFoldState::CB_OR_IDENT;
            }
            (Token::Symbol(Symbol::TtOpenBrace), DictFoldState::LIT_OR_OB) => {
                // we found an open brace, so this is a dict
                let mut new_dict = Dict::new();
                rfold_dict(DictFoldState::CB_OR_IDENT, state, &mut new_dict);
                state.poison_if_not(
                    dict.insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        Some(new_dict.into()),
                    )
                    .is_none(),
                );
                // at the end of a dict we either expect a comma or close brace
                mstate = DictFoldState::COMMA_OR_CB;
            }
            _ => {
                state.poison();
                state.cursor_back();
                break;
            }
        }
    }
    state.poison_if_not(mstate == DictFoldState::FINAL);
}

#[cfg(test)]
/// Fold a dictionary (**test-only**)
pub fn fold_dict(tok: &[Token]) -> Option<Dict> {
    let mut d = Dict::new();
    let mut state = State::new(tok, InplaceData::new());
    rfold_dict(DictFoldState::OB, &mut state, &mut d);
    state.okay().then_some(d)
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
pub struct TyMetaReturn {
    more: bool,
    reset: bool,
}

impl TyMetaReturn {
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            more: false,
            reset: false,
        }
    }
    #[inline(always)]
    pub const fn has_more(&self) -> bool {
        self.more
    }
    #[inline(always)]
    pub const fn has_reset(&self) -> bool {
        self.reset
    }
    #[inline(always)]
    pub fn set_has_more(&mut self) {
        self.more = true;
    }
    #[inline(always)]
    pub fn set_has_reset(&mut self) {
        self.reset = true;
    }
}

/// Fold type metadata (flag setup dependent on caller)
pub(super) fn rfold_tymeta<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    mut mstate: TyMetaFoldState,
    state: &mut State<'a, Qd>,
    dict: &mut Dict,
) -> TyMetaReturn {
    let mut tmp = MaybeInit::uninit();
    let mut tymr = TyMetaReturn::new();
    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token![type], TyMetaFoldState::IDENT_OR_CB) => {
                // we were expecting an ident but found the type keyword! increase depth
                tymr.set_has_more();
                mstate = TyMetaFoldState::FINAL;
                break;
            }
            (Token![.], TyMetaFoldState::IDENT_OR_CB) if ALLOW_RESET => {
                let reset = state.cursor_rounded_eq(Token![.]);
                state.cursor_ahead_if(reset);
                tymr.set_has_reset();
                state.poison_if_not(reset);
                mstate = TyMetaFoldState::CB;
            }
            (
                Token::Symbol(Symbol::TtCloseBrace),
                TyMetaFoldState::IDENT_OR_CB | TyMetaFoldState::COMMA_OR_CB | TyMetaFoldState::CB,
            ) => {
                // found close brace. end of stream
                mstate = TyMetaFoldState::FINAL;
                break;
            }
            (Token::Ident(ident), TyMetaFoldState::IDENT_OR_CB) => {
                tmp = MaybeInit::new(unsafe { str::from_utf8_unchecked(ident) });
                // we just saw an ident, so we expect to see a colon
                mstate = TyMetaFoldState::COLON;
            }
            (Token::Symbol(Symbol::SymColon), TyMetaFoldState::COLON) => {
                // we just saw a colon. now we want a literal or openbrace
                mstate = TyMetaFoldState::LIT_OR_OB;
            }
            (tok, TyMetaFoldState::LIT_OR_OB) if state.can_read_lit_from(tok) => {
                unsafe {
                    let v = Some(state.read_lit_unchecked_from(tok).into());
                    state
                        .poison_if_not(dict.insert(tmp.assume_init_ref().to_string(), v).is_none());
                }
                // saw a literal. next is either comma or close brace
                mstate = TyMetaFoldState::COMMA_OR_CB;
            }
            (Token![null], TyMetaFoldState::LIT_OR_OB) => {
                state.poison_if_not(
                    dict.insert(unsafe { tmp.assume_init_ref() }.to_string(), None)
                        .is_none(),
                );
                // saw null, start parsing another entry
                mstate = TyMetaFoldState::COMMA_OR_CB;
            }
            (Token::Symbol(Symbol::SymComma), TyMetaFoldState::COMMA_OR_CB) => {
                // next is strictly a close brace or ident
                mstate = TyMetaFoldState::IDENT_OR_CB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), TyMetaFoldState::LIT_OR_OB) => {
                // another dict in here
                let mut nd = Dict::new();
                let ret =
                    rfold_tymeta::<Qd, ALLOW_RESET>(TyMetaFoldState::IDENT_OR_CB, state, &mut nd);
                // L2 cannot have type definitions
                state.poison_if(ret.has_more());
                // end of definition or comma followed by something
                state.poison_if_not(
                    dict.insert(
                        unsafe { tmp.assume_init_ref() }.to_string(),
                        Some(nd.into()),
                    )
                    .is_none(),
                );
                mstate = TyMetaFoldState::COMMA_OR_CB;
            }
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }
    state.poison_if_not(mstate == TyMetaFoldState::FINAL);
    tymr
}

#[cfg(test)]
/// (**test-only**) fold type metadata
pub(super) fn fold_tymeta(tok: &[Token]) -> (TyMetaReturn, bool, usize, Dict) {
    let mut state = State::new(tok, InplaceData::new());
    let mut d = Dict::new();
    let ret = rfold_tymeta::<InplaceData, DISALLOW_RESET_SYNTAX>(
        TyMetaFoldState::IDENT_OR_CB,
        &mut state,
        &mut d,
    );
    (ret, state.okay(), state.cursor(), d)
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
    state: &mut State<'a, Qd>,
    layers: &mut Vec<Layer<'a>>,
) {
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
    let mut mstate = start;
    let mut tmp = MaybeInit::uninit();
    let mut dict = Dict::new();
    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token::Ident(ty), LayerFoldState::TY) => {
                // expecting type, and found type. next is either end or an open brace or some arbitrary token
                tmp = MaybeInit::new(ty.clone());
                mstate = LayerFoldState::END_OR_OB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), LayerFoldState::END_OR_OB) => {
                // since we found an open brace, this type has some meta
                let ret =
                    rfold_tymeta::<Qd, ALLOW_RESET>(TyMetaFoldState::IDENT_OR_CB, state, &mut dict);
                if ret.has_more() {
                    // more layers
                    rfold_layers::<Qd, ALLOW_RESET>(LayerFoldState::TY, state, layers);
                    mstate = LayerFoldState::FOLD_DICT_INCOMPLETE;
                } else if state.okay() {
                    // done folding dictionary. nothing more expected. break
                    mstate = LayerFoldState::FOLD_COMPLETED;
                    layers.push(Layer {
                        ty: unsafe { tmp.assume_init() }.clone(),
                        props: dict,
                        reset: ret.has_reset(),
                    });
                    break;
                }
            }
            (Token::Symbol(Symbol::SymComma), LayerFoldState::FOLD_DICT_INCOMPLETE) => {
                // there is a comma at the end of this
                let ret =
                    rfold_tymeta::<Qd, ALLOW_RESET>(TyMetaFoldState::IDENT_OR_CB, state, &mut dict);
                state.poison_if(ret.has_more()); // not more than one type depth
                if state.okay() {
                    // done folding dict successfully. nothing more expected. break.
                    mstate = LayerFoldState::FOLD_COMPLETED;
                    layers.push(Layer {
                        ty: unsafe { tmp.assume_init() }.clone(),
                        props: dict,
                        reset: ret.has_reset(),
                    });
                    break;
                }
            }
            (Token::Symbol(Symbol::TtCloseBrace), LayerFoldState::FOLD_DICT_INCOMPLETE) => {
                // end of stream
                mstate = LayerFoldState::FOLD_COMPLETED;
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() }.clone(),
                    props: dict,
                    reset: false,
                });
                break;
            }
            (_, LayerFoldState::END_OR_OB) => {
                state.cursor_back();
                // random arbitrary byte. finish append
                mstate = LayerFoldState::FOLD_COMPLETED;
                layers.push(Layer {
                    ty: unsafe { tmp.assume_init() }.clone(),
                    props: dict,
                    reset: false,
                });
                break;
            }
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }
    state.poison_if_not(mstate == LayerFoldState::FOLD_COMPLETED);
}

#[cfg(test)]
#[inline(always)]
/// (**test-only**) fold layers
pub(super) fn fold_layers<'a>(tok: &'a [Token]) -> (Vec<Layer<'a>>, usize, bool) {
    let mut state = State::new(tok, InplaceData::new());
    let mut l = Vec::new();
    rfold_layers::<InplaceData, DISALLOW_RESET_SYNTAX>(LayerFoldState::TY, &mut state, &mut l);
    (l, state.consumed(), state.okay())
}

#[inline(always)]
/// Collect field properties
pub(super) fn collect_field_properties<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> FieldProperties {
    let mut props = FieldProperties::default();
    while state.loop_tt() {
        match state.fw_read() {
            Token![primary] => {
                state.poison_if_not(props.properties.insert(FieldProperties::PRIMARY))
            }
            Token![null] => state.poison_if_not(props.properties.insert(FieldProperties::NULL)),
            Token::Ident(_) => {
                state.cursor_back();
                break;
            }
            _ => {
                // we could pass this over to the caller, but it's better if we do it since we're doing
                // a linear scan anyways
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }
    props
}

#[cfg(test)]
#[inline(always)]
/// (**test-only**) parse field properties
pub(super) fn parse_field_properties(tok: &[Token]) -> (FieldProperties, usize, bool) {
    let mut state = State::new(tok, InplaceData::new());
    let p = collect_field_properties(&mut state);
    (p, state.cursor(), state.okay())
}

#[cfg(test)]
pub(super) fn parse_field_full<'a>(tok: &'a [Token]) -> LangResult<(usize, Field<'a>)> {
    let mut state = State::new(tok, InplaceData::new());
    self::parse_field(&mut state).map(|field| (state.cursor(), field))
}

#[inline(always)]
/// Parse a field using the declaration-syntax (not field syntax)
///
/// Expected start token: field name (ident)
pub(super) fn parse_field<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<Field<'a>> {
    // parse field properties
    let props = collect_field_properties(state);
    // if exhauted or broken, simply return
    if compiler::unlikely(state.exhausted() | !state.okay() || state.remaining() == 1) {
        return Err(LangError::UnexpectedEndofStatement);
    }
    // field name
    let field_name = match (state.fw_read(), state.fw_read()) {
        (Token::Ident(id), Token![:]) => id,
        _ => return Err(LangError::UnexpectedToken),
    };

    // layers
    let mut layers = Vec::new();
    rfold_layers::<Qd, DISALLOW_RESET_SYNTAX>(LayerFoldState::TY, state, &mut layers);
    if state.okay() {
        Ok(Field {
            field_name: field_name.clone(),
            layers,
            props: props.properties,
        })
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
    let mut state = State::new(tok, InplaceData::new());
    self::parse_model_from_tokens::<InplaceData>(&mut state).map(|model| (model, state.cursor()))
}

#[inline(always)]
/// Parse a fresh schema with declaration-syntax fields
pub(super) fn parse_model_from_tokens<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<Model<'a>> {
    // parse fields
    // check if we have our model name
    // smallest model declaration: create model mymodel(username: string, password: binary) -> 10 tokens
    if compiler::unlikely(state.remaining() < 10) {
        return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
    }
    let model_name = state.fw_read();
    state.poison_if_not(model_name.is_ident());
    let mut fields = Vec::with_capacity(2);
    let mut mstate = SchemaParseState::OPEN_PAREN;

    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token::Symbol(Symbol::TtOpenParen), SchemaParseState::OPEN_PAREN) => {
                mstate = SchemaParseState::FIELD;
            }
            (
                Token![primary] | Token![null] | Token::Ident(_),
                SchemaParseState::FIELD | SchemaParseState::END_OR_FIELD,
            ) => {
                state.cursor_back();
                // fine, we found a field. let's see what we've got
                let f = self::parse_field(state)?;
                fields.push(f);
                mstate = SchemaParseState::COMMA_OR_END;
            }
            (Token::Symbol(Symbol::SymComma), SchemaParseState::COMMA_OR_END) => {
                // expect a field or close paren
                mstate = SchemaParseState::END_OR_FIELD;
            }
            (
                Token::Symbol(Symbol::TtCloseParen),
                SchemaParseState::COMMA_OR_END | SchemaParseState::END_OR_FIELD,
            ) => {
                // end of stream
                break;
            }
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }

    // model properties
    if !state.okay() {
        return Err(LangError::UnexpectedToken);
    }

    let model_name = unsafe {
        // UNSAFE(@ohsayan): Now that we're sure that we have the model name ident, get it
        extract!(model_name, Token::Ident(ref model_name) => model_name.clone())
    };

    if state.cursor_rounded_eq(Token![with]) {
        // we have some more input, and it should be a dict of properties
        state.cursor_ahead(); // +WITH

        // great, parse the dict
        let mut dict = Dict::new();
        self::rfold_dict(DictFoldState::OB, state, &mut dict);

        if state.okay() {
            // sweet, so we got our dict
            Ok(Model {
                model_name,
                props: dict,
                fields,
            })
        } else {
            Err(LangError::UnexpectedToken)
        }
    } else {
        // we've reached end of stream, so there's nothing more to parse
        Ok(Model {
            model_name,
            props: dict! {},
            fields,
        })
    }
}

#[inline(always)]
/// Parse space data from the given tokens
pub(super) fn parse_space_from_tokens<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<Space<'a>> {
    // smallest declaration: `create space myspace` -> >= 1 token
    if compiler::unlikely(state.remaining() < 1) {
        return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
    }
    let space_name = state.fw_read();
    state.poison_if_not(space_name.is_ident());
    // either we have `with` or nothing. don't be stupid
    let has_more_properties = state.cursor_rounded_eq(Token![with]);
    state.poison_if_not(has_more_properties | state.exhausted());
    state.cursor_ahead_if(has_more_properties); // +WITH
    let mut d = Dict::new();
    // properties
    if has_more_properties && state.okay() {
        self::rfold_dict(DictFoldState::OB, state, &mut d);
    }
    if state.okay() {
        Ok(Space {
            space_name: unsafe { extract!(space_name, Token::Ident(ref id) => id.clone()) },
            props: d,
        })
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[inline(always)]
/// Parse alter space from tokens
pub(super) fn parse_alter_space_from_tokens<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<AlterSpace<'a>> {
    if compiler::unlikely(state.remaining() <= 3) {
        return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
    }
    let space_name = state.fw_read();
    state.poison_if_not(state.cursor_eq(Token![with]));
    state.cursor_ahead(); // ignore errors
    state.poison_if_not(state.cursor_eq(Token![open {}]));
    state.cursor_ahead(); // ignore errors

    if compiler::unlikely(!state.okay()) {
        return Err(LangError::UnexpectedToken);
    }

    let space_name = unsafe { extract!(space_name, Token::Ident(ref space) => space.clone()) };
    let mut d = Dict::new();
    rfold_dict(DictFoldState::CB_OR_IDENT, state, &mut d);
    if state.okay() {
        Ok(AlterSpace {
            space_name,
            updated_props: d,
        })
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[cfg(test)]
pub(super) fn alter_space_full<'a>(tok: &'a [Token]) -> LangResult<AlterSpace<'a>> {
    let mut state = State::new(tok, InplaceData::new());
    let a = self::parse_alter_space_from_tokens(&mut state)?;
    assert_full_tt!(state);
    Ok(a)
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
    let mut state = State::new(tok, InplaceData::new());
    self::parse_field_syntax::<InplaceData, ALLOW_RESET>(&mut state)
        .map(|efield| (efield, state.cursor()))
}

#[inline(always)]
/// Parse a field declared using the field syntax
pub(super) fn parse_field_syntax<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    state: &mut State<'a, Qd>,
) -> LangResult<ExpandedField<'a>> {
    let mut mstate = FieldSyntaxParseState::IDENT;
    let mut tmp = MaybeInit::uninit();
    let mut props = Dict::new();
    let mut layers = vec![];
    let mut reset = false;
    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token::Ident(field), FieldSyntaxParseState::IDENT) => {
                tmp = MaybeInit::new(field.clone());
                // expect open brace
                mstate = FieldSyntaxParseState::OB;
            }
            (Token::Symbol(Symbol::TtOpenBrace), FieldSyntaxParseState::OB) => {
                let r = self::rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    state,
                    &mut props,
                );
                if r.has_more() && state.not_exhausted() {
                    // now parse layers
                    self::rfold_layers::<Qd, ALLOW_RESET>(LayerFoldState::TY, state, &mut layers);
                    mstate = FieldSyntaxParseState::FOLD_DICT_INCOMPLETE;
                } else {
                    state.poison();
                    break;
                }
            }
            (Token::Symbol(Symbol::SymComma), FieldSyntaxParseState::FOLD_DICT_INCOMPLETE) => {
                let r = self::rfold_tymeta::<Qd, ALLOW_RESET>(
                    TyMetaFoldState::IDENT_OR_CB,
                    state,
                    &mut props,
                );
                reset = ALLOW_RESET && r.has_reset();
                if state.okay() {
                    mstate = FieldSyntaxParseState::COMPLETED;
                    break;
                }
            }
            (Token::Symbol(Symbol::TtCloseBrace), FieldSyntaxParseState::FOLD_DICT_INCOMPLETE) => {
                // great, were done
                mstate = FieldSyntaxParseState::COMPLETED;
                break;
            }
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }
    state.poison_if_not(mstate == FieldSyntaxParseState::COMPLETED);
    if state.okay() {
        Ok(ExpandedField {
            field_name: unsafe { tmp.assume_init() },
            layers,
            props,
            reset,
        })
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
    state: &mut State<'a, Qd>,
) -> LangResult<Alter<'a>> {
    // alter model mymodel remove x
    if state.remaining() <= 2 || !state.cursor_has_ident_rounded() {
        return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
    }
    let model_name = unsafe { extract!(state.fw_read(), Token::Ident(ref l) => l.clone()) };
    match state.fw_read() {
        Token![add] => alter_add(state)
            .map(AlterKind::Add)
            .map(|kind| Alter::new(model_name, kind)),
        Token![remove] => alter_remove(state)
            .map(AlterKind::Remove)
            .map(|kind| Alter::new(model_name, kind)),
        Token![update] => alter_update(state)
            .map(AlterKind::Update)
            .map(|kind| Alter::new(model_name, kind)),
        _ => return Err(LangError::ExpectedStatement),
    }
}

#[inline(always)]
/// Parse multiple fields declared using the field syntax. Flag setting allows or disallows reset syntax
pub(super) fn parse_multiple_field_syntax<'a, Qd: QueryData<'a>, const ALLOW_RESET: bool>(
    state: &mut State<'a, Qd>,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    const DEFAULT_ADD_COL_CNT: usize = 4;
    /*
        WARNING: No trailing commas allowed

        <add> ::= (<field_syntax> <comma>)*

        Smallest length:
        alter model add myfield { type string }
    */
    if compiler::unlikely(state.remaining() < 5) {
        return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
    }
    match state.read() {
        Token::Ident(_) => {
            let ef = parse_field_syntax::<Qd, ALLOW_RESET>(state)?;
            Ok([ef].into())
        }
        Token::Symbol(Symbol::TtOpenParen) => {
            state.cursor_ahead();
            let mut stop = false;
            let mut cols = Vec::with_capacity(DEFAULT_ADD_COL_CNT);
            while state.loop_tt() && !stop {
                match state.read() {
                    Token::Ident(_) => {
                        let ef = parse_field_syntax::<Qd, ALLOW_RESET>(state)?;
                        cols.push(ef);
                        let nx_comma = state.cursor_rounded_eq(Token![,]);
                        let nx_close = state.cursor_rounded_eq(Token![() close]);
                        stop = nx_close;
                        state.poison_if_not(nx_comma | nx_close);
                        state.cursor_ahead_if(state.okay());
                    }
                    _ => {
                        state.poison();
                        break;
                    }
                }
            }
            state.poison_if_not(stop);
            if state.okay() {
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
    state: &mut State<'a, Qd>,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    self::parse_multiple_field_syntax::<Qd, DISALLOW_RESET_SYNTAX>(state)
}

#[cfg(test)]
pub(super) fn alter_add_full<'a>(
    tok: &'a [Token],
    current: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    let mut state = State::new(tok, InplaceData::new());
    let r = self::alter_add(&mut state);
    *current += state.consumed();
    r
}

#[inline(always)]
/// Parse the expression for `alter model <> remove (..)`
pub(super) fn alter_remove<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<Box<[Slice<'a>]>> {
    const DEFAULT_REMOVE_COL_CNT: usize = 4;
    /*
        WARNING: No trailing commas allowed
        <remove> ::= <ident> | <openparen> (<ident> <comma>)*<closeparen>
    */
    if compiler::unlikely(state.exhausted()) {
        return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
    }

    let r = match state.fw_read() {
        Token::Ident(id) => Box::new([id.clone()]),
        Token::Symbol(Symbol::TtOpenParen) => {
            let mut stop = false;
            let mut cols = Vec::with_capacity(DEFAULT_REMOVE_COL_CNT);
            while state.loop_tt() && !stop {
                match state.fw_read() {
                    Token::Ident(ref ident) => {
                        cols.push(ident.clone());
                        let nx_comma = state.cursor_rounded_eq(Token![,]);
                        let nx_close = state.cursor_rounded_eq(Token![() close]);
                        state.poison_if_not(nx_comma | nx_close);
                        stop = nx_close;
                        state.cursor_ahead_if(state.okay());
                    }
                    _ => {
                        state.cursor_back();
                        state.poison();
                        break;
                    }
                }
            }
            state.poison_if_not(stop);
            if state.okay() {
                cols.into_boxed_slice()
            } else {
                return Err(LangError::UnexpectedToken);
            }
        }
        _ => return Err(LangError::ExpectedStatement),
    };
    Ok(r)
}

#[cfg(test)]
pub(super) fn alter_remove_full<'a>(
    tok: &'a [Token<'a>],
    i: &mut usize,
) -> LangResult<Box<[Slice<'a>]>> {
    let mut state = State::new(tok, InplaceData::new());
    let r = self::alter_remove(&mut state);
    *i += state.consumed();
    r
}

#[inline(always)]
/// Parse the expression for `alter model <> update (..)`
pub(super) fn alter_update<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    self::parse_multiple_field_syntax::<Qd, ALLOW_RESET_SYNTAX>(state)
}

#[cfg(test)]
pub(super) fn alter_update_full<'a>(
    tok: &'a [Token],
    i: &mut usize,
) -> LangResult<Box<[ExpandedField<'a>]>> {
    let mut state = State::new(tok, InplaceData::new());
    let r = self::alter_update(&mut state);
    *i += state.consumed();
    r
}
