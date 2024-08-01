/*
 * Created on Wed Feb 01 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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
    NOTE: The `ASTNode` impls are test-only. most of the time they do stupid things. we should never rely on `ASTNode`
    impls for `syn` elements

    --
    Sayan (@ohsayan)
    Feb. 2, 2023
*/

use crate::{
    engine::{
        data::{
            cell::Datacell,
            dict::{DictEntryGeneric, DictGeneric},
        },
        error::{QueryError, QueryResult},
        mem::unsafe_apis::BoxStr,
        ql::{
            ast::{QueryData, State},
            lex::{Ident, Token},
        },
    },
    util::{compiler, MaybeInit},
};

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

/*
    Context-free dict
*/

states! {
    /// The dict fold state
    pub struct DictFoldState: u8 {
        FINAL = 0xFF,
        pub(super) OB = 0x00,
        pub(super) CB_OR_IDENT = 0x01,
        COLON = 0x02,
        LIT_OR_OB = 0x03,
        COMMA_OR_CB = 0x04,
    }
}

trait Breakpoint<'a> {
    const HAS_BREAKPOINT: bool;
    fn check_breakpoint(state: DictFoldState, tok: &'a Token<'a>) -> bool;
}

struct NoBreakpoint;
impl<'a> Breakpoint<'a> for NoBreakpoint {
    const HAS_BREAKPOINT: bool = false;
    fn check_breakpoint(_: DictFoldState, _: &'a Token<'a>) -> bool {
        false
    }
}
struct TypeBreakpoint;
impl<'a> Breakpoint<'a> for TypeBreakpoint {
    const HAS_BREAKPOINT: bool = true;
    fn check_breakpoint(state: DictFoldState, tok: &'a Token<'a>) -> bool {
        (state == DictFoldState::CB_OR_IDENT) & matches!(tok, Token![type])
    }
}

/// Fold a dictionary
fn _rfold_dict<'a, Qd, Bp>(
    mut mstate: DictFoldState,
    state: &mut State<'a, Qd>,
    dict: &mut DictGeneric,
) -> bool
where
    Qd: QueryData<'a>,
    Bp: Breakpoint<'a>,
{
    /*
        NOTE: Assume rules wherever applicable

        <openbrace> ::= "{"
        <closebrace> ::= "}"
        <comma> ::= ","
        <colon> ::= ":"
        <dict> ::= <openbrace> (<ident> <colon> (<lit> | <dict>) <comma>)* <comma>* <closebrace>
    */
    let mut key = MaybeInit::uninit();
    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token![open {}], DictFoldState::OB) => {
                // open
                mstate = DictFoldState::CB_OR_IDENT;
            }
            (Token![close {}], DictFoldState::CB_OR_IDENT | DictFoldState::COMMA_OR_CB) => {
                // well, that's the end of the dict
                mstate = DictFoldState::FINAL;
                break;
            }
            (Token::Ident(id), DictFoldState::CB_OR_IDENT) => {
                key = MaybeInit::new(*id);
                // found a key, now expect colon
                mstate = DictFoldState::COLON;
            }
            (Token![:], DictFoldState::COLON) => {
                // found colon, now lit or ob
                mstate = DictFoldState::LIT_OR_OB;
            }
            (tok, DictFoldState::LIT_OR_OB) if state.can_read_lit_from(tok) => {
                // found lit
                let v = unsafe {
                    // UNSAFE(@ohsayan): verified at guard
                    state.read_lit_unchecked_from(tok).into()
                };
                state.poison_if_not(
                    dict.insert(
                        BoxStr::new(unsafe {
                            // UNSAFE(@ohsayan): we switch to this state only when we are in the LIT_OR_OB state. this means that we've already read in a key
                            key.take().as_str()
                        }),
                        v,
                    )
                    .is_none(),
                );
                // after lit we're either done or expect something else
                mstate = DictFoldState::COMMA_OR_CB;
            }
            (Token![null], DictFoldState::LIT_OR_OB) => {
                // found a null
                state.poison_if_not(
                    dict.insert(
                        BoxStr::new(unsafe {
                            // UNSAFE(@ohsayan): we only switch to this when we've already read in a key
                            key.take().as_str()
                        }),
                        DictEntryGeneric::Data(Datacell::null()),
                    )
                    .is_none(),
                );
                // after a null (essentially counts as a lit) we're either done or expect something else
                mstate = DictFoldState::COMMA_OR_CB;
            }
            (Token![open {}], DictFoldState::LIT_OR_OB) => {
                // found a nested dict
                let mut ndict = DictGeneric::new();
                _rfold_dict::<Qd, NoBreakpoint>(DictFoldState::CB_OR_IDENT, state, &mut ndict);
                state.poison_if_not(
                    dict.insert(
                        BoxStr::new(unsafe {
                            // UNSAFE(@ohsayan): correct again because whenever we hit an expression position, we've already read in a key (ident)
                            key.take().as_str()
                        }),
                        DictEntryGeneric::Map(ndict),
                    )
                    .is_none(),
                );
                mstate = DictFoldState::COMMA_OR_CB;
            }
            (Token![,], DictFoldState::COMMA_OR_CB) => {
                // expecting a comma, found it. now expect a close brace or an ident
                mstate = DictFoldState::CB_OR_IDENT;
            }
            (this_tok, this_key)
                if Bp::HAS_BREAKPOINT && Bp::check_breakpoint(this_key, this_tok) =>
            {
                // reached custom breakpoint
                return true;
            }
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }
    state.poison_if_not(mstate == DictFoldState::FINAL);
    false
}

pub(super) fn rfold_dict<'a, Qd: QueryData<'a>>(
    mstate: DictFoldState,
    state: &mut State<'a, Qd>,
    dict: &mut DictGeneric,
) {
    _rfold_dict::<Qd, NoBreakpoint>(mstate, state, dict);
}

pub fn parse_dict<'a, Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> Option<DictGeneric> {
    let mut d = DictGeneric::new();
    rfold_dict(DictFoldState::OB, state, &mut d);
    if state.okay() {
        Some(d)
    } else {
        None
    }
}

pub(super) fn rfold_tymeta<'a, Qd: QueryData<'a>>(
    mstate: DictFoldState,
    state: &mut State<'a, Qd>,
    dict: &mut DictGeneric,
) -> bool {
    _rfold_dict::<Qd, TypeBreakpoint>(mstate, state, dict)
}

#[derive(Debug, PartialEq)]
/// A layer contains a type and corresponding metadata
pub struct LayerSpec<'a> {
    pub(in crate::engine) ty: Ident<'a>,
    pub(in crate::engine) props: DictGeneric,
}

impl<'a> LayerSpec<'a> {
    //// Create a new layer
    #[cfg(test)]
    pub const fn new(ty: Ident<'a>, props: DictGeneric) -> Self {
        Self { ty, props }
    }
}

states! {
    /// Layer fold state
    pub struct LayerFoldState: u8 {
        BEGIN_IDENT = 0x01,
        FOLD_INCOMPLETE = 0x03,
        FINAL_OR_OB = 0x04,
        FINAL = 0xFF,
    }
}

fn rfold_layers<'a, Qd: QueryData<'a>>(state: &mut State<'a, Qd>, layers: &mut Vec<LayerSpec<'a>>) {
    let mut mstate = LayerFoldState::BEGIN_IDENT;
    let mut ty = MaybeInit::uninit();
    let mut props = Default::default();
    while state.loop_tt() {
        match (state.fw_read(), mstate) {
            (Token::Ident(id), LayerFoldState::BEGIN_IDENT) => {
                ty = MaybeInit::new(*id);
                mstate = LayerFoldState::FINAL_OR_OB;
            }
            (Token![open {}], LayerFoldState::FINAL_OR_OB) => {
                // we were done ... but we found some props
                if rfold_tymeta(DictFoldState::CB_OR_IDENT, state, &mut props) {
                    // we have more layers
                    // but we first need a colon
                    state.poison_if_not(state.cursor_rounded_eq(Token![:]));
                    state.cursor_ahead_if(state.okay());
                    rfold_layers(state, layers);
                    // we are yet to parse the remaining props
                    mstate = LayerFoldState::FOLD_INCOMPLETE;
                } else {
                    // didn't hit bp; so we should be done here
                    mstate = LayerFoldState::FINAL;
                    break;
                }
            }
            (Token![close {}], LayerFoldState::FOLD_INCOMPLETE) => {
                // found end of the dict. roger the terminal!
                mstate = LayerFoldState::FINAL;
                break;
            }
            (Token![,], LayerFoldState::FOLD_INCOMPLETE) => {
                // we found a comma, but we should finish parsing the dict
                rfold_dict(DictFoldState::CB_OR_IDENT, state, &mut props);
                // we're done parsing
                mstate = LayerFoldState::FINAL;
                break;
            }
            // FIXME(@ohsayan): if something falls apart, it's the arm below
            (_, LayerFoldState::FINAL_OR_OB) => {
                state.cursor_back();
                mstate = LayerFoldState::FINAL;
                break;
            }
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        }
    }
    if ((mstate == LayerFoldState::FINAL) | (mstate == LayerFoldState::FINAL_OR_OB)) & state.okay()
    {
        layers.push(LayerSpec {
            ty: unsafe {
                // UNSAFE(@ohsayan): our start state always looks for an ident
                ty.take()
            },
            props,
        });
    } else {
        state.poison();
    }
}

#[derive(Debug, PartialEq)]
/// A field definition
pub struct FieldSpec<'a> {
    /// the field name
    pub(in crate::engine) field_name: Ident<'a>,
    /// layers
    pub(in crate::engine) layers: Vec<LayerSpec<'a>>,
    /// is null
    pub(in crate::engine) null: bool,
    /// is primary
    pub(in crate::engine) primary: bool,
}

impl<'a> FieldSpec<'a> {
    #[cfg(test)]
    pub fn new(
        field_name: Ident<'a>,
        layers: Vec<LayerSpec<'a>>,
        null: bool,
        primary: bool,
    ) -> Self {
        Self {
            field_name,
            layers,
            null,
            primary,
        }
    }
    pub fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        if compiler::unlikely(state.remaining() < 2) {
            // smallest field: `ident: type`
            return Err(QueryError::QLUnexpectedEndOfStatement);
        }
        // check if primary or null
        let is_primary = state.cursor_eq(Token![primary]);
        state.cursor_ahead_if(is_primary);
        let is_null = state.cursor_eq(Token![null]);
        state.cursor_ahead_if(is_null);
        state.poison_if(is_primary & is_null);
        // parse layers
        // field name
        let field_name = match (state.fw_read(), state.fw_read()) {
            (Token::Ident(id), Token![:]) => id,
            _ => return Err(QueryError::QLInvalidSyntax),
        };
        // layers
        let mut layers = Vec::new();
        rfold_layers(state, &mut layers);
        if state.okay() {
            Ok(FieldSpec {
                field_name: *field_name,
                layers,
                null: is_null,
                primary: is_primary,
            })
        } else {
            Err(QueryError::QLInvalidTypeDefinitionSyntax)
        }
    }
}

#[derive(Debug, PartialEq)]
/// An [`ExpandedField`] is a full field definition with advanced metadata
pub struct ExpandedField<'a> {
    pub(in crate::engine) field_name: Ident<'a>,
    pub(in crate::engine) layers: Vec<LayerSpec<'a>>,
    pub(in crate::engine) props: DictGeneric,
}

impl<'a> ExpandedField<'a> {
    #[cfg(test)]
    pub fn new(field_name: Ident<'a>, layers: Vec<LayerSpec<'a>>, props: DictGeneric) -> Self {
        Self {
            field_name,
            layers,
            props,
        }
    }
    #[inline(always)]
    /// Parse a field declared using the field syntax
    pub(super) fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        if compiler::unlikely(state.remaining() < 6) {
            // smallest: fieldname { type: ident }
            return Err(QueryError::QLUnexpectedEndOfStatement);
        }
        let field_name = state.fw_read();
        state.poison_if_not(field_name.is_ident());
        state.poison_if_not(state.cursor_eq(Token![open {}]));
        state.cursor_ahead();
        // ignore errors; now attempt a tymeta-like parse
        let mut props = DictGeneric::new();
        let mut layers = Vec::new();
        if rfold_tymeta(DictFoldState::CB_OR_IDENT, state, &mut props) {
            // this has layers. fold them; but don't forget the colon
            if compiler::unlikely(state.exhausted()) {
                // we need more tokens
                return Err(QueryError::QLUnexpectedEndOfStatement);
            }
            state.poison_if_not(state.cursor_eq(Token![:]));
            state.cursor_ahead();
            rfold_layers(state, &mut layers);
            match state.fw_read() {
                Token![,] => {
                    rfold_dict(DictFoldState::CB_OR_IDENT, state, &mut props);
                }
                Token![close {}] => {
                    // hit end
                }
                _ => {
                    state.poison();
                }
            }
        }
        if state.okay() {
            Ok(Self {
                field_name: unsafe {
                    // UNSAFE(@ohsayan): We just verified if `field_name` returns `is_ident`
                    field_name.uck_read_ident()
                },
                props,
                layers,
            })
        } else {
            Err(QueryError::QLInvalidSyntax)
        }
    }
    #[inline(always)]
    /// Parse multiple fields declared using the field syntax. Flag setting allows or disallows reset syntax
    pub fn parse_multiple<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Box<[Self]>> {
        const DEFAULT_ADD_COL_CNT: usize = 4;
        /*
            WARNING: No trailing commas allowed

            <add> ::= (<field_syntax> <comma>)*

            Smallest length:
            alter model add myfield { type string }
        */
        if compiler::unlikely(state.remaining() < 5) {
            return compiler::cold_rerr(QueryError::QLUnexpectedEndOfStatement);
        }
        match state.read() {
            Token::Ident(_) => {
                let ef = Self::parse(state)?;
                Ok([ef].into())
            }
            Token![() open] => {
                state.cursor_ahead();
                let mut stop = false;
                let mut cols = Vec::with_capacity(DEFAULT_ADD_COL_CNT);
                while state.loop_tt() && !stop {
                    match state.read() {
                        Token::Ident(_) => {
                            let ef = Self::parse(state)?;
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
                    Err(QueryError::QLInvalidSyntax)
                }
            }
            _ => Err(QueryError::QLExpectedStatement),
        }
    }
}

#[cfg(test)]
pub use impls::{DictBasic, DictTypeMeta, DictTypeMetaSplit};
#[cfg(test)]
mod impls {
    use {
        super::{
            rfold_dict, rfold_layers, rfold_tymeta, DictFoldState, DictGeneric, ExpandedField,
            FieldSpec, LayerSpec,
        },
        crate::engine::{
            error::QueryResult,
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for ExpandedField<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            Self::parse(state)
        }
        fn _multiple_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Vec<Self>> {
            Self::parse_multiple(state).map(Vec::from)
        }
    }
    impl<'a> ASTNode<'a> for LayerSpec<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        // important: upstream must verify this
        const VERIFY_STATE_BEFORE_RETURN: bool = true;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            let mut layers = Vec::new();
            rfold_layers(state, &mut layers);
            assert!(layers.len() == 1);
            Ok(layers.swap_remove(0))
        }
        fn _multiple_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Vec<Self>> {
            let mut l = Vec::new();
            rfold_layers(state, &mut l);
            Ok(l)
        }
    }
    #[derive(sky_macros::Wrapper, Debug)]
    pub struct DictBasic(DictGeneric);
    impl<'a> ASTNode<'a> for DictBasic {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        // important: upstream must verify this
        const VERIFY_STATE_BEFORE_RETURN: bool = true;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            let mut dict = DictGeneric::new();
            rfold_dict(DictFoldState::OB, state, &mut dict);
            Ok(Self(dict))
        }
    }
    #[derive(sky_macros::Wrapper, Debug)]
    pub struct DictTypeMetaSplit(DictGeneric);
    impl<'a> ASTNode<'a> for DictTypeMetaSplit {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        // important: upstream must verify this
        const VERIFY_STATE_BEFORE_RETURN: bool = true;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            let mut dict = DictGeneric::new();
            rfold_tymeta(DictFoldState::CB_OR_IDENT, state, &mut dict);
            Ok(Self(dict))
        }
    }
    #[derive(sky_macros::Wrapper, Debug)]
    pub struct DictTypeMeta(DictGeneric);
    impl<'a> ASTNode<'a> for DictTypeMeta {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        // important: upstream must verify this
        const VERIFY_STATE_BEFORE_RETURN: bool = true;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            let mut dict = DictGeneric::new();
            rfold_tymeta(DictFoldState::OB, state, &mut dict);
            Ok(Self(dict))
        }
    }
    impl<'a> ASTNode<'a> for FieldSpec<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            Self::parse(state)
        }
    }
}
