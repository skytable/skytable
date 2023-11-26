/*
 * Created on Thu Feb 02 2023
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

use {
    super::syn::{self, DictFoldState, ExpandedField},
    crate::{
        engine::{
            core::EntityIDRef,
            data::DictGeneric,
            error::{QueryError, QueryResult},
            ql::{
                ast::{QueryData, State},
                lex::{Ident, Token},
            },
        },
        util::compiler,
    },
};

#[derive(Debug, PartialEq)]
/// An alter space query with corresponding data
pub struct AlterSpace<'a> {
    pub space_name: Ident<'a>,
    pub updated_props: DictGeneric,
}

impl<'a> AlterSpace<'a> {
    #[cfg(test)]
    pub fn new(space_name: Ident<'a>, updated_props: DictGeneric) -> Self {
        Self {
            space_name,
            updated_props,
        }
    }
    #[inline(always)]
    /// Parse alter space from tokens
    fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        if compiler::unlikely(state.remaining() <= 3) {
            return compiler::cold_rerr(QueryError::QLUnexpectedEndOfStatement);
        }
        let space_name = state.fw_read();
        state.poison_if_not(space_name.is_ident());
        state.poison_if_not(state.cursor_eq(Token![with]));
        state.cursor_ahead(); // ignore errors
        state.poison_if_not(state.cursor_eq(Token![open {}]));
        state.cursor_ahead(); // ignore errors

        if compiler::unlikely(!state.okay()) {
            return Err(QueryError::QLInvalidSyntax);
        }

        let space_name = unsafe {
            // UNSAFE(@ohsayan): We just verified that `space_name` is an ident
            space_name.uck_read_ident()
        };
        let mut d = DictGeneric::new();
        syn::rfold_dict(DictFoldState::CB_OR_IDENT, state, &mut d);
        if state.okay() {
            Ok(AlterSpace {
                space_name,
                updated_props: d,
            })
        } else {
            Err(QueryError::QLInvalidCollectionSyntax)
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AlterModel<'a> {
    pub(in crate::engine) model: EntityIDRef<'a>,
    pub(in crate::engine) kind: AlterKind<'a>,
}

impl<'a> AlterModel<'a> {
    #[inline(always)]
    pub fn new(model: EntityIDRef<'a>, kind: AlterKind<'a>) -> Self {
        Self { model, kind }
    }
}

#[derive(Debug, PartialEq)]
/// The alter operation kind
pub enum AlterKind<'a> {
    Add(Box<[ExpandedField<'a>]>),
    Remove(Box<[Ident<'a>]>),
    Update(Box<[ExpandedField<'a>]>),
}

impl<'a> AlterModel<'a> {
    #[inline(always)]
    /// Parse an [`AlterKind`] from the given token stream
    fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        // alter model mymodel remove x
        if state.remaining() <= 2 || !state.cursor_has_ident_rounded() {
            return compiler::cold_rerr(QueryError::QLInvalidSyntax);
            // FIXME(@ohsayan): bad because no specificity
        }
        let model_name = state.try_entity_ref_result()?;
        let kind = match state.fw_read() {
            Token![add] => AlterKind::alter_add(state),
            Token![remove] => AlterKind::alter_remove(state),
            Token![update] => AlterKind::alter_update(state),
            _ => Err(QueryError::QLExpectedStatement),
        };
        kind.map(|kind| AlterModel::new(model_name, kind))
    }
}

impl<'a> AlterKind<'a> {
    #[inline(always)]
    /// Parse the expression for `alter model <> add (..)`
    fn alter_add<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        ExpandedField::parse_multiple(state).map(Self::Add)
    }
    #[inline(always)]
    /// Parse the expression for `alter model <> add (..)`
    fn alter_update<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        ExpandedField::parse_multiple(state).map(Self::Update)
    }
    #[inline(always)]
    /// Parse the expression for `alter model <> remove (..)`
    fn alter_remove<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        const DEFAULT_REMOVE_COL_CNT: usize = 4;
        /*
            WARNING: No trailing commas allowed
            <remove> ::= <ident> | <openparen> (<ident> <comma>)*<closeparen>
        */
        if compiler::unlikely(state.exhausted()) {
            return compiler::cold_rerr(QueryError::QLUnexpectedEndOfStatement);
        }

        let r = match state.fw_read() {
            Token::Ident(id) => Box::new([*id]),
            Token![() open] => {
                let mut stop = false;
                let mut cols = Vec::with_capacity(DEFAULT_REMOVE_COL_CNT);
                while state.loop_tt() && !stop {
                    match state.fw_read() {
                        Token::Ident(ref ident) => {
                            cols.push(*ident);
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
                    return Err(QueryError::QLInvalidSyntax);
                }
            }
            _ => return Err(QueryError::QLInvalidSyntax),
        };
        Ok(Self::Remove(r))
    }
}

mod impls {
    use {
        super::{AlterModel, AlterSpace},
        crate::engine::{
            error::QueryResult,
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for AlterModel<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = true;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            Self::parse(state)
        }
    }
    impl<'a> ASTNode<'a> for AlterSpace<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = true;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            Self::parse(state)
        }
    }
}
