/*
 * Created on Wed Nov 16 2022
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

#[macro_use]
pub(in crate::engine) mod syn;
pub(in crate::engine) mod alt;
pub(in crate::engine) mod crt;
pub(in crate::engine) mod drop;

use {
    super::{
        ast::{traits::ASTNode, QueryData, State},
        lex::{Ident, Token},
    },
    crate::engine::{
        core::EntityIDRef,
        error::{QueryError, QueryResult},
    },
};

#[derive(Debug, PartialEq)]
pub enum Use<'a> {
    Space(Ident<'a>),
    RefreshCurrent,
    Null,
}

impl<'a> ASTNode<'a> for Use<'a> {
    const MUST_USE_FULL_TOKEN_RANGE: bool = true;
    const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
    fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self> {
        /*
            should have either an ident or null
        */
        if state.exhausted() | (state.remaining() > 2) {
            return Err(QueryError::QLInvalidSyntax);
        }
        Ok(match state.fw_read() {
            Token::Ident(new_space) => Self::Space(*new_space),
            Token![null] => Self::Null,
            Token![$] => {
                if state.exhausted() {
                    return Err(QueryError::QLInvalidSyntax);
                }
                match state.fw_read() {
                    Token::Ident(id) if id.eq_ignore_ascii_case("current") => Self::RefreshCurrent,
                    _ => return Err(QueryError::QLInvalidSyntax),
                }
            }
            _ => return Err(QueryError::QLInvalidSyntax),
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum Inspect<'a> {
    Global,
    Space(Ident<'a>),
    Model(EntityIDRef<'a>),
}

impl<'a> ASTNode<'a> for Inspect<'a> {
    const MUST_USE_FULL_TOKEN_RANGE: bool = true;
    const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
    fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self> {
        if state.exhausted() {
            return Err(QueryError::QLUnexpectedEndOfStatement);
        }
        let me = match state.fw_read() {
            Token::Ident(id) if id.eq_ignore_ascii_case("global") => Self::Global,
            Token![space] => {
                if state.exhausted() {
                    return Err(QueryError::QLUnexpectedEndOfStatement);
                }
                match state.fw_read() {
                    Token::Ident(space) => Self::Space(*space),
                    _ => return Err(QueryError::QLInvalidSyntax),
                }
            }
            Token![model] => {
                let entity = state.try_entity_ref_result()?;
                Self::Model(entity)
            }
            _ => return Err(QueryError::QLInvalidSyntax),
        };
        Ok(me)
    }
}
