/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use crate::engine::{
    control_flow::{errors::QueryError, QueryResult},
    core::EntityIDRef,
    ql::ast::{traits::ASTNode, QueryData, State},
};

#[derive(Debug, PartialEq)]
pub enum TruncateStmt<'a> {
    Model(EntityIDRef<'a>),
}

impl<'a> TruncateStmt<'a> {
    fn decode<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        match state.try_next() {
            Some(Token![model]) => state.try_entity_ref_result().map(Self::Model),
            Some(_) => Err(QueryError::QLUnknownStatement),
            None => Err(QueryError::QLUnexpectedEndOfStatement),
        }
    }
}

impl<'a> ASTNode<'a> for TruncateStmt<'a> {
    const MUST_USE_FULL_TOKEN_RANGE: bool = true;
    const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
    fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self> {
        Self::decode(state)
    }
}
