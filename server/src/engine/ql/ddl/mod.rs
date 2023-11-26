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
        ast::traits::ASTNode,
        lex::{Ident, Token},
    },
    crate::engine::error::QueryError,
};

#[derive(Debug, PartialEq)]
pub enum Use<'a> {
    Space(Ident<'a>),
    Null,
}

impl<'a> ASTNode<'a> for Use<'a> {
    const MUST_USE_FULL_TOKEN_RANGE: bool = true;
    const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = true;
    fn __base_impl_parse_from_state<Qd: super::ast::QueryData<'a>>(
        state: &mut super::ast::State<'a, Qd>,
    ) -> crate::engine::error::QueryResult<Self> {
        /*
            should have either an ident or null
        */
        if state.remaining() != 1 {
            return Err(QueryError::QLInvalidSyntax);
        }
        Ok(match state.fw_read() {
            Token![null] => Self::Null,
            Token::Ident(id) => Self::Space(id.clone()),
            _ => return Err(QueryError::QLInvalidSyntax),
        })
    }
}
