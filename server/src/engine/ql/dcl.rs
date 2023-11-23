/*
 * Created on Thu Sep 21 2023
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

use crate::engine::{
    data::{
        tag::{DataTag, TagClass},
        DictGeneric,
    },
    error::{QueryError, QueryResult},
    ql::{
        ast::{traits, QueryData, State},
        ddl::syn,
    },
};

fn parse<'a, Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<UserMeta<'a>> {
    /*
        [username] with { password: [password], ... }
        ^cursor
        7 tokens
    */
    if state.remaining() < 7 {
        return Err(QueryError::QLInvalidSyntax);
    }
    let token_buffer = state.current();
    // initial sig
    let signature_okay = token_buffer[0].is_lit()
        & token_buffer[1].eq(&Token![with])
        & token_buffer[2].eq(&Token![open {}]);
    // get props
    state.poison_if_not(signature_okay);
    state.cursor_ahead_by(2);
    let Some(dict) = syn::parse_dict(state) else {
        return Err(QueryError::QLInvalidCollectionSyntax);
    };
    let maybe_username = unsafe {
        // UNSAFE(@ohsayan): the dict parse ensures state correctness
        token_buffer[0].uck_read_lit()
    };
    state.poison_if_not(maybe_username.kind().tag_class() == TagClass::Str);
    if state.not_exhausted() | !state.okay() {
        // we shouldn't have more tokens
        return Err(QueryError::QLInvalidSyntax);
    }
    Ok(UserMeta {
        username: unsafe {
            // UNSAFE(@ohsayan): +tagck in state
            maybe_username.str()
        },
        options: dict,
    })
}

struct UserMeta<'a> {
    username: &'a str,
    options: DictGeneric,
}

#[derive(Debug, PartialEq)]
pub struct UserAdd<'a> {
    username: &'a str,
    options: DictGeneric,
}

impl<'a> UserAdd<'a> {
    pub(in crate::engine::ql) fn new(username: &'a str, options: DictGeneric) -> Self {
        Self { username, options }
    }
    /// Parse a `user add` DCL command
    ///
    /// MUSTENDSTREAM: YES
    pub fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        parse(state).map(|UserMeta { username, options }: UserMeta| Self::new(username, options))
    }
    pub fn username(&self) -> &str {
        self.username
    }
    pub fn options_mut(&mut self) -> &mut DictGeneric {
        &mut self.options
    }
    pub fn options(&self) -> &DictGeneric {
        &self.options
    }
}

impl<'a> traits::ASTNode<'a> for UserAdd<'a> {
    fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        Self::parse(state)
    }
}

#[derive(Debug, PartialEq)]
pub struct UserDel<'a> {
    username: &'a str,
}

impl<'a> UserDel<'a> {
    pub(in crate::engine::ql) fn new(username: &'a str) -> Self {
        Self { username }
    }
    /// Parse a `user del` DCL command
    ///
    /// MUSTENDSTREAM: YES
    pub fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        if state.can_read_lit_rounded() & (state.remaining() == 1) {
            let lit = unsafe {
                // UNSAFE(@ohsayan): +boundck
                state.read_cursor_lit_unchecked()
            };
            state.cursor_ahead();
            if lit.kind().tag_class() == TagClass::Str {
                return Ok(Self::new(unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    lit.str()
                }));
            }
        }
        Err(QueryError::QLInvalidSyntax)
    }
    pub fn username(&self) -> &str {
        self.username
    }
}

impl<'a> traits::ASTNode<'a> for UserDel<'a> {
    fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        Self::parse(state)
    }
}
