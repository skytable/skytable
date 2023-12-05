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
    data::DictGeneric,
    error::{QueryError, QueryResult},
    ql::{
        ast::{traits, QueryData, State},
        ddl::syn,
        lex::Ident,
    },
};

#[derive(Debug, PartialEq)]
pub enum SysctlCommand<'a> {
    /// `sysctl create user ...`
    CreateUser(UserDecl<'a>),
    /// `sysctl drop user ...`
    DropUser(UserDel<'a>),
    /// `systcl alter user ...`
    AlterUser(UserDecl<'a>),
    /// `sysctl status`
    ReportStatus,
}

impl<'a> SysctlCommand<'a> {
    pub fn needs_root(&self) -> bool {
        !matches!(self, Self::ReportStatus)
    }
}

impl<'a> traits::ASTNode<'a> for SysctlCommand<'a> {
    const MUST_USE_FULL_TOKEN_RANGE: bool = true;
    const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
    fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self> {
        if state.remaining() < 2 {
            return Err(QueryError::QLUnexpectedEndOfStatement);
        }
        let (a, b) = (state.fw_read(), state.fw_read());
        let alter = Token![alter].eq(a) & b.ident_eq("user");
        let create = Token![create].eq(a) & b.ident_eq("user");
        let drop = Token![drop].eq(a) & b.ident_eq("user");
        let status = a.ident_eq("report") & b.ident_eq("status");
        if !(create | drop | status | alter) {
            return Err(QueryError::QLUnknownStatement);
        }
        if create {
            UserDecl::parse(state).map(SysctlCommand::CreateUser)
        } else if drop {
            UserDel::parse(state).map(SysctlCommand::DropUser)
        } else if alter {
            UserDecl::parse(state).map(SysctlCommand::AlterUser)
        } else {
            Ok(SysctlCommand::ReportStatus)
        }
    }
}

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
    let signature_okay = token_buffer[0].is_ident()
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
        token_buffer[0].uck_read_ident()
    };
    if state.not_exhausted() | !state.okay() {
        // we shouldn't have more tokens
        return Err(QueryError::QLInvalidSyntax);
    }
    Ok(UserMeta {
        username: maybe_username,
        options: dict,
    })
}

struct UserMeta<'a> {
    username: Ident<'a>,
    options: DictGeneric,
}

#[derive(Debug, PartialEq)]
pub struct UserDecl<'a> {
    username: Ident<'a>,
    options: DictGeneric,
}

impl<'a> UserDecl<'a> {
    pub(in crate::engine::ql) fn new(username: Ident<'a>, options: DictGeneric) -> Self {
        Self { username, options }
    }
    pub fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        parse(state).map(|UserMeta { username, options }: UserMeta| Self::new(username, options))
    }
    pub fn username(&self) -> &str {
        self.username.as_str()
    }
    pub fn options_mut(&mut self) -> &mut DictGeneric {
        &mut self.options
    }
    pub fn options(&self) -> &DictGeneric {
        &self.options
    }
}

#[derive(Debug, PartialEq)]
pub struct UserDel<'a> {
    username: Ident<'a>,
}

impl<'a> UserDel<'a> {
    pub(in crate::engine::ql) fn new(username: Ident<'a>) -> Self {
        Self { username }
    }
    /// Parse a `user del` DCL command
    ///
    /// MUSTENDSTREAM: YES
    pub fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        if state.cursor_has_ident_rounded() & (state.remaining() == 1) {
            let username = unsafe {
                // UNSAFE(@ohsayan): +boundck
                state.read().uck_read_ident()
            };
            state.cursor_ahead();
            return Ok(Self::new(username));
        }
        Err(QueryError::QLInvalidSyntax)
    }
    pub fn username(&self) -> &str {
        self.username.as_str()
    }
}
