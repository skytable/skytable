/*
 * Created on Fri Jan 06 2023
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
    super::{WhereClause, WhereClauseCollection},
    crate::{
        engine::{
            error::{LangError, LangResult},
            ql::{
                ast::{Entity, QueryData, State},
                lex::{Ident, Token},
            },
        },
        util::{compiler, MaybeInit},
    },
};

/*
    Impls for select
*/

#[derive(Debug, PartialEq)]
pub struct SelectStatement<'a> {
    /// the entity
    pub(super) entity: Entity<'a>,
    /// fields in order of querying. will be zero when wildcard is set
    pub(super) fields: Vec<Ident<'a>>,
    /// whether a wildcard was passed
    pub(super) wildcard: bool,
    /// where clause
    pub(super) clause: WhereClause<'a>,
}

impl<'a> SelectStatement<'a> {
    #[inline(always)]
    pub(crate) fn new_test(
        entity: Entity<'a>,
        fields: Vec<Ident<'a>>,
        wildcard: bool,
        clauses: WhereClauseCollection<'a>,
    ) -> SelectStatement<'a> {
        Self::new(entity, fields, wildcard, clauses)
    }
    #[inline(always)]
    fn new(
        entity: Entity<'a>,
        fields: Vec<Ident<'a>>,
        wildcard: bool,
        clauses: WhereClauseCollection<'a>,
    ) -> SelectStatement<'a> {
        Self {
            entity,
            fields,
            wildcard,
            clause: WhereClause::new(clauses),
        }
    }
    pub fn entity(&self) -> Entity<'a> {
        self.entity
    }
    pub fn clauses(&self) -> &WhereClause<'a> {
        &self.clause
    }
    pub fn clauses_mut(&mut self) -> &mut WhereClause<'a> {
        &mut self.clause
    }
    pub fn is_wildcard(&self) -> bool {
        self.wildcard
    }
    pub fn into_fields(self) -> Vec<Ident<'a>> {
        self.fields
    }
}

impl<'a> SelectStatement<'a> {
    pub fn parse_select<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
        /*
            Smallest query:
            select * from model
                   ^ ^    ^
                   1 2    3
        */
        if compiler::unlikely(state.remaining() < 3) {
            return compiler::cold_rerr(LangError::UnexpectedEOS);
        }
        let mut select_fields = Vec::new();
        let is_wildcard = state.cursor_eq(Token![*]);
        state.cursor_ahead_if(is_wildcard);
        while state.not_exhausted() && state.okay() && !is_wildcard {
            match state.read() {
                Token::Ident(id) => select_fields.push(*id),
                _ => break,
            }
            state.cursor_ahead();
            let nx_comma = state.cursor_rounded_eq(Token![,]);
            let nx_from = state.cursor_rounded_eq(Token![from]);
            state.poison_if_not(nx_comma | nx_from);
            state.cursor_ahead_if(nx_comma);
        }
        state.poison_if_not(is_wildcard | !select_fields.is_empty());
        // we should have from + model
        if compiler::unlikely(state.remaining() < 2 || !state.okay()) {
            return compiler::cold_rerr(LangError::BadSyntax);
        }
        state.poison_if_not(state.cursor_eq(Token![from]));
        state.cursor_ahead(); // ignore errors
        let mut entity = MaybeInit::uninit();
        Entity::parse_from_state_rounded(state, &mut entity);
        let mut clauses = <_ as Default>::default();
        if state.cursor_rounded_eq(Token![where]) {
            state.cursor_ahead();
            WhereClause::parse_where_and_append_to(state, &mut clauses);
            state.poison_if(clauses.is_empty());
        }
        if compiler::likely(state.okay()) {
            Ok(SelectStatement {
                entity: unsafe {
                    // UNSAFE(@ohsayan): `process_entity` and `okay` assert correctness
                    entity.assume_init()
                },
                fields: select_fields,
                wildcard: is_wildcard,
                clause: WhereClause::new(clauses),
            })
        } else {
            compiler::cold_rerr(LangError::BadSyntax)
        }
    }
}

mod impls {
    use {
        super::SelectStatement,
        crate::engine::{
            error::LangResult,
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for SelectStatement<'a> {
        fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
            Self::parse_select(state)
        }
    }
}
