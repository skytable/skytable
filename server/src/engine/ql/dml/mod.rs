/*
 * Created on Fri Oct 14 2022
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
    TODO(@ohsayan): For now we've settled for an imprecise error site reporting for simplicity, which we
    should augment in future revisions of the QL engine
*/

pub mod del;
pub mod ins;
pub mod sel;
pub mod upd;

use {
    super::{
        ast::{QueryData, State},
        lex::Ident,
    },
    crate::{engine::data::lit::Lit, util::compiler},
    std::collections::HashMap,
};

#[inline(always)]
fn u(b: bool) -> u8 {
    b as _
}

/*
    Misc
*/

/*
    Contexts
*/

#[derive(Debug, PartialEq)]
pub struct RelationalExpr<'a> {
    pub(super) lhs: Ident<'a>,
    pub(super) rhs: Lit<'a>,
    pub(super) opc: u8,
}

impl<'a> RelationalExpr<'a> {
    #[inline(always)]
    pub(super) fn new(lhs: Ident<'a>, rhs: Lit<'a>, opc: u8) -> RelationalExpr<'a> {
        Self { lhs, rhs, opc }
    }
    pub(super) const OP_EQ: u8 = 1;
    pub(super) const OP_NE: u8 = 2;
    pub(super) const OP_GT: u8 = 3;
    pub(super) const OP_GE: u8 = 4;
    pub(super) const OP_LT: u8 = 5;
    pub(super) const OP_LE: u8 = 6;
    pub fn filter_hint_none(&self) -> bool {
        self.opc == Self::OP_EQ
    }
    pub fn rhs(&self) -> Lit<'a> {
        self.rhs.clone()
    }
    #[inline(always)]
    fn parse_operator<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> u8 {
        let tok = state.current();
        let op_eq = u(tok[0] == Token![=]) * Self::OP_EQ;
        let op_ne = u(tok[0] == Token![!] && tok[1] == Token![=]) * Self::OP_NE;
        let op_ge = u(tok[0] == Token![>] && tok[1] == Token![=]) * Self::OP_GE;
        let op_gt = u(tok[0] == Token![>] && op_ge == 0) * Self::OP_GT;
        let op_le = u(tok[0] == Token![<] && tok[1] == Token![=]) * Self::OP_LE;
        let op_lt = u(tok[0] == Token![<] && op_le == 0) * Self::OP_LT;
        let opc = op_eq + op_ne + op_ge + op_gt + op_le + op_lt;
        state.poison_if_not(opc != 0);
        state.cursor_ahead_by(1 + (opc & 1 == 0) as usize);
        opc
    }
    #[inline(always)]
    fn try_parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> Option<Self> {
        if compiler::likely(state.remaining() < 3) {
            return compiler::cold_val(None);
        }
        let ident = state.read();
        state.poison_if_not(ident.is_ident());
        state.cursor_ahead(); // ignore any errors
        let operator = Self::parse_operator(state);
        state.poison_if_not(state.can_read_lit_rounded());
        if compiler::likely(state.okay()) {
            unsafe {
                // UNSAFE(@ohsayan): we verified this above
                let lit = state.read_cursor_lit_unchecked();
                state.cursor_ahead();
                // UNSAFE(@ohsayan): we checked if `ident` returns `is_ident` and updated state
                Some(Self::new(ident.uck_read_ident(), lit, operator))
            }
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct WhereClause<'a> {
    c: WhereClauseCollection<'a>,
}

type WhereClauseCollection<'a> = HashMap<Ident<'a>, RelationalExpr<'a>>;

impl<'a> WhereClause<'a> {
    #[inline(always)]
    pub(super) fn new(c: WhereClauseCollection<'a>) -> Self {
        Self { c }
    }
    pub fn clauses_mut(&mut self) -> &mut WhereClauseCollection<'a> {
        &mut self.c
    }
    #[inline(always)]
    fn parse_where_and_append_to<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
        c: &mut WhereClauseCollection<'a>,
    ) {
        let mut has_more = true;
        while has_more && state.not_exhausted() && state.okay() {
            if let Some(expr) = RelationalExpr::try_parse(state) {
                state.poison_if_not(c.insert(expr.lhs, expr).is_none());
            }
            has_more = state.cursor_rounded_eq(Token![and]);
            state.cursor_ahead_if(has_more);
        }
    }
    #[inline(always)]
    /// Parse a where context
    ///
    /// Notes:
    /// - Enforce a minimum of 1 clause
    pub(super) fn parse_where<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> Self {
        let mut c = HashMap::with_capacity(2);
        Self::parse_where_and_append_to(state, &mut c);
        state.poison_if(c.is_empty());
        Self { c }
    }
}

#[cfg(test)]
mod impls {
    use {
        super::{RelationalExpr, WhereClause},
        crate::engine::{
            error::{QueryError, QueryResult},
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for WhereClause<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        // important: upstream must verify this
        const VERIFY_STATE_BEFORE_RETURN: bool = true;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            let wh = Self::parse_where(state);
            Ok(wh)
        }
    }
    impl<'a> ASTNode<'a> for RelationalExpr<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = false;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            Self::try_parse(state).ok_or(QueryError::QLInvalidSyntax)
        }
    }
}
