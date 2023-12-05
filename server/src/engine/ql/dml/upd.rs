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
    super::{u, WhereClause},
    crate::{
        engine::{
            core::{query_meta::AssignmentOperator, EntityIDRef},
            data::lit::Lit,
            error::{QueryError, QueryResult},
            ql::{
                ast::{QueryData, State},
                lex::Ident,
            },
        },
        util::compiler,
    },
};

/*
    Impls for update
*/

static OPERATOR: [AssignmentOperator; 6] = [
    AssignmentOperator::Assign,
    AssignmentOperator::Assign,
    AssignmentOperator::AddAssign,
    AssignmentOperator::SubAssign,
    AssignmentOperator::MulAssign,
    AssignmentOperator::DivAssign,
];

#[derive(Debug, PartialEq)]
pub struct AssignmentExpression<'a> {
    /// the LHS ident
    pub lhs: Ident<'a>,
    /// the RHS lit
    pub rhs: Lit<'a>,
    /// operator
    pub operator_fn: AssignmentOperator,
}

impl<'a> AssignmentExpression<'a> {
    pub fn new(lhs: Ident<'a>, rhs: Lit<'a>, operator_fn: AssignmentOperator) -> Self {
        Self {
            lhs,
            rhs,
            operator_fn,
        }
    }
    fn parse_and_append_expression<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
        expressions: &mut Vec<Self>,
    ) {
        /*
            smallest expr:
            x = y
        */
        if compiler::unlikely(state.remaining() < 3) {
            state.poison();
            return;
        }
        let lhs = state.fw_read();
        state.poison_if_not(lhs.is_ident());
        let op_ass = u(state.cursor_eq(Token![=]));
        let op_add = u(state.cursor_eq(Token![+])) * 2;
        let op_sub = u(state.cursor_eq(Token![-])) * 3;
        let op_mul = u(state.cursor_eq(Token![*])) * 4;
        let op_div = u(state.cursor_eq(Token![/])) * 5;
        let operator_code = op_ass + op_add + op_sub + op_mul + op_div;
        unsafe {
            // UNSAFE(@ohsayan): A hint, obvious from above
            if operator_code > 5 {
                impossible!();
            }
        }
        state.cursor_ahead();
        state.poison_if(operator_code == 0);
        let has_double_assign = state.cursor_rounded_eq(Token![=]);
        let double_assign_okay = operator_code != 1 && has_double_assign;
        let single_assign_okay = operator_code == 1 && !double_assign_okay;
        state.poison_if_not(single_assign_okay | double_assign_okay);
        state.cursor_ahead_if(double_assign_okay);
        state.poison_if_not(state.can_read_lit_rounded());

        if state.okay() {
            unsafe {
                // UNSAFE(@ohsayan): Checked lit, state flag ensures we have ident for lhs
                let rhs = state.read_cursor_lit_unchecked();
                state.cursor_ahead();
                expressions.push(AssignmentExpression::new(
                    // UNSAFE(@ohsayan): we verified if `lhs` returns `is_ident`
                    lhs.uck_read_ident(),
                    rhs,
                    OPERATOR[operator_code as usize],
                ))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct UpdateStatement<'a> {
    pub(super) entity: EntityIDRef<'a>,
    pub(super) expressions: Vec<AssignmentExpression<'a>>,
    pub(super) wc: WhereClause<'a>,
}

impl<'a> UpdateStatement<'a> {
    pub fn entity(&self) -> EntityIDRef<'a> {
        self.entity
    }
    pub fn expressions(&self) -> &[AssignmentExpression<'a>] {
        &self.expressions
    }
    pub fn clauses_mut(&mut self) -> &mut WhereClause<'a> {
        &mut self.wc
    }
    pub fn into_expressions(self) -> Vec<AssignmentExpression<'a>> {
        self.expressions
    }
}

impl<'a> UpdateStatement<'a> {
    #[inline(always)]
    #[cfg(test)]
    pub fn new(
        entity: EntityIDRef<'a>,
        expressions: Vec<AssignmentExpression<'a>>,
        wc: WhereClause<'a>,
    ) -> Self {
        Self {
            entity,
            expressions,
            wc,
        }
    }
    #[inline(always)]
    pub fn parse_update<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        /*
            TODO(@ohsayan): Allow volcanoes
            smallest tt:
            update model SET x  =  1 where x = 1
                   ^1    ^2  ^3 ^4 ^5^6    ^7^8^9
        */
        if compiler::unlikely(state.remaining() < 9) {
            return compiler::cold_rerr(QueryError::QLUnexpectedEndOfStatement);
        }
        // parse entity
        let entity = state.try_entity_buffered_into_state_uninit();
        if !(state.has_remaining(6)) {
            unsafe {
                // UNSAFE(@ohsayan): Obvious from above, max 3 fw
                impossible!();
            }
        }
        state.poison_if_not(state.cursor_eq(Token![set]));
        state.cursor_ahead(); // ignore errors if any
        let mut nx_where = false;
        let mut expressions = Vec::new();
        while state.not_exhausted() && state.okay() && !nx_where {
            AssignmentExpression::parse_and_append_expression(state, &mut expressions);
            let nx_comma = state.cursor_rounded_eq(Token![,]);
            nx_where = state.cursor_rounded_eq(Token![where]); // NOTE: volcano
            state.poison_if_not(nx_comma | nx_where);
            state.cursor_ahead_if(nx_comma);
        }
        state.poison_if_not(nx_where);
        state.cursor_ahead_if(state.okay());
        // check where clauses
        let mut clauses = <_ as Default>::default();
        WhereClause::parse_where_and_append_to(state, &mut clauses);
        state.poison_if(clauses.is_empty()); // NOTE: volcano
        if compiler::likely(state.okay()) {
            Ok(Self {
                entity: unsafe {
                    // UNSAFE(@ohsayan): This is safe because of `parse_entity` and `okay`
                    entity.assume_init()
                },
                expressions,
                wc: WhereClause::new(clauses),
            })
        } else {
            compiler::cold_rerr(QueryError::QLInvalidSyntax)
        }
    }
}

mod impls {
    use {
        super::UpdateStatement,
        crate::engine::{
            error::QueryResult,
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for UpdateStatement<'a> {
        const MUST_USE_FULL_TOKEN_RANGE: bool = true;
        const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
        fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
            state: &mut State<'a, Qd>,
        ) -> QueryResult<Self> {
            Self::parse_update(state)
        }
    }
    #[cfg(test)]
    mod test {
        use super::{super::AssignmentExpression, ASTNode, QueryData, QueryResult, State};
        impl<'a> ASTNode<'a> for AssignmentExpression<'a> {
            const MUST_USE_FULL_TOKEN_RANGE: bool = false;
            const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool = false;
            // important: upstream must verify this
            const VERIFY_STATE_BEFORE_RETURN: bool = true;
            fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
                state: &mut State<'a, Qd>,
            ) -> QueryResult<Self> {
                let mut expr = Vec::new();
                AssignmentExpression::parse_and_append_expression(state, &mut expr);
                state.poison_if_not(expr.len() == 1);
                Ok(expr.remove(0))
            }
            fn _multiple_from_state<Qd: QueryData<'a>>(
                state: &mut State<'a, Qd>,
            ) -> QueryResult<Vec<Self>> {
                let mut expr = Vec::new();
                AssignmentExpression::parse_and_append_expression(state, &mut expr);
                Ok(expr)
            }
        }
    }
}
