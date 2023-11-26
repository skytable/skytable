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

#[cfg(test)]
use crate::engine::ql::{ast::InplaceData, lex::Token};
use crate::engine::{
    error::{QueryError, QueryResult},
    ql::ast::{QueryData, State},
};

/// An AST node
pub trait ASTNode<'a>: Sized {
    /// This AST node MUST use the full token range
    const MUST_USE_FULL_TOKEN_RANGE: bool;
    /// This AST node MUST use the full token range, and it also verifies that this is the case
    const VERIFIES_FULL_TOKEN_RANGE_USAGE: bool;
    /// This AST node doesn't handle "deep errors" (for example, recursive collections)
    const VERIFY_STATE_BEFORE_RETURN: bool = false;
    /// A hardened parse that guarantees:
    /// - The result is verified (even if it is a deep error)
    /// - The result utilizes the full token range
    fn parse_from_state_hardened<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self> {
        let r = Self::__base_impl_parse_from_state(state)?;
        if Self::VERIFY_STATE_BEFORE_RETURN {
            // must verify
            if !state.okay() {
                return Err(QueryError::QLInvalidSyntax);
            }
        }
        if Self::MUST_USE_FULL_TOKEN_RANGE {
            if !Self::VERIFIES_FULL_TOKEN_RANGE_USAGE {
                if state.not_exhausted() {
                    return Err(QueryError::QLInvalidSyntax);
                }
            }
        }
        Ok(r)
    }
    /// Parse this AST node from the given state
    ///
    /// Note to implementors:
    /// - If the implementor uses a cow style parse, then set [`ASTNode::VERIFY`] to
    /// true
    /// - Try to propagate errors via [`State`] if possible
    fn __base_impl_parse_from_state<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self>;
    #[cfg(test)]
    fn test_parse_from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Self> {
        let r = <Self as ASTNode>::__base_impl_parse_from_state(state);
        if Self::VERIFY_STATE_BEFORE_RETURN {
            return if state.okay() {
                r
            } else {
                Err(QueryError::QLInvalidSyntax)
            };
        }
        r
    }
    #[cfg(test)]
    /// Parse multiple nodes of this AST node type. Intended for the test suite.
    fn _multiple_from_state<Qd: QueryData<'a>>(_: &mut State<'a, Qd>) -> QueryResult<Vec<Self>> {
        unimplemented!()
    }
    #[cfg(test)]
    fn multiple_from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> QueryResult<Vec<Self>> {
        let r = <Self as ASTNode>::_multiple_from_state(state);
        if Self::VERIFY_STATE_BEFORE_RETURN {
            return if state.okay() {
                r
            } else {
                Err(QueryError::QLInvalidSyntax)
            };
        }
        r
    }
    #[cfg(test)]
    /// Parse this AST node utilizing the full token-stream. Intended for the test suite.
    fn from_insecure_tokens_full(tok: &'a [Token<'a>]) -> QueryResult<Self> {
        let mut state = State::new(tok, InplaceData::new());
        let r = <Self as ASTNode>::test_parse_from_state(&mut state)?;
        assert!(state.exhausted());
        Ok(r)
    }
    #[cfg(test)]
    fn from_insecure_tokens_full_with_space(
        tok: &'a [Token<'a>],
        space_name: &'static str,
    ) -> QueryResult<Self> {
        let mut state = State::new(tok, InplaceData::new());
        state.set_space(space_name);
        let r = <Self as ASTNode>::test_parse_from_state(&mut state)?;
        assert!(state.exhausted());
        Ok(r)
    }
    #[cfg(test)]
    /// Parse multiple nodes of this AST node type, utilizing the full token stream.
    /// Intended for the test suite.
    fn multiple_from_insecure_tokens_full(tok: &'a [Token<'a>]) -> QueryResult<Vec<Self>> {
        let mut state = State::new(tok, InplaceData::new());
        let r = Self::multiple_from_state(&mut state);
        if state.exhausted() && state.okay() {
            r
        } else {
            Err(QueryError::QLInvalidSyntax)
        }
    }
}

#[cfg(test)]
pub fn parse_ast_node_full<'a, N: ASTNode<'a>>(tok: &'a [Token<'a>]) -> QueryResult<N> {
    N::from_insecure_tokens_full(tok)
}
#[cfg(test)]
pub fn parse_ast_node_full_with_space<'a, N: ASTNode<'a>>(
    tok: &'a [Token<'a>],
    space_name: &'static str,
) -> QueryResult<N> {
    N::from_insecure_tokens_full_with_space(tok, space_name)
}
#[cfg(test)]
pub fn parse_ast_node_multiple_full<'a, N: ASTNode<'a>>(
    tok: &'a [Token<'a>],
) -> QueryResult<Vec<N>> {
    N::multiple_from_insecure_tokens_full(tok)
}
