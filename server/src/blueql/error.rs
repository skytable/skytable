/*
 * Created on Tue Jun 14 2022
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

use crate::{
    actions::{ActionError, ActionResult},
    protocol::interface::ProtocolSpec,
};

#[derive(Debug, PartialEq)]
#[repr(u8)]
/// BlueQL errors
pub enum LangError {
    /// Invalid syntax
    InvalidSyntax,
    /// Invalid numeric literal
    InvalidNumericLiteral,
    /// Unexpected end-of-statement
    UnexpectedEOF,
    /// Expected a statement but found some other token
    ExpectedStatement,
    /// Got an unknown create query
    UnknownCreateQuery,
    /// Bad expression
    BadExpression,
    /// An invalid string literal
    InvalidStringLiteral,
    /// Unsupported model declaration
    UnsupportedModelDeclaration,
    /// Unexpected character
    UnexpectedChar,
}

/// Results for BlueQL
pub type LangResult<T> = Result<T, LangError>;

#[inline(never)]
#[cold]
pub(super) const fn cold_err<P: ProtocolSpec>(e: LangError) -> &'static [u8] {
    match e {
        LangError::BadExpression => P::BQL_BAD_EXPRESSION,
        LangError::ExpectedStatement => P::BQL_EXPECTED_STMT,
        LangError::InvalidNumericLiteral => P::BQL_INVALID_NUMERIC_LITERAL,
        LangError::InvalidStringLiteral => P::BQL_INVALID_STRING_LITERAL,
        LangError::InvalidSyntax => P::BQL_INVALID_SYNTAX,
        LangError::UnexpectedEOF => P::BQL_UNEXPECTED_EOF,
        LangError::UnknownCreateQuery => P::BQL_UNKNOWN_CREATE_QUERY,
        LangError::UnsupportedModelDeclaration => P::BQL_UNSUPPORTED_MODEL_DECL,
        LangError::UnexpectedChar => P::BQL_UNEXPECTED_CHAR,
    }
}

#[inline(always)]
pub fn map_ql_err_to_resp<T, P: ProtocolSpec>(e: LangResult<T>) -> ActionResult<T> {
    match e {
        Ok(v) => Ok(v),
        Err(e) => Err(ActionError::ActionError(cold_err::<P>(e))),
    }
}
