/*
 * Created on Wed Feb 01 2023
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

use crate::{
    engine::{
        error::{LangError, LangResult},
        ql::{
            ast::{Entity, QueryData, State, Statement},
            lex::Token,
        },
    },
    util::compiler,
};

pub fn parse_inspect<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> LangResult<Statement<'a>> {
    /*
        inpsect model <entity>
        inspect space <entity>
        inspect spaces

        min length -> (<model> | <space>) <model> = 2
    */

    if compiler::unlikely(state.remaining() < 1) {
        return compiler::cold_rerr(LangError::UnexpectedEOS);
    }

    match state.fw_read() {
        Token![model] => {
            Entity::parse_from_state_rounded_result(state).map(Statement::InspectModel)
        }
        Token![space] if state.cursor_has_ident_rounded() => {
            Ok(Statement::InspectSpace(unsafe {
                // UNSAFE(@ohsayan): Safe because of the match predicate
                state.fw_read().uck_read_ident()
            }))
        }
        Token::Ident(id) if id.eq_ignore_ascii_case("spaces") && state.exhausted() => {
            Ok(Statement::InspectSpaces)
        }
        _ => {
            state.cursor_back();
            Err(LangError::ExpectedStatement)
        }
    }
}

pub use impls::InspectStatementAST;
mod impls {
    use crate::engine::{
        error::LangResult,
        ql::ast::{traits::ASTNode, QueryData, State, Statement},
    };
    #[derive(sky_macros::Wrapper, Debug)]
    pub struct InspectStatementAST<'a>(Statement<'a>);
    impl<'a> ASTNode<'a> for InspectStatementAST<'a> {
        fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
            super::parse_inspect(state).map(Self)
        }
    }
}
