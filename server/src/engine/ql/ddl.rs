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

use super::{ast::Statement, lexer::Token, LangError, LangResult, RawSlice};

#[derive(Debug, PartialEq)]
pub struct DropItem(pub RawSlice, pub bool);

impl DropItem {
    #[inline(always)]
    pub(super) const fn new(slice: RawSlice, force: bool) -> Self {
        Self(slice, force)
    }
}

// drop (<space> | <model>) <ident> [<force>]
pub(super) fn parse_drop(tok: &[Token], counter: &mut usize) -> LangResult<Statement> {
    let l = tok.len();
    // drop space/model
    let mut i = 0;
    let drop_space = i < l && tok[i] == Token![space];
    let drop_model = i < l && tok[i] == Token![model];
    let mut okay = drop_space | drop_model;
    i += okay as usize;
    // check if we have the target entity name
    okay &= i < l && tok[i].is_ident();
    i += okay as usize;
    // next token is either `force` or end of stream
    let force_drop = i < l && tok[i] == Token::Ident("force".into());
    okay &= force_drop | (i == l);
    i += force_drop as usize;

    if !okay {
        return Err(LangError::UnexpectedToken);
    }

    let drop_item = DropItem(
        unsafe { extract!(tok[1], Token::Ident(ref id) => id.clone()) },
        force_drop,
    );

    *counter += i;

    let stmt = if drop_space {
        Statement::DropSpace(drop_item)
    } else {
        Statement::DropModel(drop_item)
    };
    Ok(stmt)
}

#[cfg(test)]
pub(super) fn parse_drop_full(tok: &[Token]) -> LangResult<Statement> {
    let mut i = 0;
    let r = self::parse_drop(tok, &mut i);
    assert_eq!(i, tok.len(), "full token stream not utilized");
    r
}
