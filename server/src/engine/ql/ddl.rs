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

use super::{
    ast::{Entity, Statement},
    lexer::Token,
    LangError, LangResult, RawSlice,
};

#[derive(Debug, PartialEq)]
/// A generic representation of `drop` query
pub struct DropSpace {
    pub(super) space: RawSlice,
    pub(super) force: bool,
}

impl DropSpace {
    #[inline(always)]
    /// Instantiate
    pub(super) const fn new(space: RawSlice, force: bool) -> Self {
        Self { space, force }
    }
}

#[derive(Debug, PartialEq)]
pub struct DropModel {
    pub(super) entity: Entity,
    pub(super) force: bool,
}

impl DropModel {
    #[inline(always)]
    pub fn new(entity: Entity, force: bool) -> Self {
        Self { entity, force }
    }
}

// drop (<space> | <model>) <ident> [<force>]
pub(super) fn parse_drop(tok: &[Token], counter: &mut usize) -> LangResult<Statement> {
    match tok.get(0) {
        Some(Token![model]) => {
            // we have a model. now parse entity and see if we should force deletion
            let mut i = 1;
            let e = Entity::parse_from_tokens(&tok[1..], &mut i)?;
            let force = i < tok.len() && tok[i] == Token::Ident("force".into());
            i += force as usize;
            *counter += i;
            // if we've exhausted the stream, we're good to go (either `force`, or nothing)
            if tok.len() == i {
                return Ok(Statement::DropModel(DropModel::new(e, force)));
            }
        }
        Some(Token![space]) if tok.len() > 1 && tok[1].is_ident() => {
            let mut i = 2; // (`space` and space name)
                           // should we force drop?
            let force = i < tok.len() && tok[i] == Token::Ident("force".into());
            i += force as usize;
            *counter += i;
            // either `force` or nothing
            if tok.len() == i {
                return Ok(Statement::DropSpace(DropSpace::new(
                    unsafe {
                        // UNSAFE(@ohsayan): Safe because the match predicate ensures that tok[1] is indeed an ident
                        extract!(tok[1], Token::Ident(ref space) => space.clone())
                    },
                    force,
                )));
            }
        }
        _ => {}
    }
    Err(LangError::UnexpectedToken)
}

#[cfg(test)]
pub(super) fn parse_drop_full(tok: &[Token]) -> LangResult<Statement> {
    let mut i = 0;
    let r = self::parse_drop(tok, &mut i);
    assert_full_tt!(i, tok.len());
    r
}

pub(super) fn parse_inspect(tok: &[Token], c: &mut usize) -> LangResult<Statement> {
    /*
        inpsect model <entity>
        inspect space <entity>
        inspect spaces
    */

    let nxt = tok.get(0);
    *c += nxt.is_some() as usize;
    match nxt {
        Some(Token![model]) => Entity::parse_from_tokens(&tok[1..], c).map(Statement::InspectModel),
        Some(Token![space]) if tok.len() == 2 && tok[1].is_ident() => {
            *c += 1;
            Ok(Statement::InspectSpace(unsafe {
                // UNSAFE(@ohsayan): Safe because of the match predicate
                extract!(tok[1], Token::Ident(ref space) => space.clone())
            }))
        }
        Some(Token::Ident(id))
            if unsafe {
                // UNSAFE(@ohsayan): Token lifetime ensures validity of slice
                id.as_slice().eq_ignore_ascii_case(b"spaces")
            } && tok.len() == 1 =>
        {
            Ok(Statement::InspectSpaces)
        }
        _ => Err(LangError::ExpectedStatement),
    }
}

#[cfg(test)]
pub(super) fn parse_inspect_full(tok: &[Token]) -> LangResult<Statement> {
    let mut i = 0;
    let r = self::parse_inspect(tok, &mut i);
    assert_full_tt!(i, tok.len());
    r
}
