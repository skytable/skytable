/*
 * Created on Tue Sep 13 2022
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

use {
    super::{
        ddl, dml,
        lexer::{LitIR, Slice, Token},
        schema, LangError, LangResult,
    },
    crate::util::compiler,
};

pub trait QueryData<'a> {
    /// Check if the given token is a lit, while also checking `self`'s data if necessary
    fn can_read_lit_from(&self, tok: &Token) -> bool;
    /// Read a lit using the given token, using `self`'s data as necessary
    ///
    /// ## Safety
    /// The current token **must match** the signature of a lit
    unsafe fn read_lit(&mut self, tok: &'a Token) -> LitIR<'a>;
}

pub struct InplaceData;
impl InplaceData {
    #[inline(always)]
    pub const fn new() -> Self {
        Self
    }
}

impl<'a> QueryData<'a> for InplaceData {
    #[inline(always)]
    fn can_read_lit_from(&self, tok: &Token) -> bool {
        tok.is_lit()
    }
    #[inline(always)]
    unsafe fn read_lit(&mut self, tok: &'a Token) -> LitIR<'a> {
        extract!(tok, Token::Lit(l) => l.as_ir())
    }
}

pub struct SubstitutedData<'a> {
    data: &'a [LitIR<'a>],
}
impl<'a> SubstitutedData<'a> {
    #[inline(always)]
    pub const fn new(src: &'a [LitIR<'a>]) -> Self {
        Self { data: src }
    }
}

impl<'a> QueryData<'a> for SubstitutedData<'a> {
    #[inline(always)]
    fn can_read_lit_from(&self, tok: &Token) -> bool {
        Token![?].eq(tok) && !self.data.is_empty()
    }
    #[inline(always)]
    unsafe fn read_lit(&mut self, tok: &'a Token) -> LitIR<'a> {
        debug_assert!(Token![?].eq(tok));
        let ret = self.data[0];
        self.data = &self.data[1..];
        ret
    }
}

/*
    AST
*/

#[derive(Debug, PartialEq)]
/// An [`Entity`] represents the location for a specific structure, such as a model
pub enum Entity<'a> {
    /// A partial entity is used when switching to a model wrt the currently set space (commonly used
    /// when running `use` queries)
    ///
    /// syntax:
    /// ```sql
    /// :model
    /// ```
    Partial(Slice<'a>),
    /// A single entity is used when switching to a model wrt the currently set space (commonly used
    /// when running DML queries)
    ///
    /// syntax:
    /// ```sql
    /// model
    /// ```
    Single(Slice<'a>),
    /// A full entity is a complete definition to a model wrt to the given space (commonly used with
    /// DML queries)
    ///
    /// syntax:
    /// ```sql
    /// space.model
    /// ```
    Full(Slice<'a>, Slice<'a>),
}

impl<'a> From<(Slice<'a>, Slice<'a>)> for Entity<'a> {
    #[inline(always)]
    fn from((space, model): (Slice<'a>, Slice<'a>)) -> Self {
        Self::Full(space, model)
    }
}

impl<'a> Entity<'a> {
    #[inline(always)]
    /// Parse a full entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a full entity
    pub(super) unsafe fn full_entity_from_slice(sl: &'a [Token]) -> Self {
        Entity::Full(
            extract!(&sl[0], Token::Ident(sl) => sl.clone()),
            extract!(&sl[2], Token::Ident(sl) => sl.clone()),
        )
    }
    #[inline(always)]
    /// Parse a single entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a single entity
    pub(super) unsafe fn single_entity_from_slice(sl: &'a [Token]) -> Self {
        Entity::Single(extract!(&sl[0], Token::Ident(sl) => sl.clone()))
    }
    #[inline(always)]
    /// Parse a partial entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a partial entity
    pub(super) unsafe fn partial_entity_from_slice(sl: &'a [Token]) -> Self {
        Entity::Partial(extract!(&sl[1], Token::Ident(sl) => sl.clone()))
    }
    #[inline(always)]
    /// Returns true if the given token stream matches the signature of partial entity syntax
    pub(super) fn tokens_with_partial(tok: &[Token]) -> bool {
        tok.len() > 1 && tok[0] == Token![:] && tok[1].is_ident()
    }
    #[inline(always)]
    /// Returns true if the given token stream matches the signature of single entity syntax
    ///
    /// ⚠ WARNING: This will pass for full and single
    pub(super) fn tokens_with_single(tok: &[Token]) -> bool {
        !tok.is_empty() && tok[0].is_ident()
    }
    #[inline(always)]
    /// Returns true if the given token stream matches the signature of full entity syntax
    pub(super) fn tokens_with_full(tok: &[Token]) -> bool {
        tok.len() > 2 && tok[0].is_ident() && tok[1] == Token![.] && tok[2].is_ident()
    }
    #[inline(always)]
    /// Attempt to parse an entity using the given token stream. It also accepts a counter
    /// argument to forward the cursor
    pub fn parse_from_tokens(tok: &'a [Token], c: &mut usize) -> LangResult<Self> {
        let is_partial = Self::tokens_with_partial(tok);
        let is_current = Self::tokens_with_single(tok);
        let is_full = Self::tokens_with_full(tok);
        let r = match () {
            _ if is_full => unsafe {
                *c += 3;
                Self::full_entity_from_slice(tok)
            },
            _ if is_current => unsafe {
                *c += 1;
                Self::single_entity_from_slice(tok)
            },
            _ if is_partial => unsafe {
                *c += 2;
                Self::partial_entity_from_slice(tok)
            },
            _ => return Err(LangError::UnexpectedToken),
        };
        Ok(r)
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
/// A [`Statement`] is a fully BlueQL statement that can be executed by the query engine
// TODO(@ohsayan): Determine whether we actually need this
pub enum Statement<'a> {
    /// DDL query to switch between spaces and models
    Use(Entity<'a>),
    /// DDL query to create a model
    CreateModel(schema::Model<'a>),
    /// DDL query to create a space
    CreateSpace(schema::Space<'a>),
    /// DDL query to alter a space (properties)
    AlterSpace(schema::AlterSpace<'a>),
    /// DDL query to alter a model (properties, field types, etc)
    AlterModel(schema::Alter<'a>),
    /// DDL query to drop a model
    ///
    /// Conditions:
    /// - Model view is empty
    /// - Model is not in active use
    DropModel(ddl::DropModel<'a>),
    /// DDL query to drop a space
    ///
    /// Conditions:
    /// - Space doesn't have any other structures
    /// - Space is not in active use
    DropSpace(ddl::DropSpace<'a>),
    /// DDL query to inspect a space (returns a list of models in the space)
    InspectSpace(Slice<'a>),
    /// DDL query to inspect a model (returns the model definition)
    InspectModel(Entity<'a>),
    /// DDL query to inspect all spaces (returns a list of spaces in the database)
    InspectSpaces,
    /// DML insert
    Insert(dml::InsertStatement<'a>),
    /// DML select
    Select(dml::SelectStatement<'a>),
    /// DML update
    Update(dml::UpdateStatement<'a>),
    /// DML delete
    Delete(dml::DeleteStatement<'a>),
}

pub fn compile<'a, Qd: QueryData<'a>>(tok: &'a [Token], mut qd: Qd) -> LangResult<Statement<'a>> {
    let mut i = 0;
    let ref mut qd = qd;
    if compiler::unlikely(tok.len() < 2) {
        return Err(LangError::UnexpectedEndofStatement);
    }
    match tok[0] {
        // DDL
        Token![use] => Entity::parse_from_tokens(&tok[1..], &mut i).map(Statement::Use),
        Token![create] => match tok[1] {
            Token![model] => schema::parse_schema_from_tokens(&tok[2..], qd).map(|(q, c)| {
                i += c;
                Statement::CreateModel(q)
            }),
            Token![space] => schema::parse_space_from_tokens(&tok[2..], qd).map(|(q, c)| {
                i += c;
                Statement::CreateSpace(q)
            }),
            _ => compiler::cold_rerr(LangError::UnknownCreateStatement),
        },
        Token![drop] if tok.len() >= 3 => ddl::parse_drop(&tok[1..], &mut i),
        Token![alter] => match tok[1] {
            Token![model] => schema::parse_alter_kind_from_tokens(&tok[2..], qd, &mut i)
                .map(Statement::AlterModel),
            Token![space] => {
                schema::parse_alter_space_from_tokens(&tok[2..], qd).map(|(q, incr)| {
                    i += incr;
                    Statement::AlterSpace(q)
                })
            }
            _ => compiler::cold_rerr(LangError::UnknownAlterStatement),
        },
        Token::Ident(id) if id.eq_ignore_ascii_case(b"inspect") => {
            ddl::parse_inspect(&tok[1..], &mut i)
        }
        // DML
        Token![insert] => dml::parse_insert(&tok[1..], qd, &mut i).map(Statement::Insert),
        Token![select] => dml::parse_select(&tok[1..], qd, &mut i).map(Statement::Select),
        Token![update] => {
            dml::UpdateStatement::parse_update(&tok[1..], qd, &mut i).map(Statement::Update)
        }
        Token![delete] => {
            dml::DeleteStatement::parse_delete(&tok[1..], qd, &mut i).map(Statement::Delete)
        }
        _ => compiler::cold_rerr(LangError::ExpectedStatement),
    }
}
