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
        ddl,
        lexer::{InsecureLexer, LitIR, Token},
        schema, LangError, LangResult, RawSlice,
    },
    crate::util::Life,
    core::{marker::PhantomData, slice},
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
    pub const fn new() -> Self {
        Self
    }
}

impl<'a> QueryData<'a> for InplaceData {
    fn can_read_lit_from(&self, tok: &Token) -> bool {
        tok.is_lit()
    }
    unsafe fn read_lit(&mut self, tok: &'a Token) -> LitIR<'a> {
        extract!(tok, Token::Lit(l) => l.as_ir())
    }
}

pub struct SubstitutedData<'a> {
    data: &'a [LitIR<'a>],
}
impl<'a> SubstitutedData<'a> {
    pub const fn new(src: &'a [LitIR<'a>]) -> Self {
        Self { data: src }
    }
}

impl<'a> QueryData<'a> for SubstitutedData<'a> {
    fn can_read_lit_from(&self, tok: &Token) -> bool {
        Token![?].eq(tok) && !self.data.is_empty()
    }
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
pub enum Entity {
    /// A partial entity is used when switching to a model wrt the currently set space (commonly used
    /// when running `use` queries)
    ///
    /// syntax:
    /// ```sql
    /// :model
    /// ```
    Partial(RawSlice),
    /// A single entity is used when switching to a model wrt the currently set space (commonly used
    /// when running DML queries)
    ///
    /// syntax:
    /// ```sql
    /// model
    /// ```
    Single(RawSlice),
    /// A full entity is a complete definition to a model wrt to the given space (commonly used with
    /// DML queries)
    ///
    /// syntax:
    /// ```sql
    /// space.model
    /// ```
    Full(RawSlice, RawSlice),
}

impl<T: Into<RawSlice>, U: Into<RawSlice>> From<(T, U)> for Entity {
    fn from((space, model): (T, U)) -> Self {
        Self::Full(space.into(), model.into())
    }
}

impl Entity {
    #[inline(always)]
    /// Parse a full entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a full entity
    pub(super) unsafe fn full_entity_from_slice(sl: &[Token]) -> Self {
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
    pub(super) unsafe fn single_entity_from_slice(sl: &[Token]) -> Self {
        Entity::Single(extract!(&sl[0], Token::Ident(sl) => sl.clone()))
    }
    #[inline(always)]
    /// Parse a partial entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a partial entity
    pub(super) unsafe fn partial_entity_from_slice(sl: &[Token]) -> Self {
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
    pub(super) fn parse_from_tokens(tok: &[Token], c: &mut usize) -> LangResult<Self> {
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
    #[inline(always)]
    /// Parse an entity using the given [`Compiler`] instance. Internally this just evalutes it
    /// using a token stream, finally forwarding the [`Compiler`]'s internal cursor depending on the
    /// number of bytes consumed
    pub(super) fn parse(cm: &mut Compiler) -> LangResult<Self> {
        let sl = cm.remslice();
        let mut c = 0;
        let r = Self::parse_from_tokens(sl, &mut c);
        unsafe {
            cm.incr_cursor_by(c);
        }
        r
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
/// A [`Statement`] is a fully BlueQL statement that can be executed by the query engine
// TODO(@ohsayan): Determine whether we actually need this
pub enum Statement {
    /// DDL query to switch between spaces and models
    Use(Entity),
    /// DDL query to create a model
    CreateModel(schema::Model),
    /// DDL query to create a space
    CreateSpace(schema::Space),
    /// DDL query to alter a space (properties)
    AlterSpace(schema::AlterSpace),
    /// DDL query to alter a model (properties, field types, etc)
    AlterModel(schema::Alter),
    /// DDL query to drop a model
    ///
    /// Conditions:
    /// - Model view is empty
    /// - Model is not in active use
    DropModel(ddl::DropModel),
    /// DDL query to drop a space
    ///
    /// Conditions:
    /// - Space doesn't have any other structures
    /// - Space is not in active use
    DropSpace(ddl::DropSpace),
    /// DDL query to inspect a space (returns a list of models in the space)
    InspectSpace(RawSlice),
    /// DDL query to inspect a model (returns the model definition)
    InspectModel(Entity),
    /// DDL query to inspect all spaces (returns a list of spaces in the database)
    InspectSpaces,
}

/// A [`Compiler`] for BlueQL queries
// TODO(@ohsayan): Decide whether we need this
pub struct Compiler<'a> {
    c: *const Token,
    e: *const Token,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Compiler<'a> {
    /// Compile a BlueQL query
    pub fn compile(src: &'a [u8]) -> LangResult<Life<'a, Statement>> {
        let token_stream = InsecureLexer::lex(src)?;
        Self::new(&token_stream).compile_link_lt()
    }
    #[inline(always)]
    /// Create a new [`Compiler`] instance
    pub(super) const fn new(token_stream: &[Token]) -> Self {
        unsafe {
            Self {
                c: token_stream.as_ptr(),
                e: token_stream.as_ptr().add(token_stream.len()),
                _lt: PhantomData,
            }
        }
    }
    #[inline(always)]
    /// Utility method to link a lifetime to the statement since the statement makes use of some
    /// unsafe lifetime-free code that would otherwise cause the program to crash and burn
    fn compile_link_lt(mut self) -> LangResult<Life<'a, Statement>> {
        match self.stage0() {
            Ok(t) if self.exhausted() => Ok(Life::new(t)),
            Err(e) => Err(e),
            _ => Err(LangError::UnexpectedToken),
        }
    }
    #[inline(always)]
    /// Stage 0: what statement
    fn stage0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt_forward() {
            Some(Token![create]) => self.create0(),
            Some(Token![drop]) => self.drop0(),
            Some(Token![alter]) => self.alter0(),
            Some(Token![describe]) => self.inspect0(),
            Some(Token![use]) => self.use0(),
            _ => Err(LangError::ExpectedStatement),
        }
    }
    #[inline(always)]
    /// Create 0: Create what (model/space)
    fn create0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt_forward() {
            Some(Token![model]) => self.c_model0(),
            Some(Token![space]) => self.c_space0(),
            _ => Err(LangError::UnexpectedEndofStatement),
        }
    }
    #[inline(always)]
    /// Drop 0: Drop what (model/space)
    fn drop0(&mut self) -> Result<Statement, LangError> {
        let mut i = 0;
        let r = ddl::parse_drop(self.remslice(), &mut i);
        unsafe {
            self.incr_cursor_by(i);
        }
        r
    }
    #[inline(always)]
    /// Alter 0: Alter what (model/space)
    fn alter0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt_forward() {
            Some(Token![model]) => self.alter_model(),
            Some(Token![space]) => self.alter_space(),
            Some(_) => Err(LangError::ExpectedStatement),
            None => Err(LangError::UnexpectedEndofStatement),
        }
    }
    #[inline(always)]
    /// Alter model
    fn alter_model(&mut self) -> Result<Statement, LangError> {
        let mut c = 0;
        let r =
            schema::parse_alter_kind_from_tokens(self.remslice(), &mut InplaceData::new(), &mut c);
        unsafe {
            self.incr_cursor_by(c);
        }
        r.map(Statement::AlterModel)
    }
    #[inline(always)]
    /// Alter space
    fn alter_space(&mut self) -> Result<Statement, LangError> {
        let (alter, i) =
            schema::parse_alter_space_from_tokens(self.remslice(), &mut InplaceData::new())?;
        unsafe {
            self.incr_cursor_by(i);
        }
        Ok(Statement::AlterSpace(alter))
    }
    #[inline(always)]
    /// Inspect 0: Inpsect what (model/space/spaces)
    fn inspect0(&mut self) -> Result<Statement, LangError> {
        let mut i = 0;
        let r = ddl::parse_inspect(self.remslice(), &mut i);
        unsafe {
            self.incr_cursor_by(i);
        }
        r
    }
    #[inline(always)]
    /// Parse an `use` query
    fn use0(&mut self) -> Result<Statement, LangError> {
        let entity = Entity::parse(self)?;
        Ok(Statement::Use(entity))
    }
    #[inline(always)]
    /// Create model
    fn c_model0(&mut self) -> Result<Statement, LangError> {
        let (model, i) =
            schema::parse_schema_from_tokens(self.remslice(), &mut InplaceData::new())?;
        unsafe {
            self.incr_cursor_by(i);
        }
        Ok(Statement::CreateModel(model))
    }
    #[inline(always)]
    /// Create space
    fn c_space0(&mut self) -> Result<Statement, LangError> {
        let (space, i) = schema::parse_space_from_tokens(self.remslice(), &mut InplaceData::new())?;
        unsafe {
            self.incr_cursor_by(i);
        }
        Ok(Statement::CreateSpace(space))
    }
}

impl<'a> Compiler<'a> {
    #[inline(always)]
    /// Attempt to read the next token and forward the interal cursor if there is a cursor ahead
    pub(super) fn nxtok_opt_forward<'b>(&mut self) -> Option<&'b Token>
    where
        'a: 'b,
    {
        if self.not_exhausted() {
            unsafe {
                let r = Some(&*self.c);
                self.incr_cursor();
                r
            }
        } else {
            None
        }
    }
    #[inline(always)]
    /// Returns the cursor
    pub(super) const fn cursor(&self) -> *const Token {
        self.c
    }
    #[inline(always)]
    /// Returns the remaining buffer as a slice
    pub(super) fn remslice(&'a self) -> &'a [Token] {
        unsafe { slice::from_raw_parts(self.c, self.remaining()) }
    }
    #[inline(always)]
    /// Check if the buffer is not exhausted
    pub(super) fn not_exhausted(&self) -> bool {
        self.c != self.e
    }
    #[inline(always)]
    /// Check if the buffer is exhausted
    pub(super) fn exhausted(&self) -> bool {
        self.c == self.e
    }
    #[inline(always)]
    /// Check the remaining bytes in the buffer
    pub(super) fn remaining(&self) -> usize {
        unsafe { self.e.offset_from(self.c) as usize }
    }
    /// Deref the cursor
    ///
    /// ## Safety
    ///
    /// Have to ensure it isn't pointing to garbage i.e beyond EOA
    pub(super) unsafe fn deref_cursor(&self) -> &Token {
        &*self.c
    }
    /// Increment the cursor if the next token matches the given token
    pub(super) fn peek_eq_and_forward(&mut self, t: Token) -> bool {
        let did_fw = self.not_exhausted() && unsafe { self.deref_cursor() == &t };
        unsafe {
            self.incr_cursor_if(did_fw);
        }
        did_fw
    }
    #[inline(always)]
    /// Increment the cursor
    ///
    /// ## Safety
    ///
    /// Should be >= EOA
    pub(super) unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    /// Increment the cursor if the given boolean expr is satisified
    ///
    /// ## Safety
    ///
    /// Should be >= EOA (if true)
    pub(super) unsafe fn incr_cursor_if(&mut self, did_fw: bool) {
        self.incr_cursor_by(did_fw as _)
    }
    #[inline(always)]
    /// Increment the cursor by the given count
    ///
    /// ## Safety
    ///
    /// >= EOA (if nonzero)
    pub(super) unsafe fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.remaining() >= by);
        self.c = self.c.add(by);
    }
}
