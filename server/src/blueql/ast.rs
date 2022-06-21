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

use {
    super::{
        error::{LangError, LangResult},
        lexer::{Keyword, Lexer, Token, Type, TypeExpression},
        RawSlice,
    },
    crate::util::{compiler, Life},
    core::{marker::PhantomData, mem::transmute, ptr},
};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
#[repr(u8)]
/// A statement that can be executed
pub enum Statement {
    /// Create a new space with the provided ID
    CreateSpace(RawSlice),
    /// Create a new model with the provided configuration
    CreateModel {
        entity: Entity,
        model: FieldConfig,
        volatile: bool,
    },
    /// Drop the given model
    DropModel { entity: Entity, force: bool },
    /// Drop the given space
    DropSpace { entity: RawSlice, force: bool },
    /// Inspect the given space
    InspectSpace(Option<RawSlice>),
    /// Inspect the given model
    InspectModel(Option<Entity>),
    /// Inspect all the spaces in the database
    InspectSpaces,
    /// Switch to the given entity
    Use(Entity),
}

pub type StatementLT<'a> = Life<'a, Statement>;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum Entity {
    Current(RawSlice),
    Full(RawSlice, RawSlice),
}

impl Entity {
    const MAX_LENGTH_EX: usize = 65;
    pub fn from_slice(slice: &[u8]) -> LangResult<Self> {
        Compiler::new(&Lexer::lex(slice)?).parse_entity_name()
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// The field configuration used when declaring the fields for a model
pub struct FieldConfig {
    /// the types of the fields
    pub types: Vec<TypeExpression>,
    /// the names of the fields
    pub names: Vec<RawSlice>,
}

impl FieldConfig {
    /// Create an empty field configuration
    pub const fn new() -> Self {
        Self {
            types: Vec::new(),
            names: Vec::new(),
        }
    }
    // TODO(@ohsayan): Completely deprecate the model-code based API
    pub fn get_model_code(&self) -> LangResult<u8> {
        let Self { types, names } = self;
        let invalid_expr = {
            // the model API doesn't support named fields (it's super limited; we need to drop it)
            !names.is_empty()
            || types.len() != 2
            // the key type cannot be compound
            || types[0].0.len() != 1
            // the key type cannot be a list
            || types[0].0[0] == Type::List
            // the value cannot have a depth more than two
            || types[1].0.len() > 2
            // if the value is a string or binary, it cannot have a depth more than 1
            || ((types[1].0[0] == Type::Binary || types[1].0[0] == Type::String) && types[1].0.len() != 1)
            // if the value is a list, it must have a depth of two
            || (types[1].0[0] == Type::List && types[1].0.len() != 2)
            // if the value is a list, the type argument cannot be a list (it's stupid, I know; that's exactly
            // why I'll be ditching this API in the next two PRs)
            || (types[1].0[0] == Type::List && types[1].0[1] == Type::List)
        };
        if compiler::unlikely(invalid_expr) {
            // the value type cannot have a depth more than 2
            return Err(LangError::UnsupportedModelDeclaration);
        }
        let key_expr = &types[0].0;
        let value_expr = &types[1].0;
        if value_expr[0] == Type::List {
            let k_enc = key_expr[0] == Type::String;
            let v_enc = value_expr[1] == Type::String;
            Ok(((k_enc as u8) << 1) + (v_enc as u8) + 4)
        } else {
            let k_enc = key_expr[0] == Type::String;
            let v_enc = value_expr[0] == Type::String;
            let ret = k_enc as u8 + v_enc as u8;
            Ok((ret & 1) + ((k_enc as u8) << 1))
        }
    }
}

// expect state
#[derive(Debug)]
#[repr(u8)]
#[derive(PartialEq)]
/// What to expect next
enum Expect {
    /// Expect a type
    Type = 0,
    /// Expect a [`Token::CloseAngular`]
    Close = 1,
}

/// A compiler for BlueQL queries
///
/// This compiler takes an input stream and evaluates the query using a traditional
/// lexer-parser pipeline
pub struct Compiler<'a> {
    cursor: *const Token,
    end_ptr: *const Token,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Compiler<'a> {
    #[inline(always)]
    /// Check if we have not exhausted the token stream
    fn not_exhausted(&self) -> bool {
        self.cursor < self.end_ptr
    }
    #[inline(always)]
    /// Deref the cursor
    unsafe fn deref_cursor(&self) -> &Token {
        &*self.cursor
    }
    #[inline(always)]
    fn peek_neq(&self, token: &Token) -> bool {
        self.not_exhausted() && unsafe { self.deref_cursor() != token }
    }
    #[inline(always)]
    /// Check if the token ahead equals the given token
    fn peek_eq(&self, tok: &Token) -> bool {
        self.not_exhausted() && unsafe { self.deref_cursor() == tok }
    }
    #[inline(always)]
    /// Check if the token ahead equals the given token, moving the cursor ahead if so
    fn next_eq(&mut self, tok: &Token) -> bool {
        let next_is_eq = self.not_exhausted() && unsafe { self.deref_cursor() == tok };
        unsafe { self.incr_cursor_if(next_is_eq) };
        next_is_eq
    }
    #[inline(always)]
    /// Increment the cursor if the condition is true
    unsafe fn incr_cursor_if(&mut self, cond: bool) {
        self.incr_cursor_by(cond as usize)
    }
    #[inline(always)]
    /// Move the cursor ahead by `by` positions
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.add(by)
    }
    #[inline(always)]
    /// Move the cursor ahead by one
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    #[inline(always)]
    unsafe fn decr_cursor(&mut self) {
        self.decr_cursor_by(1)
    }
    #[inline(always)]
    unsafe fn decr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.sub(by)
    }
    #[inline(always)]
    /// Read the element ahead if we have not exhausted the token stream. This
    /// will forward the cursor
    fn next(&mut self) -> Option<Token> {
        if self.not_exhausted() {
            let r = Some(unsafe { ptr::read(self.cursor) });
            unsafe { self.incr_cursor() };
            r
        } else {
            None
        }
    }
    #[inline(always)]
    fn next_result(&mut self) -> LangResult<Token> {
        if compiler::likely(self.not_exhausted()) {
            let r = unsafe { ptr::read(self.cursor) };
            unsafe { self.incr_cursor() };
            Ok(r)
        } else {
            Err(LangError::UnexpectedEOF)
        }
    }
    #[inline(always)]
    fn next_ident(&mut self) -> LangResult<RawSlice> {
        match self.next() {
            Some(Token::Identifier(rws)) => Ok(rws),
            Some(_) => Err(LangError::InvalidSyntax),
            None => Err(LangError::UnexpectedEOF),
        }
    }
    #[inline(always)]
    /// Returns the remaining number of tokens
    fn remaining(&self) -> usize {
        self.end_ptr as usize - self.cursor as usize
    }
}

impl<'a> Compiler<'a> {
    #[inline(always)]
    #[cfg(test)]
    /// Compile the given BlueQL source
    pub fn compile(src: &'a [u8]) -> LangResult<Life<'a, Statement>> {
        Self::compile_with_extra(src, 0)
    }
    #[inline(always)]
    /// Compile the given BlueQL source with optionally supplied extra arguments
    /// HACK: Just helps us omit an additional check
    pub fn compile_with_extra(src: &'a [u8], len: usize) -> LangResult<Life<'a, Statement>> {
        let tokens = Lexer::lex(src)?;
        Self::new(&tokens).eval(len).map(Life::new)
    }
    #[inline(always)]
    pub const fn new(tokens: &[Token]) -> Self {
        unsafe {
            Self {
                cursor: tokens.as_ptr(),
                end_ptr: tokens.as_ptr().add(tokens.len()),
                _lt: PhantomData,
            }
        }
    }
    #[inline(always)]
    /// The inner eval method
    fn eval(&mut self, extra_len: usize) -> LangResult<Statement> {
        let stmt = match self.next() {
            Some(tok) => match tok {
                Token::Keyword(Keyword::Create) => self.parse_create0(),
                Token::Keyword(Keyword::Drop) => self.parse_drop0(),
                Token::Keyword(Keyword::Inspect) => self.parse_inspect0(),
                Token::Keyword(Keyword::Use) => self.parse_use0(),
                _ => Err(LangError::ExpectedStatement),
            },
            None => Err(LangError::UnexpectedEOF),
        };
        if compiler::likely(self.remaining() == 0 && extra_len == 0) {
            stmt
        } else {
            Err(LangError::InvalidSyntax)
        }
    }
    #[inline(always)]
    fn parse_use0(&mut self) -> LangResult<Statement> {
        Ok(Statement::Use(self.parse_entity_name()?))
    }
    #[inline(always)]
    /// Parse an inspect statement
    fn parse_inspect0(&mut self) -> LangResult<Statement> {
        match self.next_result()? {
            Token::Keyword(Keyword::Model) => self.parse_inspect_model0(),
            Token::Keyword(Keyword::Space) => self.parse_inspect_space0(),
            Token::Identifier(spaces)
                if unsafe { spaces.as_slice() }.eq_ignore_ascii_case(b"spaces") =>
            {
                Ok(Statement::InspectSpaces)
            }
            _ => Err(LangError::InvalidSyntax),
        }
    }
    #[inline(always)]
    /// Parse `inspect model <model>`
    fn parse_inspect_model0(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Identifier(ident)) => Ok(Statement::InspectModel(Some(
                self.parse_entity_name_with_start(ident)?,
            ))),
            Some(_) => Err(LangError::InvalidSyntax),
            None => Ok(Statement::InspectModel(None)),
        }
    }
    #[inline(always)]
    /// Parse `inspect space <space>`
    fn parse_inspect_space0(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Identifier(ident)) => Ok(Statement::InspectSpace(Some(ident))),
            Some(_) => Err(LangError::InvalidSyntax),
            None => Ok(Statement::InspectSpace(None)),
        }
    }
    #[inline(always)]
    /// Parse a drop statement
    fn parse_drop0(&mut self) -> LangResult<Statement> {
        let (drop_container, drop_id) = (self.next(), self.next());
        match (drop_container, drop_id) {
            (Some(Token::Keyword(Keyword::Model)), Some(Token::Identifier(model_name))) => {
                Ok(Statement::DropModel {
                    entity: self.parse_entity_name_with_start(model_name)?,
                    force: self.next_eq(&Token::Keyword(Keyword::Force)),
                })
            }
            (Some(Token::Keyword(Keyword::Space)), Some(Token::Identifier(space_name))) => {
                Ok(Statement::DropSpace {
                    entity: space_name,
                    force: self.next_eq(&Token::Keyword(Keyword::Force)),
                })
            }
            _ => Err(LangError::InvalidSyntax),
        }
    }
    #[inline(always)]
    /// Parse a create statement
    fn parse_create0(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Keyword(Keyword::Model)) => self.parse_create_model0(),
            Some(Token::Keyword(Keyword::Space)) => self.parse_create_space0(),
            Some(_) => Err(LangError::UnknownCreateQuery),
            None => Err(LangError::UnexpectedEOF),
        }
    }
    #[inline(always)]
    /// Parse a `create model` statement
    fn parse_create_model0(&mut self) -> LangResult<Statement> {
        let entity = self.parse_entity_name()?;
        self.parse_create_model1(entity)
    }
    #[inline(always)]
    /// Parse a field expression and return a `Statement::CreateModel`
    pub(super) fn parse_create_model1(&mut self, entity: Entity) -> LangResult<Statement> {
        let mut fc = FieldConfig::new();
        let mut is_good_expr = self.next_eq(&Token::OpenParen);
        while is_good_expr && self.peek_neq(&Token::CloseParen) {
            match self.next() {
                Some(Token::Identifier(field_name)) => {
                    // we have a field name
                    is_good_expr &= self.next_eq(&Token::Colon);
                    if let Some(Token::Keyword(Keyword::Type(ty))) = self.next() {
                        fc.names.push(field_name);
                        fc.types.push(self.parse_type_expression(ty)?);
                    } else {
                        is_good_expr = false;
                    }
                    is_good_expr &= self.peek_eq(&Token::CloseParen) || self.next_eq(&Token::Comma);
                }
                Some(Token::Keyword(Keyword::Type(ty))) => {
                    // we have a type name
                    fc.types.push(self.parse_type_expression(ty)?);
                    is_good_expr &= self.peek_eq(&Token::CloseParen) || self.next_eq(&Token::Comma);
                }
                _ => is_good_expr = false,
            }
        }
        is_good_expr &= self.next_eq(&Token::CloseParen);
        is_good_expr &= fc.types.len() >= 2;
        // important; we either have all unnamed fields or all named fields; having some unnamed
        // and some named is ambiguous because there's not "straightforward" way to query them
        // without introducing some funky naming conventions ($<field_number> if you don't have the
        // right name sounds like an outrageous idea)
        is_good_expr &= fc.names.is_empty() || fc.names.len() == fc.types.len();
        let volatile = self.next_eq(&Token::Keyword(Keyword::Volatile));
        if compiler::likely(is_good_expr) {
            Ok(Statement::CreateModel {
                entity,
                model: fc,
                volatile,
            })
        } else {
            Err(LangError::BadExpression)
        }
    }
    #[inline(always)]
    /// Parse a type expression return a `TypeExpression`
    fn parse_type_expression(&mut self, first_type: Type) -> LangResult<TypeExpression> {
        let mut expr = Vec::with_capacity(2);
        expr.push(first_type);

        // count of open and close brackets
        let mut p_open = 0;
        let mut p_close = 0;
        let mut valid_expr = true;

        // we already have the starting type; next is either nothing or open angular
        let mut has_more_args = self.peek_eq(&Token::OpenAngular);

        let mut expect = Expect::Type;
        while valid_expr && has_more_args && self.peek_neq(&Token::CloseParen) {
            match self.next() {
                Some(Token::OpenAngular) => p_open += 1,
                Some(Token::Keyword(Keyword::Type(ty))) if expect == Expect::Type => {
                    expr.push(ty);
                    let next = self.next();
                    let next_is_open = next == Some(Token::OpenAngular);
                    let next_is_close = next == Some(Token::CloseAngular);
                    p_open += next_is_open as usize;
                    p_close += next_is_close as usize;
                    expect = unsafe { transmute(next_is_close) };
                }
                Some(Token::CloseAngular) if expect == Expect::Close => {
                    p_close += 1;
                    expect = Expect::Close;
                }
                Some(Token::Comma) => {
                    unsafe { self.decr_cursor() }
                    has_more_args = false
                }
                _ => valid_expr = false,
            }
        }
        valid_expr &= p_open == p_close;
        if compiler::likely(valid_expr) {
            Ok(TypeExpression(expr))
        } else {
            Err(LangError::InvalidSyntax)
        }
    }
    #[inline(always)]
    /// Parse a `create space` statement
    fn parse_create_space0(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Identifier(model_name)) => Ok(Statement::CreateSpace(model_name)),
            Some(_) => Err(LangError::InvalidSyntax),
            None => Err(LangError::UnexpectedEOF),
        }
    }
    #[inline(always)]
    fn parse_entity_name_with_start(&mut self, start: RawSlice) -> LangResult<Entity> {
        if self.peek_eq(&Token::Period) {
            unsafe { self.incr_cursor() };
            Ok(Entity::Full(start, self.next_ident()?))
        } else {
            Ok(Entity::Current(start))
        }
    }
    #[inline(always)]
    pub(super) fn parse_entity_name(&mut self) -> LangResult<Entity> {
        // let's peek the next token
        match self.next_ident()? {
            id if self.peek_eq(&Token::Period)
                && compiler::likely(id.len() < Entity::MAX_LENGTH_EX) =>
            {
                unsafe { self.incr_cursor() };
                Ok(Entity::Full(id, self.next_ident()?))
            }
            id if compiler::likely(id.len() < Entity::MAX_LENGTH_EX) => Ok(Entity::Current(id)),
            _ => Err(LangError::InvalidSyntax),
        }
    }
}
