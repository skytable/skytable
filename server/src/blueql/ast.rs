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
    core::{marker::PhantomData, mem::transmute, ptr},
};

#[derive(Debug, PartialEq)]
#[repr(u8)]
/// A statement that can be executed
pub enum Statement {
    /// Create a new space with the provided ID
    CreateSpace(RawSlice),
    /// Create a new model with the provided configuration
    CreateModel { name: RawSlice, model: FieldConfig },
    /// Drop the given model
    DropModel(RawSlice),
    /// Drop the given space
    DropSpace(RawSlice),
}

#[derive(PartialEq, Debug)]
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
    /// Attempt to look ahead of the cursor
    fn peek(&self) -> Option<&Token> {
        if self.not_exhausted() {
            Some(unsafe { self.deref_cursor() })
        } else {
            None
        }
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
    /// Returns the remaining number of tokens
    fn remaining(&self) -> usize {
        self.end_ptr as usize - self.cursor as usize
    }
}

impl<'a> Compiler<'a> {
    #[inline(always)]
    /// Compile the given BlueQL source
    pub fn compile(src: &[u8]) -> LangResult<Statement> {
        let tokens = Lexer::lex(src)?;
        unsafe {
            Self {
                cursor: tokens.as_ptr(),
                end_ptr: tokens.as_ptr().add(tokens.len()),
                _lt: PhantomData,
            }
        }
        .eval()
    }
    #[inline(always)]
    /// The inner eval method
    fn eval(&mut self) -> LangResult<Statement> {
        let stmt = match self.next() {
            Some(tok) => match tok {
                Token::Keyword(Keyword::Create) => self.parse_create(),
                Token::Keyword(Keyword::Drop) => self.parse_drop(),
                _ => Err(LangError::ExpectedStatement),
            },
            None => Err(LangError::UnexpectedEOF),
        };
        if self.remaining() == 0 {
            stmt
        } else {
            Err(LangError::InvalidSyntax)
        }
    }
    /// Parse a drop statement
    fn parse_drop(&mut self) -> LangResult<Statement> {
        match (self.next(), self.next()) {
            (Some(Token::Keyword(Keyword::Model)), Some(Token::Identifier(model_name))) => {
                Ok(Statement::DropModel(model_name))
            }
            (Some(Token::Keyword(Keyword::Space)), Some(Token::Identifier(space_name))) => {
                Ok(Statement::DropSpace(space_name))
            }
            _ => Err(LangError::InvalidSyntax),
        }
    }
    #[inline(always)]
    /// Parse a create statement
    fn parse_create(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Keyword(Keyword::Model)) => self.parse_create_model(),
            Some(Token::Keyword(Keyword::Space)) => self.parse_create_space(),
            Some(_) => Err(LangError::UnknownCreateQuery),
            None => Err(LangError::UnexpectedEOF),
        }
    }
    #[inline(always)]
    /// Parse a `create model` statement
    fn parse_create_model(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Identifier(model_name)) => self.parse_fields(model_name),
            Some(_) => Err(LangError::InvalidSyntax),
            None => Err(LangError::UnexpectedEOF),
        }
    }
    #[inline(always)]
    /// Parse a field expression and return a `Statement::CreateTable`
    fn parse_fields(&mut self, name: RawSlice) -> LangResult<Statement> {
        let mut fc = FieldConfig::new();
        let mut is_good_expr = self.next() == Some(Token::OpenParen);
        while is_good_expr && self.peek() != Some(&Token::CloseParen) {
            match self.next() {
                Some(Token::Identifier(field_name)) => {
                    // we have a field name
                    is_good_expr &= self.next() == Some(Token::Colon);
                    if let Some(Token::Keyword(Keyword::Type(ty))) = self.next() {
                        fc.names.push(field_name);
                        fc.types.push(self.parse_type_expression(ty)?);
                    } else {
                        is_good_expr = false;
                    }
                    is_good_expr &= self.peek() == Some(&Token::CloseParen)
                        || self.next() == Some(Token::Comma);
                }
                Some(Token::Keyword(Keyword::Type(ty))) => {
                    // we have a type name
                    fc.names.push("unnamed".into());
                    fc.types.push(self.parse_type_expression(ty)?);
                    is_good_expr &= self.peek() == Some(&Token::CloseParen)
                        || self.next() == Some(Token::Comma);
                }
                _ => is_good_expr = false,
            }
        }
        is_good_expr &= self.next() == Some(Token::CloseParen);
        is_good_expr &= fc.names.len() >= 2;
        if is_good_expr {
            Ok(Statement::CreateModel { name, model: fc })
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
        let has_more_args = self.peek() == Some(&Token::OpenAngular);

        let mut expect = Expect::Type;
        while valid_expr && has_more_args && self.peek() != Some(&Token::CloseParen) {
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
                _ => valid_expr = false,
            }
        }
        valid_expr &= p_open == p_close;
        if valid_expr {
            Ok(TypeExpression(expr))
        } else {
            Err(LangError::InvalidSyntax)
        }
    }
    #[inline(always)]
    /// Parse a `create space` statement
    fn parse_create_space(&mut self) -> LangResult<Statement> {
        match self.next() {
            Some(Token::Identifier(model_name)) => Ok(Statement::CreateSpace(model_name)),
            Some(_) => Err(LangError::InvalidSyntax),
            None => Err(LangError::UnexpectedEOF),
        }
    }
}
