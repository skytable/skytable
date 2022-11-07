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
        lexer::{Lexer, Token},
        schema, LangError, LangResult, RawSlice,
    },
    crate::util::Life,
    core::{marker::PhantomData, slice},
};

/*
    AST
*/

#[derive(Debug, PartialEq)]
pub enum Entity {
    Current(RawSlice),
    Partial(RawSlice),
    Full(RawSlice, RawSlice),
}

impl Entity {
    pub(super) fn parse(cm: &mut Compiler) -> LangResult<Self> {
        let sl = cm.remslice();
        let is_partial = sl.len() > 1 && sl[0] == Token![:] && sl[1].is_ident();
        let is_current = !sl.is_empty() && sl[0].is_ident();
        let is_full = sl.len() > 2 && sl[0].is_ident() && sl[1] == Token![.] && sl[2].is_ident();
        let c;
        let r = match () {
            _ if is_full => unsafe {
                c = 3;
                Entity::Full(
                    extract!(&sl[0], Token::Ident(sl) => sl.clone()),
                    extract!(&sl[2], Token::Ident(sl) => sl.clone()),
                )
            },
            _ if is_current => unsafe {
                c = 1;
                Entity::Current(extract!(&sl[0], Token::Ident(sl) => sl.clone()))
            },
            _ if is_partial => unsafe {
                c = 2;
                Entity::Partial(extract!(&sl[1], Token::Ident(sl) => sl.clone()))
            },
            _ => return Err(LangError::UnexpectedToken),
        };
        unsafe {
            cm.incr_cursor_by(c);
        }
        Ok(r)
    }
}

#[cfg_attr(debug_assertions, derive(Debug, PartialEq))]
pub enum Statement {
    CreateModel(schema::Model),
    CreateSpace(schema::Space),
    Use(Entity),
    AlterSpace(schema::AlterSpace),
    AlterModel(RawSlice, schema::AlterKind),
    DropModel(RawSlice, bool),
    DropSpace(RawSlice, bool),
    InspectSpace(RawSlice),
    InspectModel(Entity),
    InspectSpaces,
}

pub struct Compiler<'a> {
    c: *const Token,
    e: *const Token,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Compiler<'a> {
    pub fn compile(src: &'a [u8]) -> LangResult<Life<'a, Statement>> {
        let token_stream = Lexer::lex(src)?;
        Self::new(&token_stream).compile_link_lt()
    }
    #[inline(always)]
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
    fn compile_link_lt(mut self) -> LangResult<Life<'a, Statement>> {
        match self.stage0() {
            Ok(t) if self.exhausted() => Ok(Life::new(t)),
            Err(e) => Err(e),
            _ => Err(LangError::UnexpectedToken),
        }
    }
    #[inline(always)]
    fn stage0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt() {
            Some(Token![create]) => self.create0(),
            Some(Token![drop]) => self.drop0(),
            Some(Token![alter]) => self.alter0(),
            Some(Token![inspect]) => self.inspect0(),
            Some(Token![use]) => self.use0(),
            _ => Err(LangError::ExpectedStatement),
        }
    }
    #[inline(always)]
    fn create0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt() {
            Some(Token![model]) => self.c_model0(),
            Some(Token![space]) => self.c_space0(),
            _ => Err(LangError::UnexpectedEndofStatement),
        }
    }
    #[inline(always)]
    fn drop0(&mut self) -> Result<Statement, LangError> {
        if self.remaining() < 2 {
            return Err(LangError::ExpectedStatement);
        }
        let rs = self.remslice();
        let ident = match rs[1] {
            Token::Ident(ref id) => id,
            _ => return Err(LangError::ExpectedStatement),
        };
        let should_force = self.remaining() > 2 && rs[2].as_ident_eq_ignore_case(b"force");
        let r = match rs[0] {
            Token![model] => {
                // dropping a model
                Ok(Statement::DropModel(ident.clone(), should_force))
            }
            Token![space] => {
                // dropping a space
                Ok(Statement::DropSpace(ident.clone(), should_force))
            }
            _ => Err(LangError::UnexpectedToken),
        };
        unsafe {
            self.incr_cursor_by(2);
            self.incr_cursor_if(should_force);
        }
        r
    }
    #[inline(always)]
    fn alter0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt() {
            Some(Token![model]) => self.alter_model(),
            Some(Token![space]) => self.alter_space(),
            Some(_) => Err(LangError::ExpectedStatement),
            None => Err(LangError::UnexpectedEndofStatement),
        }
    }
    #[inline(always)]
    fn alter_model(&mut self) -> Result<Statement, LangError> {
        let model_name = match self.nxtok_opt() {
            Some(Token::Ident(md)) => md.clone(),
            _ => return Err(LangError::ExpectedStatement),
        };
        let mut c = 0;
        schema::parse_alter_kind_from_tokens(self.remslice(), &mut c)
            .map(|ak| Statement::AlterModel(model_name.clone(), ak))
    }
    #[inline(always)]
    fn alter_space(&mut self) -> Result<Statement, LangError> {
        let space_name = match self.nxtok_opt() {
            Some(Token::Ident(id)) => id.clone(),
            Some(_) => return Err(LangError::UnexpectedToken),
            None => return Err(LangError::UnexpectedEndofStatement),
        };
        let (alter, i) = schema::parse_alter_space_from_tokens(self.remslice(), space_name)?;
        unsafe {
            self.incr_cursor_by(i);
        }
        Ok(Statement::AlterSpace(alter))
    }
    #[inline(always)]
    fn inspect0(&mut self) -> Result<Statement, LangError> {
        if self.remaining() == 0 {
            return Err(LangError::UnexpectedEndofStatement);
        }
        match self.nxtok_opt() {
            Some(Token![space]) => {
                let space_name = match self.nxtok_opt() {
                    Some(Token::Ident(id)) => id.clone(),
                    _ => return Err(LangError::UnexpectedToken),
                };
                Ok(Statement::InspectSpace(space_name))
            }
            Some(Token![model]) => {
                let entity = Entity::parse(self)?;
                Ok(Statement::InspectModel(entity))
            }
            Some(Token::Ident(id))
                if unsafe { id.as_slice() }.eq_ignore_ascii_case(b"keyspaces") =>
            {
                Ok(Statement::InspectSpaces)
            }
            _ => Err(LangError::ExpectedStatement),
        }
    }
    #[inline(always)]
    fn use0(&mut self) -> Result<Statement, LangError> {
        let entity = Entity::parse(self)?;
        Ok(Statement::Use(entity))
    }
    #[inline(always)]
    fn c_model0(&mut self) -> Result<Statement, LangError> {
        let model_name = match self.nxtok_opt() {
            Some(Token::Ident(model)) => model.clone(),
            _ => return Err(LangError::UnexpectedToken),
        };
        let (model, i) = schema::parse_schema_from_tokens(self.remslice(), model_name)?;
        unsafe {
            self.incr_cursor_by(i);
        }
        Ok(Statement::CreateModel(model))
    }
    #[inline(always)]
    fn c_space0(&mut self) -> Result<Statement, LangError> {
        let space_name = match self.nxtok_opt() {
            Some(Token::Ident(space_name)) => space_name.clone(),
            _ => return Err(LangError::UnexpectedToken),
        };
        let (space, i) = schema::parse_space_from_tokens(self.remslice(), space_name)?;
        unsafe {
            self.incr_cursor_by(i);
        }
        Ok(Statement::CreateSpace(space))
    }
}

impl<'a> Compiler<'a> {
    #[inline(always)]
    pub(super) fn nxtok_opt<'b>(&mut self) -> Option<&'b Token>
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
    pub(super) const fn cursor(&self) -> *const Token {
        self.c
    }
    #[inline(always)]
    pub(super) fn remslice(&'a self) -> &'a [Token] {
        unsafe { slice::from_raw_parts(self.c, self.remaining()) }
    }
    #[inline(always)]
    pub(super) fn not_exhausted(&self) -> bool {
        self.c != self.e
    }
    #[inline(always)]
    pub(super) fn exhausted(&self) -> bool {
        self.c == self.e
    }
    #[inline(always)]
    pub(super) fn remaining(&self) -> usize {
        unsafe { self.e.offset_from(self.c) as usize }
    }
    pub(super) unsafe fn deref_cursor(&self) -> &Token {
        &*self.c
    }
    pub(super) fn peek_eq_and_forward(&mut self, t: Token) -> bool {
        let did_fw = self.not_exhausted() && unsafe { self.deref_cursor() == &t };
        unsafe {
            self.incr_cursor_if(did_fw);
        }
        did_fw
    }
    #[inline(always)]
    pub(super) unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    pub(super) unsafe fn incr_cursor_if(&mut self, did_fw: bool) {
        self.incr_cursor_by(did_fw as _)
    }
    #[inline(always)]
    pub(super) unsafe fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.remaining() >= by);
        self.c = self.c.add(by);
    }
}
