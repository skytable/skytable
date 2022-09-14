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

use core::slice;

use {
    super::{
        dptr,
        lexer::{Kw, Lexer, Stmt, Token, Ty},
        schema, LangError, LangResult, RawSlice,
    },
    crate::util::{compiler, Life},
    core::{marker::PhantomData, mem::transmute},
};

/*
    AST
*/

#[derive(Debug, PartialEq)]
pub struct TypeExpr(Vec<Ty>);

#[repr(u8)]
#[derive(Debug)]
enum FTy {
    String = 0,
    Binary = 1,
}

#[derive(Debug)]
struct TypeDefintion {
    d: usize,
    b: Ty,
    f: FTy,
}

impl TypeDefintion {
    const PRIM: usize = 1;
    pub fn eval(s: TypeExpr) -> LangResult<Self> {
        let TypeExpr(ex) = s;
        let l = ex.len();
        #[inline(always)]
        fn ls(t: &Ty) -> bool {
            *t == Ty::Ls
        }
        let d = ex.iter().map(|v| ls(v) as usize).sum::<usize>();
        let v = (l == 1 && ex[0] != Ty::Ls) || (l > 1 && (d == l - 1) && ex[l - 1] != Ty::Ls);
        if compiler::likely(v) {
            unsafe {
                Ok(Self {
                    d: d + 1,
                    b: ex[0],
                    f: transmute(ex[l - 1]),
                })
            }
        } else {
            compiler::cold_err(Err(LangError::InvalidTypeExpression))
        }
    }
    pub const fn is_prim(&self) -> bool {
        self.d == Self::PRIM
    }
}

pub enum Entity {
    Current(RawSlice),
    Partial(RawSlice),
    Full(RawSlice, RawSlice),
}

impl Entity {
    fn parse(cm: &mut Compiler) -> LangResult<Self> {
        let a = cm.nxtok_nofw_opt();
        let b = cm.nxtok_nofw_opt();
        let c = cm.nxtok_nofw_opt();
        match (a, b, c) {
            (Some(Token::Ident(ks)), Some(Token::Period), Some(Token::Ident(tbl))) => unsafe {
                let r = Ok(Entity::Full(ks.raw_clone(), tbl.raw_clone()));
                cm.incr_cursor_by(3);
                r
            },
            (Some(Token::Ident(ident)), _, _) => unsafe {
                let r = Ok(Entity::Current(ident.raw_clone()));
                cm.incr_cursor();
                r
            },
            (Some(Token::Colon), Some(Token::Ident(tbl)), _) => unsafe {
                let r = Ok(Entity::Partial(tbl.raw_clone()));
                cm.incr_cursor_by(2);
                r
            },
            _ => Err(LangError::UnexpectedToken),
        }
    }
}

pub enum Statement {}

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
            Some(Token::Keyword(Kw::Stmt(stmt))) => match stmt {
                Stmt::Create => self.create0(),
                Stmt::Drop => self.drop0(),
                Stmt::Alter => self.alter0(),
                Stmt::Inspect => self.inspect0(),
                Stmt::Use => self.use0(),
            },
            _ => Err(LangError::ExpectedStatement),
        }
    }
    #[inline(always)]
    fn create0(&mut self) -> Result<Statement, LangError> {
        match self.nxtok_opt() {
            Some(Token::Keyword(Kw::Model)) => self.c_model0(),
            Some(Token::Keyword(Kw::Space)) => self.c_space0(),
            _ => Err(LangError::UnexpectedEndofStatement),
        }
    }
    #[inline(always)]
    fn drop0(&mut self) -> Result<Statement, LangError> {
        todo!()
    }
    #[inline(always)]
    fn alter0(&mut self) -> Result<Statement, LangError> {
        todo!()
    }
    #[inline(always)]
    fn inspect0(&mut self) -> Result<Statement, LangError> {
        todo!()
    }
    #[inline(always)]
    fn use0(&mut self) -> Result<Statement, LangError> {
        todo!()
    }
    #[inline(always)]
    fn c_model0(&mut self) -> Result<Statement, LangError> {
        let model_name = match self.nxtok_opt() {
            Some(Token::Ident(model)) => unsafe { model.raw_clone() },
            _ => return Err(LangError::UnexpectedEndofStatement),
        };
        schema::parse_schema(self, model_name)
    }
    #[inline(always)]
    fn c_space0(&mut self) -> Result<Statement, LangError> {
        todo!()
    }
}

impl<'a> Compiler<'a> {
    #[inline(always)]
    pub(super) fn nxtok_opt(&mut self) -> Option<&Token> {
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
    pub(super) fn nxtok_nofw_opt(&self) -> Option<&Token> {
        if self.not_exhausted() {
            unsafe { Some(&*self.c) }
        } else {
            None
        }
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
        dptr(self.c, self.e)
    }
    pub(super) unsafe fn deref_cursor(&self) -> &Token {
        &*self.c
    }
    pub(super) fn peek_eq_and_forward(&mut self, t: &Token) -> bool {
        let did_fw = self.not_exhausted() && unsafe { self.deref_cursor() == t };
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
    #[inline(always)]
    pub(super) unsafe fn decr_cursor_by(&mut self, by: usize) {
        debug_assert!(
            self.remaining().checked_sub(by).is_some(),
            "cursor crossed e"
        );
        self.c = self.c.sub(by);
    }
}
