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

use crate::util::Life;

use {
    super::{LangError, LangResult, RawSlice},
    crate::util::compiler,
    core::{marker::PhantomData, slice, str},
};

/*
    Lex meta
*/

#[derive(Debug)]
#[cfg_attr(debug_assertions, derive(PartialEq, Clone))]
#[repr(u8)]
pub enum Token {
    OpenParen,      // (
    CloseParen,     // )
    OpenAngular,    // <
    CloseAngular,   // >
    OpenSqBracket,  // [
    CloseSqBracket, // ]
    OpenBrace,      // {
    CloseBrace,     // }
    Comma,          // ,
    #[cfg(test)]
    /// A comma that can be ignored (used for fuzzing)
    IgnorableComma,
    Colon,           // :
    Period,          // .
    Ident(RawSlice), // ident
    Keyword(Kw),     // kw
    OperatorEq,      // =
    OperatorAdd,     // +
    Lit(Lit),        // literal
}

impl From<Kw> for Token {
    fn from(kw: Kw) -> Self {
        Self::Keyword(kw)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Lit {
    Str(String),
    Bool(bool),
    Num(u64),
}

impl From<Lit> for Token {
    fn from(l: Lit) -> Self {
        Self::Lit(l)
    }
}

impl From<u64> for Lit {
    fn from(n: u64) -> Self {
        Self::Num(n)
    }
}

impl From<String> for Lit {
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

impl From<bool> for Lit {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Stmt {
    Use,
    Create,
    Drop,
    Inspect,
    Alter,
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Kw {
    Stmt(Stmt),
    TypeId(Ty),
    Space,
    Model,
    Type,
}

impl From<Ty> for Kw {
    fn from(ty: Ty) -> Self {
        Self::TypeId(ty)
    }
}

impl Kw {
    // FIXME(@ohsayan): Use our pf hack
    pub fn try_from_slice(s: &[u8]) -> Option<Token> {
        let r = match s.to_ascii_lowercase().as_slice() {
            b"use" => Self::Stmt(Stmt::Use).into(),
            b"create" => Self::Stmt(Stmt::Create).into(),
            b"drop" => Self::Stmt(Stmt::Drop).into(),
            b"inspect" => Self::Stmt(Stmt::Inspect).into(),
            b"alter" => Self::Stmt(Stmt::Alter).into(),
            b"space" => Self::Space.into(),
            b"model" => Self::Model.into(),
            b"string" => Self::TypeId(Ty::String).into(),
            b"binary" => Self::TypeId(Ty::Binary).into(),
            b"list" => Self::TypeId(Ty::Ls).into(),
            b"true" => Token::Lit(Lit::Bool(true)),
            b"type" => Kw::Type.into(),
            b"false" => Token::Lit(Lit::Bool(false)),
            _ => return None,
        };
        return Some(r);
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Ty {
    String = 0_u8,
    Binary = 1_u8,
    Ls = 2_u8,
}

/*
    Lexer impl
*/

pub struct Lexer<'a> {
    c: *const u8,
    e: *const u8,
    last_error: Option<LangError>,
    tokens: Vec<Token>,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Lexer<'a> {
    pub const fn new(src: &'a [u8]) -> Self {
        unsafe {
            Self {
                c: src.as_ptr(),
                e: src.as_ptr().add(src.len()),
                last_error: None,
                tokens: Vec::new(),
                _lt: PhantomData,
            }
        }
    }
}

// meta
impl<'a> Lexer<'a> {
    #[inline(always)]
    const fn cursor(&self) -> *const u8 {
        self.c
    }
    #[inline(always)]
    const fn data_end_ptr(&self) -> *const u8 {
        self.e
    }
    #[inline(always)]
    fn not_exhausted(&self) -> bool {
        self.data_end_ptr() > self.cursor()
    }
    #[inline(always)]
    fn exhausted(&self) -> bool {
        self.cursor() == self.data_end_ptr()
    }
    #[inline(always)]
    fn remaining(&self) -> usize {
        unsafe { self.e.offset_from(self.c) as usize }
    }
    unsafe fn deref_cursor(&self) -> u8 {
        *self.cursor()
    }
    #[inline(always)]
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.remaining() >= by);
        self.c = self.cursor().add(by)
    }
    #[inline(always)]
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    #[inline(always)]
    unsafe fn incr_cursor_if(&mut self, iff: bool) {
        self.incr_cursor_by(iff as usize)
    }
    #[inline(always)]
    fn push_token(&mut self, token: Token) {
        self.tokens.push(token)
    }
    #[inline(always)]
    fn peek_is(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
        self.not_exhausted() && unsafe { f(self.deref_cursor()) }
    }
    #[inline(always)]
    fn peek_is_and_forward(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
        let did_fw = self.not_exhausted() && unsafe { f(self.deref_cursor()) };
        unsafe {
            self.incr_cursor_if(did_fw);
        }
        did_fw
    }
    #[inline(always)]
    fn peek_eq_and_forward_or_eof(&mut self, eq: u8) -> bool {
        unsafe {
            let eq = self.not_exhausted() && self.deref_cursor() == eq;
            self.incr_cursor_if(eq);
            eq | self.exhausted()
        }
    }
    #[inline(always)]
    fn peek_neq(&self, b: u8) -> bool {
        self.not_exhausted() && unsafe { self.deref_cursor() != b }
    }
    #[inline(always)]
    fn peek_eq_and_forward(&mut self, b: u8) -> bool {
        unsafe {
            let r = self.not_exhausted() && self.deref_cursor() == b;
            self.incr_cursor_if(r);
            r
        }
    }
    #[inline(always)]
    fn trim_ahead(&mut self) {
        while self.peek_is_and_forward(|b| b == b' ' || b == b'\t' || b == b'\n') {}
    }
}

impl<'a> Lexer<'a> {
    fn scan_ident(&mut self) -> RawSlice {
        let s = self.cursor();
        unsafe {
            while self.peek_is(|b| b.is_ascii_alphanumeric() || b == b'_') {
                self.incr_cursor();
            }
            RawSlice::new(s, self.cursor().offset_from(s) as usize)
        }
    }
    fn scan_ident_or_keyword(&mut self) {
        let s = self.scan_ident();
        match Kw::try_from_slice(unsafe { s.as_slice() }) {
            Some(kw) => self.tokens.push(kw),
            None => self.tokens.push(Token::Ident(s)),
        }
    }
    fn scan_number(&mut self) {
        let s = self.cursor();
        unsafe {
            while self.peek_is(|b| b.is_ascii_digit()) {
                self.incr_cursor();
            }
            /*
                1234; // valid
                1234} // valid
                1234{ // invalid
                1234, // valid
                1234a // invalid
            */
            static TERMINAL_CHAR: [u8; 6] = [b';', b'}', b',', b' ', b'\n', b'\t'];
            let wseof = self.peek_is(|b| TERMINAL_CHAR.contains(&b)) || self.exhausted();
            match str::from_utf8_unchecked(slice::from_raw_parts(
                s,
                self.cursor().offset_from(s) as usize,
            ))
            .parse()
            {
                Ok(num) if compiler::likely(wseof) => self.tokens.push(Token::Lit(Lit::Num(num))),
                _ => self.last_error = Some(LangError::InvalidNumericLiteral),
            }
        }
    }
    fn scan_quoted_string(&mut self, quote_style: u8) {
        debug_assert!(
            unsafe { self.deref_cursor() } == quote_style,
            "illegal call to scan_quoted_string"
        );
        unsafe { self.incr_cursor() }
        let mut buf = Vec::new();
        unsafe {
            while self.peek_neq(quote_style) {
                match self.deref_cursor() {
                    b if b != b'\\' => {
                        buf.push(b);
                    }
                    _ => {
                        self.incr_cursor();
                        if self.exhausted() {
                            break;
                        }
                        let b = self.deref_cursor();
                        let quote = b == quote_style;
                        let bs = b == b'\\';
                        if quote | bs {
                            buf.push(b);
                        } else {
                            break; // what on good earth is that escape?
                        }
                    }
                }
                self.incr_cursor();
            }
            let terminated = self.peek_eq_and_forward(quote_style);
            match String::from_utf8(buf) {
                Ok(st) if terminated => self.tokens.push(Token::Lit(st.into())),
                _ => self.last_error = Some(LangError::InvalidStringLiteral),
            }
        }
    }
    fn scan_byte(&mut self, byte: u8) {
        let b = match byte {
            b':' => Token::Colon,
            b'(' => Token::OpenParen,
            b')' => Token::CloseParen,
            b'<' => Token::OpenAngular,
            b'>' => Token::CloseAngular,
            b'[' => Token::OpenSqBracket,
            b']' => Token::CloseSqBracket,
            b'{' => Token::OpenBrace,
            b'}' => Token::CloseBrace,
            b',' => Token::Comma,
            b'.' => Token::Period,
            b'=' => Token::OperatorEq,
            b'+' => Token::OperatorAdd,
            #[cfg(test)]
            b'\r' => Token::IgnorableComma,
            _ => {
                self.last_error = Some(LangError::UnexpectedChar);
                return;
            }
        };
        unsafe {
            self.incr_cursor();
        }
        self.tokens.push(b)
    }

    fn _lex(&mut self) {
        while self.not_exhausted() && self.last_error.is_none() {
            match unsafe { self.deref_cursor() } {
                byte if byte.is_ascii_alphabetic() => self.scan_ident_or_keyword(),
                byte if byte.is_ascii_digit() => self.scan_number(),
                qs @ (b'\'' | b'"') => self.scan_quoted_string(qs),
                b' ' | b'\n' | b'\t' => self.trim_ahead(),
                b => self.scan_byte(b),
            }
        }
    }

    pub fn lex(src: &'a [u8]) -> LangResult<Life<'a, Vec<Token>>> {
        let mut slf = Self::new(src);
        slf._lex();
        match slf.last_error {
            None => Ok(Life::new(slf.tokens)),
            Some(e) => Err(e),
        }
    }
}
