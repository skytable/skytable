/*
 * Created on Mon Sep 12 2022
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

macro_rules! boxed {
    ([] $ty:ty) => {
        ::std::boxed::Box::<[$ty]>
    };
}

/*
    Definitions
*/

use crate::util::compiler;
use std::{
    fmt,
    marker::PhantomData,
    mem::{self, transmute},
    ops::Deref,
    slice, str,
};

/// An unsafe, C-like slice that holds a ptr and length. Construction and usage is at the risk of the user
pub struct RawSlice {
    ptr: *const u8,
    len: usize,
}

impl RawSlice {
    const _EALIGN: () = assert!(mem::align_of::<Self>() == mem::align_of::<&[u8]>());
    const unsafe fn new(ptr: *const u8, len: usize) -> Self {
        Self { ptr, len }
    }
    unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr, self.len)
    }
}

#[cfg(debug_assertions)]
impl fmt::Debug for RawSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(unsafe {
                // UNSAFE(@ohsayan): Note, the caller is responsible for ensuring validity as long the
                // slice is used. also note, the Debug impl only exists for Debug builds so we never use
                // this in release builds
                self.as_slice()
            })
            .finish()
    }
}

#[cfg(debug_assertions)]
impl PartialEq for RawSlice {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): Callers must ensure validity during usage
            self.as_slice() == other.as_slice()
        }
    }
}

#[cfg(debug_assertions)]
impl<U> PartialEq<U> for RawSlice
where
    U: Deref<Target = [u8]>,
{
    fn eq(&self, other: &U) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): Callers must ensure validity during usage
            self.as_slice() == other.deref()
        }
    }
}

/*
    Lang errors
*/

type LangResult<T> = Result<T, LangError>;

#[derive(Debug, PartialEq)]
pub enum LangError {
    InvalidNumericLiteral,
    InvalidStringLiteral,
    UnexpectedChar,
    InvalidTypeExpression,
}

/*
    Lex meta
*/

#[derive(Debug)]
#[cfg_attr(debug_assertions, derive(PartialEq))]
pub enum Token {
    OpenParen,            // (
    CloseParen,           // )
    OpenAngular,          // <
    CloseAngular,         // >
    OpenSqBracket,        // [
    CloseSqBracket,       // ]
    Comma,                // ,
    Colon,                // :
    Period,               // .
    LitString(String),    // str lit
    Identifier(RawSlice), // ident
    LitNum(u64),          // num lit
    Keyword(Kw),          // kw
    OperatorEq,           // =
    OperatorAdd,          // +
}

impl From<Kw> for Token {
    fn from(kw: Kw) -> Self {
        Self::Keyword(kw)
    }
}

#[derive(Debug, PartialEq)]
pub enum Kw {
    Use,
    Create,
    Drop,
    Inspect,
    Alter,
    Space,
    Model,
    Force,
    Type(Ty),
}

impl From<Ty> for Kw {
    fn from(ty: Ty) -> Self {
        Self::Type(ty)
    }
}

impl Kw {
    // FIXME(@ohsayan): Use our pf hack
    pub fn try_from_slice(s: &[u8]) -> Option<Kw> {
        let r = match s.to_ascii_lowercase().as_slice() {
            b"use" => Self::Use,
            b"create" => Self::Create,
            b"drop" => Self::Drop,
            b"inspect" => Self::Inspect,
            b"alter" => Self::Alter,
            b"space" => Self::Space,
            b"model" => Self::Model,
            b"force" => Self::Force,
            b"string" => Self::Type(Ty::String),
            b"binary" => Self::Type(Ty::Binary),
            b"list" => Self::Type(Ty::Ls),
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

#[inline(always)]
fn dptr(s: *const u8, e: *const u8) -> usize {
    e as usize - s as usize
}

pub struct Lexer<'a> {
    c: *const u8,
    eptr: *const u8,
    last_error: Option<LangError>,
    tokens: Vec<Token>,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Lexer<'a> {
    pub const fn new(src: &'a [u8]) -> Self {
        unsafe {
            Self {
                c: src.as_ptr(),
                eptr: src.as_ptr().add(src.len()),
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
        self.eptr
    }
    #[inline(always)]
    fn not_exhausted(&self) -> bool {
        self.data_end_ptr() > self.cursor()
    }
    #[inline(always)]
    fn exhausted(&self) -> bool {
        self.cursor() == self.data_end_ptr()
    }
    unsafe fn deref_cursor(&self) -> u8 {
        *self.cursor()
    }
    #[inline(always)]
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.not_exhausted());
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
        self.not_exhausted() && unsafe { f(self.deref_cursor()) }
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
    unsafe fn check_escaped(&self, b: u8) -> bool {
        debug_assert!(self.not_exhausted());
        self.deref_cursor() == b'\\' && { self.not_exhausted() && self.deref_cursor() == b }
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
            RawSlice::new(s, dptr(s, self.cursor()))
        }
    }
    fn scan_ident_or_keyword(&mut self) {
        let s = self.scan_ident();
        match Kw::try_from_slice(unsafe { s.as_slice() }) {
            Some(kw) => self.tokens.push(kw.into()),
            None => self.tokens.push(Token::Identifier(s)),
        }
    }
    fn scan_number(&mut self) {
        let s = self.cursor();
        unsafe {
            while self.peek_is(|b| b.is_ascii_digit()) {
                self.incr_cursor();
            }
            let wseof = self.peek_eq_and_forward_or_eof(b' ');
            match str::from_utf8_unchecked(slice::from_raw_parts(s, dptr(s, self.cursor()))).parse()
            {
                Ok(num) if compiler::likely(wseof) => self.tokens.push(Token::LitNum(num)),
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
                let esc_backslash = self.check_escaped(b'\\');
                let esc_quote = self.check_escaped(quote_style);
                // mutually exclusive
                self.incr_cursor_if(esc_backslash | esc_quote);
                buf.push(self.deref_cursor());
                self.incr_cursor();
            }
            let eq = self.peek_eq_and_forward(quote_style);
            match String::from_utf8(buf) {
                Ok(st) if eq => self.tokens.push(Token::LitString(st)),
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
            b',' => Token::Comma,
            b'.' => Token::Period,
            b'=' => Token::OperatorEq,
            b'+' => Token::OperatorAdd,
            _ => {
                self.last_error = Some(LangError::UnexpectedChar);
                return;
            }
        };
        self.tokens.push(b)
    }

    fn _lex(&mut self) {
        while self.not_exhausted() && self.last_error.is_none() {
            match unsafe { self.deref_cursor() } {
                byte if byte.is_ascii_alphabetic() => self.scan_ident_or_keyword(),
                byte if byte.is_ascii_digit() => self.scan_number(),
                qs @ (b'\'' | b'"') => self.scan_quoted_string(qs),
                b' ' => self.trim_ahead(),
                b => self.scan_byte(b),
            }
        }
    }

    pub fn lex(src: &'a [u8]) -> LangResult<Vec<Token>> {
        let mut slf = Self::new(src);
        slf._lex();
        match slf.last_error {
            None => Ok(slf.tokens),
            Some(e) => Err(e),
        }
    }
}

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
