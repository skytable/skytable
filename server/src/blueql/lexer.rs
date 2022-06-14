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
        RawSlice,
    },
    core::{marker::PhantomData, slice, str},
};

#[derive(Debug, PartialEq)]
#[repr(u8)]
/// BQL tokens
pub enum Token {
    OpenParen,    // (
    CloseParen,   // )
    OpenAngular,  // <
    CloseAngular, // >
    Comma,        // ,
    Whitespace,   // ' '
    SingleQuote,  // '
    DoubleQuote,  // "
    Colon,        // :
    Identifier(RawSlice),
    Number(u64),
    Keyword(Keyword),
}

impl From<Keyword> for Token {
    fn from(kw: Keyword) -> Self {
        Self::Keyword(kw)
    }
}

impl From<RawSlice> for Token {
    fn from(sl: RawSlice) -> Self {
        Self::Identifier(sl)
    }
}

impl From<u64> for Token {
    fn from(num: u64) -> Self {
        Self::Number(num)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
/// BlueQL keywords
pub enum Keyword {
    Create,
    Drop,
    Model,
    Space,
    Type(Type),
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
/// BlueQL types
pub enum Type {
    String,
    Binary,
    List,
}

#[derive(Debug, PartialEq)]
/// Type expression (ty<ty<...>>)
pub struct TypeExpression(pub Vec<Type>);

impl Keyword {
    /// Attempt to parse a keyword from the given slice
    pub const fn try_from_slice(slice: &[u8]) -> Option<Self> {
        let r = match slice {
            b"create" => Keyword::Create,
            b"drop" => Keyword::Drop,
            b"model" => Keyword::Model,
            b"space" => Keyword::Space,
            b"string" => Keyword::Type(Type::String),
            b"binary" => Keyword::Type(Type::Binary),
            b"list" => Keyword::Type(Type::List),
            _ => return None,
        };
        Some(r)
    }
}

#[inline(always)]
/// Find the distance between two pointers
fn find_ptr_distance(start: *const u8, stop: *const u8) -> usize {
    stop as usize - start as usize
}

/// A `Lexer` for BlueQL tokens
pub struct Lexer<'a> {
    cursor: *const u8,
    end_ptr: *const u8,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    /// Create a new `Lexer`
    pub const fn new(buf: &'a [u8]) -> Self {
        unsafe {
            Self {
                cursor: buf.as_ptr(),
                end_ptr: buf.as_ptr().add(buf.len()),
                _lt: PhantomData,
            }
        }
    }
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    /// Returns the cursor
    const fn cursor(&self) -> *const u8 {
        self.cursor
    }
    #[inline(always)]
    /// Returns the end ptr
    const fn end_ptr(&self) -> *const u8 {
        self.end_ptr
    }
    #[inline(always)]
    /// Increments the cursor by 1
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    /// Increments the cursor by 1 if `cond` is true
    #[inline(always)]
    unsafe fn incr_cursor_if(&mut self, cond: bool) {
        self.incr_cursor_by(cond as usize)
    }
    #[inline(always)]
    /// Increments the cursor by `by` positions
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.add(by)
    }
    #[inline(always)]
    /// Derefs the cursor
    unsafe fn deref_cursor(&self) -> u8 {
        *self.cursor()
    }
    #[inline(always)]
    /// Checks if we have reached EOA
    fn not_exhausted(&self) -> bool {
        self.cursor() < self.end_ptr()
    }
    #[inline(always)]
    /// Returns true if we have reached EOA
    fn exhausted(&self) -> bool {
        self.cursor() >= self.end_ptr()
    }
    #[inline(always)]
    /// Check if the peeked value matches the predicate. Returns false if EOA
    fn peek_is(&self, predicate: impl Fn(u8) -> bool) -> bool {
        self.not_exhausted() && unsafe { predicate(self.deref_cursor()) }
    }
    #[inline(always)]
    /// Check if the byte ahead is equal to the provided byte. Returns false
    /// if reached EOA
    fn peek_eq(&self, eq: u8) -> bool {
        self.not_exhausted() && unsafe { self.deref_cursor() == eq }
    }
    #[inline(always)]
    /// Same as `peek_eq`, but forwards the cursor if the byte is matched
    fn peek_eq_and_forward(&mut self, eq: u8) -> bool {
        let did_peek = self.peek_eq(eq);
        unsafe { self.incr_cursor_if(did_peek) };
        did_peek
    }
    #[inline(always)]
    /// Same as `peek_eq_or_eof` but forwards the cursor on match
    fn peek_eq_or_eof_and_forward(&mut self, eq: u8) -> bool {
        let did_forward = self.peek_eq_and_forward(eq);
        unsafe { self.incr_cursor_if(did_forward) };
        did_forward | self.exhausted()
    }
    #[inline(always)]
    /// Trim the whitespace ahead
    fn trim_ahead(&mut self) {
        while self.peek_eq_and_forward(b' ') {}
    }
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    /// Attempt to scan a number
    fn scan_number(&mut self) -> LangResult<u64> {
        let start = self.cursor();
        while self.peek_is(|byte| byte.is_ascii_digit()) {
            unsafe { self.incr_cursor() }
        }
        let slice = unsafe {
            str::from_utf8_unchecked(slice::from_raw_parts(
                start,
                find_ptr_distance(start, self.cursor()),
            ))
        };
        slice.parse().map_err(|_| LangError::InvalidNumericLiteral)
    }
    #[inline(always)]
    /// Attempt to scan an ident
    fn scan_ident(&mut self) -> RawSlice {
        let start = self.cursor();
        while self.peek_is(|byte| (byte.is_ascii_alphanumeric() || byte == b'_')) {
            unsafe { self.incr_cursor() }
        }
        let len = find_ptr_distance(start, self.cursor());
        unsafe { RawSlice::new(start, len) }
    }
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    /// Lex the input stream into tokens
    pub fn lex(src: &'a [u8]) -> LangResult<Vec<Token>> {
        let mut slf = Self::new(src);
        slf._lex()
    }
    #[inline(always)]
    /// The inner lex method
    fn _lex(&mut self) -> LangResult<Vec<Token>> {
        let mut is_okay = true;
        let mut tokens = Vec::new();
        while self.not_exhausted() && is_okay {
            match unsafe { self.deref_cursor() } {
                byte if byte.is_ascii_alphabetic() => {
                    let id = self.scan_ident();
                    match Keyword::try_from_slice(id.as_slice()) {
                        Some(kw) => tokens.push(kw.into()),
                        None => tokens.push(id.into()),
                    }
                }
                byte if byte.is_ascii_digit() => match self.scan_number() {
                    Ok(num) => {
                        // a number has to end with a whitespace
                        is_okay &= self.peek_eq_or_eof_and_forward(b' ');
                        tokens.push(num.into())
                    }
                    Err(e) => return Err(e),
                },
                b' ' => self.trim_ahead(),
                byte => {
                    let r = match byte {
                        b'<' => Token::OpenAngular,
                        b'>' => Token::CloseAngular,
                        b'(' => Token::OpenParen,
                        b')' => Token::CloseParen,
                        b',' => Token::Comma,
                        b':' => Token::Colon,
                        _ => return Err(LangError::InvalidSyntax),
                    };
                    tokens.push(r);
                    unsafe { self.incr_cursor() }
                }
            }
        }
        if is_okay {
            Ok(tokens)
        } else {
            Err(LangError::InvalidSyntax)
        }
    }
}
