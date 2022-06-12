/*
 * Created on Thu Jun 09 2022
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

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

#[cfg(test)]
mod tests;
// endof tests
mod error;
mod lex;
// imports
use self::{error::LangError, lex::LexItem};

pub type LangResult<T> = Result<T, LangError>;

use {
    crate::util::Life,
    core::{marker::PhantomData, mem::discriminant, slice},
};

#[derive(Debug, Clone, Copy)]
pub struct Slice {
    start_ptr: *const u8,
    len: usize,
}

unsafe impl Send for Slice {}
unsafe impl Sync for Slice {}

impl Slice {
    /// ## Safety
    /// Ensure that `start_ptr` and `len` are valid during construction and use
    #[inline(always)]
    pub const unsafe fn new(start_ptr: *const u8, len: usize) -> Self {
        Slice { start_ptr, len }
    }
    /// ## Safety
    /// Ensure that the slice is valid in this context
    #[inline(always)]
    pub unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.start_ptr, self.len)
    }
}

impl<'a, T> From<T> for Slice
where
    T: AsRef<[u8]> + 'a,
{
    #[inline(always)]
    fn from(oth: T) -> Self {
        unsafe {
            let oth = oth.as_ref();
            Self::new(oth.as_ptr(), oth.len())
        }
    }
}

#[inline(always)]
fn find_ptr_distance(start: *const u8, stop: *const u8) -> usize {
    stop as usize - start as usize
}

pub struct Scanner<'a> {
    cursor: *const u8,
    end_ptr: *const u8,
    _lt: PhantomData<&'a [u8]>,
}

// init
impl<'a> Scanner<'a> {
    #[inline(always)]
    const fn new(buf: &[u8]) -> Self {
        unsafe {
            Self {
                cursor: buf.as_ptr(),
                end_ptr: buf.as_ptr().add(buf.len()),
                _lt: PhantomData {},
            }
        }
    }
}

// helpers
impl<'a> Scanner<'a> {
    #[inline(always)]
    pub fn exhausted(&self) -> bool {
        self.cursor >= self.end_ptr
    }
    #[inline(always)]
    pub fn not_exhausted(&self) -> bool {
        self.cursor < self.end_ptr
    }
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.add(by);
    }
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    unsafe fn deref_cursor(&self) -> u8 {
        *(self.cursor())
    }
    const fn cursor(&self) -> *const u8 {
        self.cursor
    }
    const fn end_ptr(&self) -> *const u8 {
        self.end_ptr
    }
    fn peek(&self) -> Option<u8> {
        if self.not_exhausted() {
            Some(unsafe { self.deref_cursor() })
        } else {
            None
        }
    }
    fn peek_eq(&self, eq_byte: u8) -> bool {
        unsafe { self.not_exhausted() && self.deref_cursor() == eq_byte }
    }
    fn peek_eq_and_forward(&mut self, eq_byte: u8) -> bool {
        let eq = self.peek_eq(eq_byte);
        unsafe {
            self.incr_cursor_by(eq as usize);
        }
        eq
    }
    unsafe fn deref_cursor_and_forward(&mut self) -> u8 {
        let ret = self.deref_cursor();
        self.incr_cursor();
        ret
    }
}

// parsing
impl<'a> Scanner<'a> {
    #[inline(always)]
    fn skip_char_if_present(&mut self, ch: u8) {
        self.cursor = unsafe {
            self.cursor
                .add((self.not_exhausted() && self.deref_cursor() == ch) as usize)
        };
    }
    #[inline(always)]
    fn skip_separator(&mut self) {
        self.skip_char_if_present(Self::SEPARATOR)
    }
    pub fn next<T: LexItem>(&mut self) -> LangResult<T> {
        T::lex(self)
    }
    const SEPARATOR: u8 = b' ';
    #[inline(always)]
    /// Returns the next token separated by the separator
    pub fn next_token_tl(&mut self) -> Slice {
        let start_ptr = self.cursor;
        let mut ptr = self.cursor;
        while self.end_ptr > ptr && unsafe { *ptr != Self::SEPARATOR } {
            ptr = unsafe {
                // UNSAFE(@ohsayan): The loop init invariant ensures this is safe
                ptr.add(1)
            };
        }
        // update the cursor
        self.cursor = ptr;
        self.skip_separator();
        unsafe {
            // UNSAFE(@ohsayan): The start_ptr and size were verified by the above steps
            Slice::new(start_ptr, find_ptr_distance(start_ptr, ptr))
        }
    }
    pub fn try_next_token(&mut self) -> LangResult<Slice> {
        if self.not_exhausted() {
            Ok(self.next_token_tl())
        } else {
            Err(LangError::UnexpectedEOF)
        }
    }
    pub fn parse_into_tokens(buf: &'a [u8]) -> Vec<Life<'a, Slice>> {
        let mut slf = Scanner::new(buf);
        let mut r = Vec::new();
        while slf.not_exhausted() {
            r.push(Life::new(slf.next_token_tl()));
        }
        r
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Token<'a> {
    Create,
    Drop,
    Model,
    Space,
    String,
    Binary,
    Ident(Life<'a, Slice>),
    Number(Life<'a, Slice>),
}

impl<'a> PartialEq for Token<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ident(ref id_a), Self::Ident(ref id_b)) => unsafe {
                id_a.as_slice() == id_b.as_slice()
            },
            (Self::Number(ref id_a), Self::Number(ref id_b)) => unsafe {
                id_a.as_slice() == id_b.as_slice()
            },
            (a, b) => discriminant(a) == discriminant(b),
        }
    }
}
