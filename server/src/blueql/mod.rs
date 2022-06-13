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
mod ast;
mod error;
mod lex;
// imports
use self::{error::LangError, lex::LexItem};

pub type LangResult<T> = Result<T, LangError>;

use {
    crate::util::Life,
    core::{marker::PhantomData, slice},
};

#[derive(Debug, Clone, Copy)]
/// A raw slice that resembles the same structure as a fat ptr
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
/// Finds the distance between two pointers. Panics if the stop ptr is behind the start ptr
fn find_ptr_distance(start: *const u8, stop: *const u8) -> usize {
    stop as usize - start as usize
}

/// A `QueryProcessor` provides functions to parse queries
pub struct QueryProcessor<'a> {
    cursor: *const u8,
    end_ptr: *const u8,
    _lt: PhantomData<&'a [u8]>,
}

// init
impl<'a> QueryProcessor<'a> {
    #[inline(always)]
    /// Init a new query processor
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
impl<'a> QueryProcessor<'a> {
    #[inline(always)]
    /// Check if we have exhausted the buffer
    pub fn exhausted(&self) -> bool {
        self.cursor >= self.end_ptr
    }
    #[inline(always)]
    /// Check if we still have something left in the buffer
    pub fn not_exhausted(&self) -> bool {
        self.cursor < self.end_ptr
    }
    /// Move the cursor ahead by `by` positions
    #[inline(always)]
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.add(by);
    }
    /// Move the cursor ahead by 1
    #[inline(always)]
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    /// Deref the cursor
    #[inline(always)]
    unsafe fn deref_cursor(&self) -> u8 {
        *(self.cursor())
    }
    /// Returns the cursor
    #[inline(always)]
    const fn cursor(&self) -> *const u8 {
        self.cursor
    }
    /// Returns the EOA ptr
    #[inline(always)]
    const fn end_ptr(&self) -> *const u8 {
        self.end_ptr
    }
    /// Peeks at the byte ahead if it exists
    #[inline(always)]
    fn peek(&self) -> Option<u8> {
        if self.not_exhausted() {
            Some(unsafe { self.deref_cursor() })
        } else {
            None
        }
    }
    /// Peeks at the byte ahead to see if it matches the given byte. Returns false if
    /// we've reached end of allocation
    #[inline(always)]
    fn peek_eq(&self, eq_byte: u8) -> bool {
        unsafe { self.not_exhausted() && self.deref_cursor() == eq_byte }
    }
    /// Same as `Self::peek_eq`, but forwards the cursor on match
    #[inline(always)]
    fn peek_eq_and_forward(&mut self, eq_byte: u8) -> bool {
        let eq = self.peek_eq(eq_byte);
        unsafe {
            self.incr_cursor_by(eq as usize);
        }
        eq
    }
    /// Returns the byte at cursor and moves it ahead
    #[inline(always)]
    unsafe fn deref_cursor_and_forward(&mut self) -> u8 {
        let ret = self.deref_cursor();
        self.incr_cursor();
        ret
    }
    /// Returns true if:
    /// - The byte ahead matches the provided `byte`
    /// - If we have reached end of allocation
    ///
    /// Meant to be used in places where you want to either match a predicate, but return
    /// true if you've reached EOF
    #[inline(always)]
    fn peek_eq_and_forward_or_true(&mut self, byte: u8) -> bool {
        self.peek_eq(byte) | self.exhausted()
    }
    #[inline(always)]
    /// Peeks ahead and moves the cursor ahead if the peeked byte matches the predicate
    fn skip_char_if_present(&mut self, ch: u8) {
        unsafe { self.incr_cursor_by(self.peek_eq(ch) as usize) }
    }
    #[inline(always)]
    /// Skips the delimiter
    fn skip_delimiter(&mut self) {
        self.skip_char_if_present(Self::DELIMITER)
    }
}

// parsing
impl<'a> QueryProcessor<'a> {
    const DELIMITER: u8 = b' ';

    #[inline(always)]
    pub fn next<T: LexItem>(&mut self) -> LangResult<T> {
        T::lex(self)
    }
    #[inline(always)]
    /// Returns the next token separated by the DELIMITER
    pub fn next_token_tl(&mut self) -> Slice {
        let start_ptr = self.cursor;
        let mut ptr = self.cursor;
        while self.end_ptr > ptr && unsafe { *ptr != Self::DELIMITER } {
            ptr = unsafe {
                // UNSAFE(@ohsayan): The loop init invariant ensures this is safe
                ptr.add(1)
            };
        }
        // update the cursor
        self.cursor = ptr;
        self.skip_delimiter();
        unsafe {
            // UNSAFE(@ohsayan): The start_ptr and size were verified by the above steps
            Slice::new(start_ptr, find_ptr_distance(start_ptr, ptr))
        }
    }
    #[inline(always)]
    pub fn try_next_token(&mut self) -> LangResult<Slice> {
        if self.not_exhausted() {
            Ok(self.next_token_tl())
        } else {
            Err(LangError::UnexpectedEOF)
        }
    }
    #[inline(always)]
    pub fn parse_into_tokens(buf: &'a [u8]) -> Vec<Life<'a, Slice>> {
        let mut slf = QueryProcessor::new(buf);
        let mut r = Vec::new();
        while slf.not_exhausted() {
            r.push(Life::new(slf.next_token_tl()));
        }
        r
    }
}
