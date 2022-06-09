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

use {
    crate::util::Life,
    core::{marker::PhantomData, slice},
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
}

// parsing
impl<'a> Scanner<'a> {
    #[inline(always)]
    pub fn next_token(&mut self) -> Slice {
        let start_ptr = self.cursor;
        let mut ptr = self.cursor;
        while self.end_ptr > ptr && unsafe { *ptr != b' ' } {
            ptr = unsafe {
                // UNSAFE(@ohsayan): The loop init invariant ensures this is safe
                ptr.add(1)
            };
        }
        // update the cursor
        self.cursor = ptr;
        // if self is not exhausted and the cursor is a whitespace
        let ptr_is_whitespace = unsafe {
            // UNSAFE(@ohsayan): The first operand ensures safety
            self.not_exhausted() && *self.cursor == b' '
        };
        // if ptr is whitespace, then move the cursor ahead
        self.cursor = unsafe {
            // UNSAFE(@ohsayan): The definition of ptr_is_whitespace ensures correctness
            self.cursor.add(ptr_is_whitespace as usize)
        };
        unsafe {
            // UNSAFE(@ohsayan): The start_ptr and size were verified by the above steps
            Slice::new(start_ptr, find_ptr_distance(start_ptr, ptr))
        }
    }
    pub fn parse_into_tokens(buf: &'a [u8]) -> Vec<Life<'a, Slice>> {
        let mut slf = Scanner::new(buf);
        let mut r = Vec::new();
        while slf.not_exhausted() {
            r.push(Life::new(slf.next_token()));
        }
        r
    }
}

#[test]
fn scanner_tokenize() {
    let tokens = b"create space app".to_vec();
    let scanned_tokens = Scanner::parse_into_tokens(&tokens);
    let scanned_tokens: Vec<String> = scanned_tokens
        .into_iter()
        .map(|tok| unsafe { String::from_utf8_lossy(tok.as_slice()).to_string() })
        .collect();
    assert_eq!(scanned_tokens, ["create", "space", "app"]);
}

#[test]
fn scanner_step_by_step_tokenize() {
    let tokens = b"create space app".to_vec();
    let mut scanner = Scanner::new(&tokens);
    unsafe {
        assert_eq!(scanner.next_token().as_slice(), b"create");
        assert_eq!(scanner.next_token().as_slice(), b"space");
        assert_eq!(scanner.next_token().as_slice(), b"app");
        assert!(scanner.exhausted());
        assert_eq!(scanner.next_token().as_slice(), b"");
        assert_eq!(scanner.next_token().as_slice(), b"");
        assert_eq!(scanner.next_token().as_slice(), b"");
    }
    assert!(scanner.exhausted());
}
