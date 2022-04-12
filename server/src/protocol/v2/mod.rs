/*
 * Created on Tue Apr 12 2022
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

#![allow(unused)] // TODO(@ohsayan): Remove this once we're done

use crate::protocol::{ParseError, ParseResult, UnsafeSlice};
use core::marker::PhantomData;
#[cfg(test)]
mod tests;

pub struct Parser<'a> {
    end: *const u8,
    cursor: *const u8,
    _lt: PhantomData<&'a ()>,
}

impl<'a> Parser<'a> {
    /// Initialize a new parser
    pub fn new(slice: &[u8]) -> Self {
        unsafe {
            Self {
                end: slice.as_ptr().add(slice.len()),
                cursor: slice.as_ptr(),
                _lt: PhantomData,
            }
        }
    }
}

// basic methods
impl<'a> Parser<'a> {
    /// Returns a ptr one byte past the allocation of the buffer
    const fn data_end_ptr(&self) -> *const u8 {
        self.end
    }
    /// Returns the position of the cursor
    /// WARNING: Deref might led to a segfault
    const fn cursor_ptr(&self) -> *const u8 {
        self.cursor
    }
    /// Check how many bytes we have left
    fn remaining(&self) -> usize {
        self.data_end_ptr() as usize - self.cursor_ptr() as usize
    }
    /// Check if we have `size` bytes remaining
    fn has_remaining(&self, size: usize) -> bool {
        self.remaining() >= size
    }
    /// Check if we have exhausted the buffer
    fn exhausted(&self) -> bool {
        self.cursor_ptr() >= self.data_end_ptr()
    }
    /// Check if the buffer is not exhausted
    fn not_exhausted(&self) -> bool {
        self.cursor_ptr() < self.data_end_ptr()
    }
    /// Attempts to return the byte pointed at by the cursor.
    /// WARNING: The same segfault warning
    const unsafe fn get_byte_at_cursor(&self) -> u8 {
        *self.cursor_ptr()
    }
}

// mutable refs
impl<'a> Parser<'a> {
    /// Increment the cursor by `by` positions
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.add(by);
    }
    /// Increment the position of the cursor by one position
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1);
    }
}

// higher level abstractions
impl<'a> Parser<'a> {
    fn read_until(&mut self, len: usize) -> ParseResult<UnsafeSlice> {
        if self.has_remaining(len) {
            unsafe {
                // UNSAFE(@ohsayan): Already verified lengths
                let slice = UnsafeSlice::new(self.cursor_ptr(), len);
                self.incr_cursor_by(len);
                Ok(slice)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
}
