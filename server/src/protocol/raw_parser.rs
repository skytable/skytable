/*
 * Created on Tue May 03 2022
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
    super::{ParseError, ParseResult, UnsafeSlice},
    core::mem::transmute,
};

/*
NOTE TO SELF (@ohsayan): The reason we split this into three traits is because:
- `RawParser` is the only one that is to be implemented. Just provide information about the cursor
- `RawParserMeta` provides information about the buffer based on cursor and end ptr information
- `RawParserExt` provides high-level abstractions over `RawParserMeta`. It is like the "super trait"

These distinctions reduce the likelihood of "accidentally incorrect impls" (we could've easily included
`RawParserMeta` inside `RawParser`).

-- Sayan (May, 2022)
*/

/// The `RawParser` trait has three methods that implementors must define:
///
/// - `cursor_ptr` -> Should point to the current position in the buffer for the parser
/// - `cursor_ptr_mut` -> a mutable reference to the cursor
/// - `data_end_ptr` -> a ptr to one byte past the allocated area of the buffer
///
/// All implementors of `RawParser` get a free implementation for `RawParserMeta` and `RawParserExt`
///
/// # Safety
/// - `cursor_ptr` must point to a valid location in memory
/// - `data_end_ptr` must point to a valid location in memory, in the **same allocated area**
pub(super) unsafe trait RawParser {
    fn cursor_ptr(&self) -> *const u8;
    fn cursor_ptr_mut(&mut self) -> &mut *const u8;
    fn data_end_ptr(&self) -> *const u8;
}

/// The `RawParserMeta` trait builds on top of the `RawParser` trait to provide low-level interactions
/// and information with the parser's buffer. It is implemented for any type that implements the `RawParser`
/// trait. Manual implementation is discouraged
pub(super) trait RawParserMeta: RawParser {
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
    unsafe fn get_byte_at_cursor(&self) -> u8 {
        *self.cursor_ptr()
    }
    /// Increment the cursor by `by` positions
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        let current = *self.cursor_ptr_mut();
        *self.cursor_ptr_mut() = current.add(by);
    }
    /// Increment the position of the cursor by one position
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1);
    }
}

impl<T> RawParserMeta for T where T: RawParser {}

/// `RawParserExt` builds on the `RawParser` and `RawParserMeta` traits to provide high level abstractions
/// like reading lines, or a slice of a given length. It is implemented for any type that
/// implements the `RawParser` trait. Manual implementation is discouraged
pub(super) trait RawParserExt: RawParser + RawParserMeta {
    /// Attempt to read `len` bytes
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
    #[cfg(test)]
    /// Attempt to read a byte slice terminated by an LF
    fn read_line(&mut self) -> ParseResult<UnsafeSlice> {
        let start_ptr = self.cursor_ptr();
        unsafe {
            while self.not_exhausted() && self.get_byte_at_cursor() != b'\n' {
                self.incr_cursor();
            }
            if self.not_exhausted() && self.get_byte_at_cursor() == b'\n' {
                let len = self.cursor_ptr() as usize - start_ptr as usize;
                self.incr_cursor(); // skip LF
                Ok(UnsafeSlice::new(start_ptr, len))
            } else {
                Err(ParseError::NotEnough)
            }
        }
    }
    /// Attempt to read a line, **rejecting an empty payload**
    fn read_line_pedantic(&mut self) -> ParseResult<UnsafeSlice> {
        let start_ptr = self.cursor_ptr();
        unsafe {
            while self.not_exhausted() && self.get_byte_at_cursor() != b'\n' {
                self.incr_cursor();
            }
            let len = self.cursor_ptr() as usize - start_ptr as usize;
            let has_lf = self.not_exhausted() && self.get_byte_at_cursor() == b'\n';
            if has_lf && len != 0 {
                self.incr_cursor(); // skip LF
                Ok(UnsafeSlice::new(start_ptr, len))
            } else {
                // just some silly hackery
                Err(transmute(has_lf))
            }
        }
    }
    /// Attempt to read an `usize` from the buffer
    fn read_usize(&mut self) -> ParseResult<usize> {
        let line = self.read_line_pedantic()?;
        let bytes = unsafe { line.as_slice() };
        let mut ret = 0usize;
        for byte in bytes {
            if byte.is_ascii_digit() {
                ret = match ret.checked_mul(10) {
                    Some(r) => r,
                    None => return Err(ParseError::DatatypeParseFailure),
                };
                ret = match ret.checked_add((byte & 0x0F) as _) {
                    Some(r) => r,
                    None => return Err(ParseError::DatatypeParseFailure),
                };
            } else {
                return Err(ParseError::DatatypeParseFailure);
            }
        }
        Ok(ret)
    }
}

impl<T> RawParserExt for T where T: RawParser + RawParserMeta {}
