/*
 * Created on Fri Sep 15 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

use core::{ptr, slice};

pub type BufferedScanner<'a> = Scanner<'a, u8>;

#[derive(Debug, PartialEq)]
/// A scanner over a slice buffer `[T]`
pub struct Scanner<'a, T> {
    d: &'a [T],
    __cursor: usize,
}

impl<'a, T> Scanner<'a, T> {
    /// Create a new scanner, starting at position 0
    pub const fn new(d: &'a [T]) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): starting with 0 is always correct
            Self::new_with_cursor(d, 0)
        }
    }
    /// Create a new scanner, starting with the given position
    ///
    /// ## Safety
    ///
    /// `i` must be a valid index into the given slice
    pub const unsafe fn new_with_cursor(d: &'a [T], i: usize) -> Self {
        Self { d, __cursor: i }
    }
}

impl<'a, T> Scanner<'a, T> {
    pub const fn buffer_len(&self) -> usize {
        self.d.len()
    }
    /// Returns the remaining number of **items**
    pub const fn remaining(&self) -> usize {
        self.buffer_len() - self.__cursor
    }
    /// Returns the current cursor position
    pub const fn cursor(&self) -> usize {
        self.__cursor
    }
    /// Returns the buffer from the current position
    pub fn current_buffer(&self) -> &[T] {
        &self.d[self.__cursor..]
    }
    /// Returns the ptr to the cursor
    ///
    /// WARNING: The pointer might be invalid!
    pub const fn cursor_ptr(&self) -> *const T {
        unsafe {
            // UNSAFE(@ohsayan): assuming that the cursor is correctly initialized, this is always fine
            self.d.as_ptr().add(self.__cursor)
        }
    }
    /// Returns true if the scanner has reached eof
    pub fn eof(&self) -> bool {
        self.remaining() == 0
    }
    /// Returns true if the scanner has atleast `sizeof` bytes remaining
    pub fn has_left(&self, sizeof: usize) -> bool {
        self.remaining() >= sizeof
    }
    /// Returns true if the rounded cursor matches the predicate
    pub fn rounded_cursor_matches(&self, f: impl Fn(&T) -> bool) -> bool {
        f(&self.d[self.rounded_cursor()])
    }
    /// Same as `rounded_cursor_matches`, but with the added guarantee that no rounding was done
    pub fn rounded_cursor_not_eof_matches(&self, f: impl Fn(&T) -> bool) -> bool {
        self.rounded_cursor_matches(f) & !self.eof()
    }
    /// A shorthand for equality in `rounded_cursor_not_eof_matches`
    pub fn rounded_cursor_not_eof_equals(&self, v_t: T) -> bool
    where
        T: PartialEq,
    {
        self.rounded_cursor_matches(|v| v_t.eq(v)) & !self.eof()
    }
}

impl<'a, T> Scanner<'a, T> {
    pub fn inner_buffer(&self) -> &'a [T] {
        &self.d
    }
    /// Manually set the cursor position
    ///
    /// ## Safety
    /// The index must be valid
    pub unsafe fn set_cursor(&mut self, i: usize) {
        self.__cursor = i;
    }
    /// Increment the cursor
    ///
    /// ## Safety
    /// The buffer must not have reached EOF
    pub unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    /// Increment the cursor by the given amount
    ///
    /// ## Safety
    /// The buffer must have atleast `by` remaining
    pub unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.__cursor += by;
    }
    /// Increment the cursor if the given the condition is satisfied
    ///
    /// ## Safety
    /// Custom logic should ensure only legal cursor increments
    pub unsafe fn incr_cursor_if(&mut self, iff: bool) {
        self.incr_cursor_by(iff as _)
    }
    /// Decrement the cursor
    ///
    /// ## Safety
    /// The cursor must **not be at 0**
    pub unsafe fn decr_cursor(&mut self) {
        self.decr_cursor_by(1)
    }
    /// Decrement the cursor by the given amount
    ///
    /// ## Safety
    /// Should not overflow (overflow safety is ... nevermind)
    pub unsafe fn decr_cursor_by(&mut self, by: usize) {
        self.__cursor -= by;
    }
    /// Returns the current cursor
    ///
    /// ## Safety
    /// Buffer should NOT be at EOF
    pub unsafe fn deref_cursor(&self) -> T
    where
        T: Copy,
    {
        *self.cursor_ptr()
    }
    /// Returns the rounded cursor
    pub fn rounded_cursor(&self) -> usize {
        (self.buffer_len() - 1).min(self.__cursor)
    }
    /// Returns the current cursor value with rounding
    pub fn rounded_cursor_value(&self) -> T
    where
        T: Copy,
    {
        self.d[self.rounded_cursor()]
    }
}

impl<'a> Scanner<'a, u8> {
    #[cfg(test)]
    /// Attempt to parse the next byte
    pub fn try_next_byte(&mut self) -> Option<u8> {
        if self.eof() {
            None
        } else {
            Some(unsafe {
                // UNSAFE(@ohsayan): +remaining check
                self.next_byte()
            })
        }
    }
    /// Attempt to parse the next block (variable)
    pub fn try_next_variable_block(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.has_left(len) {
            Some(unsafe {
                // UNSAFE(@ohsayan): +remaining check
                self.next_chunk_variable(len)
            })
        } else {
            None
        }
    }
}

/// Incomplete buffered reads
#[derive(Debug, PartialEq)]
pub enum ScannerDecodeResult<T> {
    /// The value was decoded
    Value(T),
    /// We need more data to determine if we have the correct value
    NeedMore,
    /// Found an error while decoding a value
    Error,
}

impl<'a> Scanner<'a, u8> {
    /// Keep moving the cursor ahead while the predicate returns true
    pub fn trim_ahead(&mut self, f: impl Fn(u8) -> bool) {
        while self.rounded_cursor_not_eof_matches(|b| f(*b)) {
            unsafe {
                // UNSAFE(@ohsayan): not eof
                self.incr_cursor()
            }
        }
    }
    /// Attempt to parse a `\n` terminated integer (we move past the LF, so you can't see it)
    ///
    /// If we were unable to read in the integer, then the cursor will be restored to its starting position
    // TODO(@ohsayan): optimize
    pub fn try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(
        &mut self,
    ) -> ScannerDecodeResult<u64> {
        self.try_next_ascii_u64_lf_separated_with_result_or::<true>()
    }
    pub fn try_next_ascii_u64_lf_separated_with_result_or<const RESTORE_CURSOR: bool>(
        &mut self,
    ) -> ScannerDecodeResult<u64> {
        let mut okay = true;
        let start = self.cursor();
        let ret = self.try_next_ascii_u64_stop_at_lf(&mut okay);
        let payload_ok = okay;
        let lf = self.rounded_cursor_not_eof_matches(|b| *b == b'\n');
        okay &= lf;
        unsafe {
            // UNSAFE(@ohsayan): not eof
            // skip LF
            self.incr_cursor_if(okay)
        };
        if okay {
            ScannerDecodeResult::Value(ret)
        } else {
            if RESTORE_CURSOR {
                unsafe {
                    // UNSAFE(@ohsayan): we correctly restore the cursor
                    self.set_cursor(start)
                }
            }
            if payload_ok {
                // payload was ok, but we missed a null
                ScannerDecodeResult::NeedMore
            } else {
                // payload was NOT ok
                ScannerDecodeResult::Error
            }
        }
    }
    /// Attempt to parse a LF terminated integer (we move past the LF)
    /// If we were unable to read in the integer, then the cursor will be restored to its starting position
    pub fn try_next_ascii_u64_lf_separated_or_restore_cursor(&mut self) -> Option<u64> {
        self.try_next_ascii_u64_lf_separated_or::<true>()
    }
    pub fn try_next_ascii_u64_lf_separated_or<const RESTORE_CURSOR: bool>(
        &mut self,
    ) -> Option<u64> {
        let start = self.cursor();
        let mut okay = true;
        let ret = self.try_next_ascii_u64_stop_at_lf(&mut okay);
        let lf = self.rounded_cursor_not_eof_matches(|b| *b == b'\n');
        unsafe {
            // UNSAFE(@ohsayan): not eof
            self.incr_cursor_if(lf & okay)
        }
        if okay & lf {
            Some(ret)
        } else {
            if RESTORE_CURSOR {
                unsafe {
                    // UNSAFE(@ohsayan): we correctly restore the cursor
                    self.set_cursor(start)
                }
            }
            None
        }
    }
    /// Extracts whatever integer is possible using the current bytestream, stopping at a LF (but **not** skipping it)
    pub fn try_next_ascii_u64_stop_at_lf(&mut self, g_okay: &mut bool) -> u64 {
        self.try_next_ascii_u64_stop_at::<true>(g_okay, |byte| byte != b'\n')
    }
    /// Extracts whatever integer is possible using the current bytestream, stopping only when either an overflow occurs or when
    /// the closure returns false
    pub fn try_next_ascii_u64_stop_at<const ASCII_CHECK: bool>(
        &mut self,
        g_okay: &mut bool,
        keep_going_if: impl Fn(u8) -> bool,
    ) -> u64 {
        let mut ret = 0u64;
        let mut okay = true;
        while self.rounded_cursor_not_eof_matches(|b| keep_going_if(*b)) & okay {
            let b = self.d[self.cursor()];
            if ASCII_CHECK {
                okay &= b.is_ascii_digit();
            }
            ret = match ret.checked_mul(10) {
                Some(r) => r,
                None => {
                    okay = false;
                    break;
                }
            };
            ret = match ret.checked_add((b & 0x0F) as u64) {
                Some(r) => r,
                None => {
                    okay = false;
                    break;
                }
            };
            unsafe {
                // UNSAFE(@ohsayan): loop invariant
                self.incr_cursor_by(1)
            }
        }
        *g_okay &= okay;
        ret
    }
}

impl<'a> Scanner<'a, u8> {
    /// Attempt to parse the next [`i64`] value, stopping and skipping the STOP_BYTE
    ///
    /// WARNING: The cursor is NOT reversed
    pub fn try_next_ascii_i64_separated_by<const STOP_BYTE: u8>(&mut self) -> (bool, i64) {
        let (okay, int) = self.try_next_ascii_i64_stop_at(|b| b == STOP_BYTE);
        let lf = self.rounded_cursor_not_eof_equals(STOP_BYTE);
        unsafe {
            // UNSAFE(@ohsayan): not eof
            self.incr_cursor_if(lf & okay)
        }
        (lf & okay, int)
    }
    /// Attempt to parse the next [`i64`] value, stopping at the stop condition or stopping if an error occurred
    ///
    /// WARNING: It is NOT guaranteed that the stop condition was met
    pub fn try_next_ascii_i64_stop_at(&mut self, stop_if: impl Fn(u8) -> bool) -> (bool, i64) {
        let mut ret = 0i64;
        // check if we have a direction
        let current = self.rounded_cursor_value();
        let direction_negative = current == b'-';
        // skip negative
        unsafe {
            // UNSAFE(@ohsayan): not eof
            self.incr_cursor_if(direction_negative)
        }
        let mut okay = direction_negative | current.is_ascii_digit() & !self.eof();
        while self.rounded_cursor_not_eof_matches(|b| !stop_if(*b)) & okay {
            let byte = unsafe {
                // UNSAFE(@ohsayan): loop invariant
                self.next_byte()
            };
            okay &= byte.is_ascii_digit();
            ret = match ret.checked_mul(10) {
                Some(r) => r,
                None => {
                    okay = false;
                    break;
                }
            };
            if direction_negative {
                ret = match ret.checked_sub((byte & 0x0f) as i64) {
                    Some(r) => r,
                    None => {
                        okay = false;
                        break;
                    }
                };
            } else {
                ret = match ret.checked_add((byte & 0x0f) as i64) {
                    Some(r) => r,
                    None => {
                        okay = false;
                        break;
                    }
                }
            }
        }
        (okay, ret)
    }
}

impl<'a> Scanner<'a, u8> {
    /// Load the next [`u64`] LE
    pub unsafe fn next_u64_le(&mut self) -> u64 {
        u64::from_le_bytes(self.next_chunk())
    }
    /// Load the next block
    pub unsafe fn next_chunk<const N: usize>(&mut self) -> [u8; N] {
        let mut b = [0u8; N];
        ptr::copy_nonoverlapping(self.cursor_ptr(), b.as_mut_ptr(), N);
        self.incr_cursor_by(N);
        b
    }
    /// Load the next variable-sized block
    pub unsafe fn next_chunk_variable(&mut self, size: usize) -> &'a [u8] {
        let r = slice::from_raw_parts(self.cursor_ptr(), size);
        self.incr_cursor_by(size);
        r
    }
    /// Load the next byte
    pub unsafe fn next_byte(&mut self) -> u8 {
        let r = *self.cursor_ptr();
        self.incr_cursor_by(1);
        r
    }
}
