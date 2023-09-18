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

#[derive(Debug)]
pub struct BufferedScanner<'a> {
    d: &'a [u8],
    __cursor: usize,
}

impl<'a> BufferedScanner<'a> {
    pub const fn new(d: &'a [u8]) -> Self {
        unsafe { Self::new_with_cursor(d, 0) }
    }
    pub const unsafe fn new_with_cursor(d: &'a [u8], i: usize) -> Self {
        Self { d, __cursor: i }
    }
    pub const fn remaining(&self) -> usize {
        self.d.len() - self.__cursor
    }
    pub const fn consumed(&self) -> usize {
        self.__cursor
    }
    pub const fn cursor(&self) -> usize {
        self.__cursor
    }
    pub fn current(&self) -> &[u8] {
        &self.d[self.__cursor..]
    }
    pub fn eof(&self) -> bool {
        self.remaining() == 0
    }
    pub fn has_left(&self, sizeof: usize) -> bool {
        self.remaining() >= sizeof
    }
    pub fn matches_cursor_rounded(&self, f: impl Fn(u8) -> bool) -> bool {
        f(self.d[self.d.len().min(self.__cursor)])
    }
    pub fn matches_cursor_rounded_and_not_eof(&self, f: impl Fn(u8) -> bool) -> bool {
        self.matches_cursor_rounded(f) & !self.eof()
    }
}

impl<'a> BufferedScanner<'a> {
    pub unsafe fn set_cursor(&mut self, i: usize) {
        self.__cursor = i;
    }
    pub unsafe fn move_ahead_by(&mut self, by: usize) {
        self._incr(by)
    }
    pub unsafe fn move_back(&mut self) {
        self.move_back_by(1)
    }
    pub unsafe fn move_back_by(&mut self, by: usize) {
        self.__cursor -= by;
    }
    unsafe fn _incr(&mut self, by: usize) {
        self.__cursor += by;
    }
    unsafe fn _cursor(&self) -> *const u8 {
        self.d.as_ptr().add(self.__cursor)
    }
}

impl<'a> BufferedScanner<'a> {
    pub fn try_next_byte(&mut self) -> Option<u8> {
        if self.eof() {
            None
        } else {
            Some(unsafe { self.next_byte() })
        }
    }
    pub fn try_next_block<const N: usize>(&mut self) -> Option<[u8; N]> {
        if self.has_left(N) {
            Some(unsafe { self.next_chunk() })
        } else {
            None
        }
    }
    pub fn try_next_variable_block(&'a mut self, len: usize) -> Option<&'a [u8]> {
        if self.has_left(len) {
            Some(unsafe { self.next_chunk_variable(len) })
        } else {
            None
        }
    }
}

pub enum BufferedReadResult<T> {
    Value(T),
    NeedMore,
    Error,
}

impl<'a> BufferedScanner<'a> {
    /// Attempt to parse a `\n` terminated (we move past the LF, so you can't see it)
    ///
    /// If we were unable to read in the integer, then the cursor will be restored to its starting position
    // TODO(@ohsayan): optimize
    pub fn try_next_ascii_u64_lf_separated(&mut self) -> BufferedReadResult<u64> {
        let mut okay = true;
        let start = self.cursor();
        let mut ret = 0u64;
        while self.matches_cursor_rounded_and_not_eof(|b| b != b'\n') & okay {
            let b = self.d[self.cursor()];
            okay &= b.is_ascii_digit();
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
            unsafe { self._incr(1) }
        }
        let payload_ok = okay;
        let null_ok = self.matches_cursor_rounded_and_not_eof(|b| b == b'\n');
        okay &= null_ok;
        unsafe { self._incr(okay as _) }; // skip LF
        if okay {
            BufferedReadResult::Value(ret)
        } else {
            unsafe { self.set_cursor(start) }
            if payload_ok {
                // payload was ok, but we missed a null
                BufferedReadResult::NeedMore
            } else {
                // payload was NOT ok
                BufferedReadResult::Error
            }
        }
    }
}

impl<'a> BufferedScanner<'a> {
    pub unsafe fn next_u64_le(&mut self) -> u64 {
        u64::from_le_bytes(self.next_chunk())
    }
    pub unsafe fn next_chunk<const N: usize>(&mut self) -> [u8; N] {
        let mut b = [0u8; N];
        ptr::copy_nonoverlapping(self._cursor(), b.as_mut_ptr(), N);
        self._incr(N);
        b
    }
    pub unsafe fn next_chunk_variable(&mut self, size: usize) -> &[u8] {
        let r = slice::from_raw_parts(self._cursor(), size);
        self._incr(size);
        r
    }
    pub unsafe fn next_byte(&mut self) -> u8 {
        let r = *self._cursor();
        self._incr(1);
        r
    }
}
