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
    i: usize,
}

impl<'a> BufferedScanner<'a> {
    pub const fn new(d: &'a [u8]) -> Self {
        Self { d, i: 0 }
    }
    pub const fn remaining(&self) -> usize {
        self.d.len() - self.i
    }
    pub const fn consumed(&self) -> usize {
        self.i
    }
    pub const fn cursor(&self) -> usize {
        self.i
    }
    pub(crate) fn has_left(&self, sizeof: usize) -> bool {
        self.remaining() >= sizeof
    }
    unsafe fn _cursor(&self) -> *const u8 {
        self.d.as_ptr().add(self.i)
    }
    pub fn eof(&self) -> bool {
        self.remaining() == 0
    }
    unsafe fn _incr(&mut self, by: usize) {
        self.i += by;
    }
    pub fn current(&self) -> &[u8] {
        &self.d[self.i..]
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
