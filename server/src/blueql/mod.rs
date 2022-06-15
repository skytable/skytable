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

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

mod ast;
mod error;
mod lexer;
// test modules
#[cfg(test)]
mod tests;
// re-export
pub use ast::Compiler;

use core::{fmt::Debug, slice};

pub struct RawSlice {
    ptr: *const u8,
    len: usize,
}

impl Debug for RawSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", String::from_utf8_lossy(self.as_slice()))
    }
}

impl PartialEq for RawSlice {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl RawSlice {
    pub const unsafe fn new(ptr: *const u8, len: usize) -> Self {
        Self { ptr, len }
    }
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
    pub const fn len(&self) -> usize {
        self.len
    }
}

impl<T> From<T> for RawSlice
where
    T: AsRef<[u8]>,
{
    fn from(t: T) -> Self {
        let t = t.as_ref();
        unsafe { Self::new(t.as_ptr(), t.len()) }
    }
}
