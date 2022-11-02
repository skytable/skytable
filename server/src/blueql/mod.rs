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

mod ast;
mod error;
mod executor;
mod lexer;
pub mod util;
// test modules
#[cfg(test)]
mod tests;
// re-export
use {
    self::{ast::Statement, error::LangResult},
    crate::util::Life,
};
pub use {ast::Compiler, ast::Entity, executor::execute};

#[cfg(test)]
use core::fmt;
use core::{mem, slice};

#[allow(clippy::needless_lifetimes)]
#[inline(always)]
pub fn compile<'a>(src: &'a [u8], extra: usize) -> LangResult<Life<'a, Statement>> {
    Compiler::compile_with_extra(src, extra)
}

#[cfg_attr(not(test), derive(Debug))]
#[cfg_attr(not(test), derive(PartialEq, Eq))]
pub struct RawSlice {
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for RawSlice {}
unsafe impl Sync for RawSlice {}

#[cfg(test)]
impl fmt::Debug for RawSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(unsafe { self.as_slice() }))
    }
}

#[cfg(test)]
impl PartialEq for RawSlice {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.as_slice() == other.as_slice() }
    }
}
#[cfg(test)]
impl Eq for RawSlice {}

impl RawSlice {
    const _ENSURE_ALIGN: () = assert!(mem::align_of::<RawSlice>() == mem::align_of::<&[u8]>());
    pub const unsafe fn new(ptr: *const u8, len: usize) -> Self {
        Self { ptr, len }
    }
    pub unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr, self.len)
    }
    pub const fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
impl<T> From<T> for RawSlice
where
    T: AsRef<[u8]>,
{
    fn from(t: T) -> Self {
        let t = t.as_ref();
        unsafe { Self::new(t.as_ptr(), t.len()) }
    }
}
