/*
 * Created on Tue Sep 13 2022
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

#[macro_use]
mod macros;
pub(super) mod ast;
pub(super) mod lexer;
pub(super) mod schema;
#[cfg(test)]
mod tests;

#[cfg(debug_assertions)]
use core::{fmt, ops::Deref};
use core::{mem, slice, str};

/*
    Lang errors
*/

pub type LangResult<T> = Result<T, LangError>;

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum LangError {
    InvalidNumericLiteral,
    InvalidStringLiteral,
    UnexpectedChar,
    InvalidTypeExpression,
    ExpectedStatement,
    UnexpectedEndofStatement,
    UnexpectedToken,
    InvalidDictionaryExpression,
    InvalidTypeDefinition,
}

/*
    Utils
*/

/// An unsafe, C-like slice that holds a ptr and length. Construction and usage is at the risk of the user
///
/// Notes:
/// - [`Clone`] is implemented for [`RawSlice`] because it is a simple bitwise copy of the fat ptr
/// - [`fmt::Debug`] is implemented in different ways
///     - With debug assertions enabled, it will output a slice
///     - In release mode, it will output the fat ptr meta
/// - [`PartialEq`] is implemented in debug mode with slice comparison, but is **NOT implemented for release mode in the
///   way you'd expect it to**. In release mode, a comparison will simply panic.
#[cfg_attr(not(debug_assertions), derive(Debug))]
#[derive(Clone)]
pub struct RawSlice {
    ptr: *const u8,
    len: usize,
}

// again, caller's responsibility
unsafe impl Send for RawSlice {}
unsafe impl Sync for RawSlice {}

impl RawSlice {
    const _EALIGN: () = assert!(mem::align_of::<Self>() == mem::align_of::<&[u8]>());
    const unsafe fn new(ptr: *const u8, len: usize) -> Self {
        Self { ptr, len }
    }
    unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr, self.len)
    }
    unsafe fn as_str(&self) -> &str {
        str::from_utf8_unchecked(self.as_slice())
    }
}

#[cfg(debug_assertions)]
impl fmt::Debug for RawSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(unsafe {
            // UNSAFE(@ohsayan): Only implemented in debug
            self.as_str()
        })
    }
}

#[cfg(debug_assertions)]
impl PartialEq for RawSlice {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): Callers must ensure validity during usage
            self.as_slice() == other.as_slice()
        }
    }
}

#[cfg(not(debug_assertions))]
impl PartialEq for RawSlice {
    fn eq(&self, _other: &Self) -> bool {
        panic!("Called partialeq on rawslice in release mode");
    }
}

#[cfg(debug_assertions)]
impl<U> PartialEq<U> for RawSlice
where
    U: Deref<Target = [u8]>,
{
    fn eq(&self, other: &U) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): Callers must ensure validity during usage
            self.as_slice() == other.deref()
        }
    }
}

impl From<&'static str> for RawSlice {
    fn from(st: &'static str) -> Self {
        unsafe { Self::new(st.as_bytes().as_ptr(), st.as_bytes().len()) }
    }
}
