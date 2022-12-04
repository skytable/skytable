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
#[cfg(feature = "nightly")]
#[cfg(test)]
mod benches;
pub(super) mod ddl;
pub(super) mod dml;
pub(super) mod lexer;
pub(super) mod schema;
#[cfg(test)]
mod tests;

#[cfg(test)]
use core::{fmt, ops::Deref};
use core::{mem, ptr::NonNull, slice, str};

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
    InvalidUnsafeLiteral,
}

/*
    Utils
*/

/// An unsafe, C-like slice that holds a ptr and length. Construction and usage is at the risk of the user
///
/// Notes:
/// - [`Clone`] is implemented for [`RawSlice`] because it is a simple bitwise copy of the fat ptr
/// - [`fmt::Debug`] is implemented in different ways
///     - For test builds like test and bench, it will output a slice
///     - In release mode, it will output the fat ptr meta
/// - [`PartialEq`] is implemented in debug mode with slice comparison, but is **NOT implemented for release mode in the
///   way you'd expect it to**. In release mode (non-test), a comparison will simply panic.
#[cfg_attr(not(test), derive(Debug))]
#[derive(Clone)]
pub struct RawSlice {
    ptr: NonNull<u8>,
    len: usize,
}

// again, caller's responsibility
unsafe impl Send for RawSlice {}
unsafe impl Sync for RawSlice {}

impl RawSlice {
    const _EALIGN: () = assert!(mem::align_of::<Self>() == mem::align_of::<&[u8]>());
    const FAKE_SLICE: Self = unsafe { Self::new_from_str("") };
    const unsafe fn new(ptr: *const u8, len: usize) -> Self {
        Self {
            ptr: NonNull::new_unchecked(ptr.cast_mut()),
            len,
        }
    }
    const unsafe fn new_from_str(s: &str) -> Self {
        Self::new(s.as_bytes().as_ptr(), s.as_bytes().len())
    }
    unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr.as_ptr(), self.len)
    }
    unsafe fn as_str(&self) -> &str {
        str::from_utf8_unchecked(self.as_slice())
    }
}

#[cfg(test)]
impl fmt::Debug for RawSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(unsafe {
            // UNSAFE(@ohsayan): Only implemented in debug
            self.as_str()
        })
    }
}

#[cfg(test)]
impl PartialEq for RawSlice {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): Callers must ensure validity during usage
            self.as_slice() == other.as_slice()
        }
    }
}

#[cfg(not(test))]
impl PartialEq for RawSlice {
    fn eq(&self, _other: &Self) -> bool {
        panic!("Called partialeq on rawslice in release mode");
    }
}

#[cfg(test)]
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
