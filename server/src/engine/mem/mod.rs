/*
 * Created on Sun Jan 22 2023
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

#[cfg(test)]
mod tests;
mod uarray;
mod vinline;

use {
    crate::engine::ql::lex::Ident,
    std::{
        borrow::Borrow,
        fmt, mem,
        ops::{Deref, DerefMut},
    },
};

pub use uarray::UArray;
pub use vinline::VInline;

#[derive(PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct AStr<const N: usize> {
    base: UArray<N, u8>,
}
impl<const N: usize> AStr<N> {
    pub fn try_new(s: &str) -> Option<Self> {
        if s.len() <= N {
            Some(unsafe {
                // UNSAFE(@ohsayan): verified len
                Self::from_len_unchecked(s)
            })
        } else {
            None
        }
    }
    pub fn new(s: &str) -> Self {
        Self::try_new(s).expect("length overflow")
    }
    pub unsafe fn from_len_unchecked_ident(i: Ident<'_>) -> Self {
        Self::from_len_unchecked(i.as_str())
    }
    pub unsafe fn from_len_unchecked(s: &str) -> Self {
        Self {
            base: UArray::from_slice(s.as_bytes()),
        }
    }
    pub unsafe fn from_len_unchecked_bytes(b: &[u8]) -> Self {
        Self::from_len_unchecked(mem::transmute(b))
    }
    pub fn as_str(&self) -> &str {
        unsafe { mem::transmute(self.base.as_slice()) }
    }
    pub fn as_mut_str(&mut self) -> &mut str {
        unsafe { mem::transmute(self.base.as_slice_mut()) }
    }
}
impl<const N: usize> fmt::Debug for AStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl<const N: usize> Deref for AStr<N> {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}
impl<const N: usize> DerefMut for AStr<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_str()
    }
}
impl<'a, const N: usize> From<Ident<'a>> for AStr<N> {
    fn from(value: Ident<'a>) -> Self {
        Self::new(value.as_str())
    }
}
impl<'a, const N: usize> From<&'a str> for AStr<N> {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}
impl<const N: usize> PartialEq<str> for AStr<N> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}
impl<const N: usize> PartialEq<AStr<N>> for str {
    fn eq(&self, other: &AStr<N>) -> bool {
        self == other.as_str()
    }
}
impl<const N: usize> PartialEq<[u8]> for AStr<N> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}
impl<const N: usize> PartialEq<AStr<N>> for [u8] {
    fn eq(&self, other: &AStr<N>) -> bool {
        self == other.as_bytes()
    }
}
impl<const N: usize> AsRef<[u8]> for AStr<N> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl<const N: usize> AsRef<str> for AStr<N> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl<const N: usize> Default for AStr<N> {
    fn default() -> Self {
        Self::new("")
    }
}
impl<const N: usize> Borrow<[u8]> for AStr<N> {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl<const N: usize> Borrow<str> for AStr<N> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
