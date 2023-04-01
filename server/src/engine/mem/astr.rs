/*
 * Created on Sat Feb 25 2023
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

use {
    super::UArray,
    crate::engine::ql::lex::Ident,
    std::{
        borrow::Borrow,
        fmt, mem,
        ops::{Deref, DerefMut},
    },
};

#[derive(PartialEq, Eq, Hash, Clone)]
#[repr(transparent)]
pub struct AStr<const N: usize> {
    base: UArray<N, u8>,
}
impl<const N: usize> AStr<N> {
    #[inline(always)]
    pub fn check(v: &str) -> bool {
        v.len() <= N
    }
    #[inline(always)]
    pub fn try_new(s: &str) -> Option<Self> {
        if Self::check(s) {
            Some(unsafe {
                // UNSAFE(@ohsayan): verified len
                Self::from_len_unchecked(s)
            })
        } else {
            None
        }
    }
    #[inline(always)]
    pub fn new(s: &str) -> Self {
        Self::try_new(s).expect("length overflow")
    }
    #[inline(always)]
    pub unsafe fn from_len_unchecked_ident(i: Ident<'_>) -> Self {
        Self::from_len_unchecked(i.as_str())
    }
    #[inline(always)]
    pub unsafe fn from_len_unchecked(s: &str) -> Self {
        Self {
            base: UArray::from_slice(s.as_bytes()),
        }
    }
    #[inline(always)]
    pub unsafe fn from_len_unchecked_bytes(b: &[u8]) -> Self {
        Self::from_len_unchecked(mem::transmute(b))
    }
    #[inline(always)]
    pub fn _as_str(&self) -> &str {
        unsafe {
            // UNSAFE(@ohsayan): same layout
            mem::transmute(self._as_bytes())
        }
    }
    #[inline(always)]
    pub fn _as_mut_str(&mut self) -> &mut str {
        unsafe {
            // UNSAFE(@ohsayan): same layout
            mem::transmute(self._as_bytes_mut())
        }
    }
    pub fn _as_bytes(&self) -> &[u8] {
        self.base.as_slice()
    }
    pub fn _as_bytes_mut(&mut self) -> &mut [u8] {
        self.base.as_slice_mut()
    }
}
impl<const N: usize> fmt::Debug for AStr<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self._as_str())
    }
}
impl<const N: usize> Deref for AStr<N> {
    type Target = str;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self._as_str()
    }
}
impl<const N: usize> DerefMut for AStr<N> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self._as_mut_str()
    }
}
impl<'a, const N: usize> From<Ident<'a>> for AStr<N> {
    #[inline(always)]
    fn from(value: Ident<'a>) -> Self {
        Self::new(value.as_str())
    }
}
impl<'a, const N: usize> From<&'a str> for AStr<N> {
    #[inline(always)]
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}
impl<const N: usize> PartialEq<str> for AStr<N> {
    #[inline(always)]
    fn eq(&self, other: &str) -> bool {
        self._as_bytes() == other.as_bytes()
    }
}
impl<const N: usize> PartialEq<AStr<N>> for str {
    #[inline(always)]
    fn eq(&self, other: &AStr<N>) -> bool {
        other._as_bytes() == self.as_bytes()
    }
}
impl<const N: usize> PartialEq<[u8]> for AStr<N> {
    #[inline(always)]
    fn eq(&self, other: &[u8]) -> bool {
        self._as_bytes() == other
    }
}
impl<const N: usize> PartialEq<AStr<N>> for [u8] {
    #[inline(always)]
    fn eq(&self, other: &AStr<N>) -> bool {
        self == other.as_bytes()
    }
}
impl<const N: usize> AsRef<[u8]> for AStr<N> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self._as_bytes()
    }
}
impl<const N: usize> AsRef<str> for AStr<N> {
    #[inline(always)]
    fn as_ref(&self) -> &str {
        self._as_str()
    }
}
impl<const N: usize> Default for AStr<N> {
    #[inline(always)]
    fn default() -> Self {
        Self::new("")
    }
}
impl<const N: usize> Borrow<[u8]> for AStr<N> {
    #[inline(always)]
    fn borrow(&self) -> &[u8] {
        self._as_bytes()
    }
}
