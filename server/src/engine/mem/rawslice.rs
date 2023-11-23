/*
 * Created on Thu Nov 23 2023
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

use core::{
    borrow::Borrow,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    slice, str,
};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct RawStr {
    base: RawSlice<u8>,
}

impl RawStr {
    pub unsafe fn new(p: *const u8, l: usize) -> Self {
        Self {
            base: RawSlice::new(p, l),
        }
    }
    pub unsafe fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
        }
    }
    pub fn as_str(&self) -> &str {
        unsafe {
            // UNSAFE(@ohsayan): up to caller to ensure proper pointers
            str::from_utf8_unchecked(self.base.as_slice())
        }
    }
}

impl From<&'static str> for RawStr {
    fn from(s: &'static str) -> Self {
        unsafe { Self::new(s.as_ptr(), s.len()) }
    }
}

impl Borrow<str> for RawStr {
    fn borrow(&self) -> &str {
        unsafe { core::mem::transmute(self.clone()) }
    }
}

impl Deref for RawStr {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Debug for RawStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <str as fmt::Debug>::fmt(self.as_str(), f)
    }
}

impl fmt::Display for RawStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <str as fmt::Display>::fmt(self.as_str(), f)
    }
}

impl Hash for RawStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

pub struct RawSlice<T> {
    t: *const T,
    l: usize,
}

unsafe impl<T: Send> Send for RawSlice<T> {}
unsafe impl<T: Sync> Sync for RawSlice<T> {}

impl<T> RawSlice<T> {
    #[inline(always)]
    pub unsafe fn new(t: *const T, l: usize) -> Self {
        Self { t, l }
    }
    pub fn as_slice(&self) -> &[T] {
        unsafe {
            // UNSAFE(@ohsayan): the caller MUST guarantee that this remains valid throughout the usage of the slice
            slice::from_raw_parts(self.t, self.l)
        }
    }
    pub unsafe fn clone(&self) -> Self {
        Self { ..*self }
    }
}

impl<T: Hash> Hash for RawSlice<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl<T: PartialEq> PartialEq for RawSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for RawSlice<T> {}

impl<T: PartialOrd> PartialOrd for RawSlice<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl<T: Ord> Ord for RawSlice<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<T: fmt::Debug> fmt::Debug for RawSlice<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

impl<T> Deref for RawSlice<T> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
