/*
 * Created on Thu Jan 18 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

/*!
    # Unsafe APIs

    This module provides abstractions (unsafe, still) over unsafe allocator and related APIs.

*/

use core::str;
use std::{
    alloc::{self, Layout},
    borrow::Borrow,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    ptr::{self, NonNull},
    slice,
};

/// Allocate the given layout. This will panic if the allocator returns an error
#[inline(always)]
pub unsafe fn alloc_layout<T>(layout: Layout) -> *mut T {
    let ptr = alloc::alloc(layout);
    assert!(!ptr.is_null(), "malloc failed");
    ptr as _
}

/// Allocate an block with an array layout of type `T` with space for `l` elements
#[inline(always)]
pub unsafe fn alloc_array<T>(l: usize) -> *mut T {
    if l != 0 {
        self::alloc_layout(Layout::array::<T>(l).unwrap_unchecked())
    } else {
        NonNull::dangling().as_ptr()
    }
}

/// Deallocate the given layout
#[inline(always)]
pub unsafe fn dealloc_layout(ptr: *mut u8, layout: Layout) {
    alloc::dealloc(ptr, layout)
}

/// Deallocate an array of type `T` with size `l`. This function will ensure that nonzero calls to the
/// allocator are made
#[inline(always)]
pub unsafe fn dealloc_array<T>(ptr: *mut T, l: usize) {
    if l != 0 {
        self::dealloc_layout(ptr as *mut u8, Layout::array::<T>(l).unwrap_unchecked())
    }
}

/// Run the dtor for the given slice (range)
#[inline(always)]
pub unsafe fn drop_slice_in_place_ref<T>(ptr: &mut [T]) {
    ptr::drop_in_place(ptr as *mut [T])
}

/// Run the dtor for the given slice (defined using ptr and len)
#[inline(always)]
pub unsafe fn drop_slice_in_place<T>(ptr: *mut T, l: usize) {
    ptr::drop_in_place(ptr::slice_from_raw_parts_mut(ptr, l))
}

/// Copy exactly `N` bytes from `src` to a new array of size `N`
#[inline(always)]
pub unsafe fn memcpy<const N: usize>(src: &[u8]) -> [u8; N] {
    let mut dst = [0u8; N];
    src.as_ptr().copy_to_nonoverlapping(dst.as_mut_ptr(), N);
    dst
}

pub struct BoxStr {
    p: *mut u8,
    l: usize,
}

impl BoxStr {
    pub fn new(b: &str) -> Self {
        let b = b.as_bytes();
        let p;
        unsafe {
            p = alloc_array::<u8>(b.len());
            ptr::copy_nonoverlapping(b.as_ptr(), p, b.len());
        }
        Self { p, l: b.len() }
    }
}

impl Deref for BoxStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe {
            // UNSAFE(@ohsayan): we own this!
            str::from_utf8_unchecked(slice::from_raw_parts(self.p, self.l))
        }
    }
}

impl Drop for BoxStr {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): we are an unique owner of this very allocation
            dealloc_array(self.p, self.l)
        }
    }
}

impl AsRef<str> for BoxStr {
    fn as_ref(&self) -> &str {
        self
    }
}

impl<T: AsRef<str>> PartialEq<T> for BoxStr {
    fn eq(&self, other: &T) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Eq for BoxStr {}

impl Hash for BoxStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl fmt::Debug for BoxStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Clone for BoxStr {
    fn clone(&self) -> Self {
        Self::new(self.as_ref())
    }
}

impl Borrow<str> for BoxStr {
    fn borrow(&self) -> &str {
        self
    }
}

unsafe impl Send for BoxStr {}
unsafe impl Sync for BoxStr {}

#[cfg(test)]
impl<'a> From<&'a str> for BoxStr {
    fn from(s: &'a str) -> Self {
        BoxStr::new(s)
    }
}
