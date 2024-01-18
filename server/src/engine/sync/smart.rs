/*
 * Created on Sun Jan 29 2023
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
    super::atm::{ORD_ACQ, ORD_REL, ORD_RLX},
    crate::engine::mem::unsafe_apis,
    std::{
        borrow::Borrow,
        fmt,
        hash::{Hash, Hasher},
        mem::{self, ManuallyDrop},
        ops::Deref,
        process,
        ptr::NonNull,
        slice, str,
        sync::atomic::{self, AtomicUsize},
    },
};

#[derive(Debug, Clone)]
pub struct StrRC {
    base: SliceRC<u8>,
}

impl StrRC {
    fn new(base: SliceRC<u8>) -> Self {
        Self { base }
    }
    pub fn from_bx(b: Box<str>) -> Self {
        let mut md = ManuallyDrop::new(b);
        Self::new(SliceRC::new(
            unsafe {
                // UNSAFE(@ohsayan): nullck + always aligned
                NonNull::new_unchecked(md.as_mut_ptr())
            },
            md.len(),
        ))
    }
    pub fn as_str(&self) -> &str {
        unsafe {
            // UNSAFE(@ohsayan): Ctor guarantees correctness
            str::from_utf8_unchecked(self.base.as_slice())
        }
    }
}

impl PartialEq for StrRC {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<str> for StrRC {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl From<String> for StrRC {
    fn from(s: String) -> Self {
        Self::from_bx(s.into_boxed_str())
    }
}

impl From<Box<str>> for StrRC {
    fn from(bx: Box<str>) -> Self {
        Self::from_bx(bx)
    }
}

impl<'a> From<&'a str> for StrRC {
    fn from(str: &'a str) -> Self {
        Self::from_bx(str.to_string().into_boxed_str())
    }
}

impl Deref for StrRC {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Eq for StrRC {}

pub struct SliceRC<T> {
    ptr: NonNull<T>,
    len: usize,
    rc: EArc,
}

impl<T> SliceRC<T> {
    #[inline(always)]
    fn new(ptr: NonNull<T>, len: usize) -> Self {
        Self {
            ptr,
            len,
            rc: unsafe {
                // UNSAFE(@ohsayan): we will eventually deallocate this
                EArc::new()
            },
        }
    }
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe {
            // UNSAFE(@ohsayan): rc guard + ctor
            slice::from_raw_parts(self.ptr.as_ptr(), self.len)
        }
    }
}

impl<T> Drop for SliceRC<T> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): Calling this within the dtor itself
            self.rc.rc_drop(|| {
                // dtor
                if mem::needs_drop::<T>() {
                    // UNSAFE(@ohsayan): dtor through, the ctor guarantees correct alignment and len
                    unsafe_apis::drop_slice_in_place(self.ptr.as_ptr(), self.len)
                }
                // dealloc
                // UNSAFE(@ohsayan): we allocated it + layout structure guaranteed by ctor
                unsafe_apis::dealloc_array(self.ptr.as_ptr(), self.len)
            })
        }
    }
}

impl<T> Clone for SliceRC<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        let new_rc = unsafe {
            // UNSAFE(@ohsayan): calling this within the clone routine
            self.rc.rc_clone()
        };
        Self {
            rc: new_rc,
            ..*self
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for SliceRC<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

impl<T: Hash> Hash for SliceRC<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl<T: PartialEq> PartialEq for SliceRC<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: PartialEq> PartialEq<[T]> for SliceRC<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.as_slice() == other
    }
}

impl<T: Eq> Eq for SliceRC<T> {}
impl<T> Borrow<[T]> for SliceRC<T> {
    fn borrow(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Deref for SliceRC<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

unsafe impl<T: Send> Send for SliceRC<T> {}
unsafe impl<T: Sync> Sync for SliceRC<T> {}

/// A simple rc
#[derive(Debug)]
pub struct EArc {
    base: RawRC<()>,
}

impl EArc {
    /// ## Safety
    ///
    /// Clean up your own memory, sir
    pub unsafe fn new() -> Self {
        Self {
            base: RawRC::new(()),
        }
    }
}

impl EArc {
    /// ## Safety
    ///
    /// Only call in an actual [`Clone`] context
    pub unsafe fn rc_clone(&self) -> Self {
        Self {
            base: self.base.rc_clone(),
        }
    }
    /// ## Safety
    ///
    /// Only call in dtor context
    pub unsafe fn rc_drop(&mut self, dropfn: impl FnMut()) {
        self.base.rc_drop(dropfn)
    }
}

/// The core atomic reference counter implementation. All smart pointers use this inside
pub struct RawRC<T> {
    rc_data: NonNull<RawRCData<T>>,
}

#[derive(Debug)]
struct RawRCData<T> {
    rc: AtomicUsize,
    data: T,
}

impl<T> RawRC<T> {
    /// Create a new [`RawRC`] instance
    ///
    /// ## Safety
    ///
    /// While this is **not unsafe** in the eyes of the language specification for safety, it still does violate a very common
    /// bug: memory leaks and we don't want that. So, it is upto the caller to clean this up
    pub unsafe fn new(data: T) -> Self {
        Self {
            rc_data: NonNull::new_unchecked(Box::leak(Box::new(RawRCData {
                rc: AtomicUsize::new(1),
                data,
            }))),
        }
    }
    pub fn data(&self) -> &T {
        unsafe {
            // UNSAFE(@ohsayan): we believe in the power of barriers!
            &(*self.rc_data.as_ptr()).data
        }
    }
    fn _rc(&self) -> &AtomicUsize {
        unsafe {
            // UNSAFE(@ohsayan): we believe in the power of barriers!
            &(*self.rc_data.as_ptr()).rc
        }
    }
    /// ## Safety
    ///
    /// Only call in an actual [`Clone`] context
    pub unsafe fn rc_clone(&self) -> Self {
        let new_rc = self._rc().fetch_add(1, ORD_RLX);
        if new_rc > (isize::MAX) as usize {
            // some incredibly degenerate case; this won't ever happen but who knows if some fella decided to have atomic overflow fun?
            process::abort();
        }
        Self { ..*self }
    }
    /// ## Safety
    ///
    /// Only call in dtor context
    pub unsafe fn rc_drop(&mut self, dropfn: impl FnMut()) {
        if self._rc().fetch_sub(1, ORD_REL) != 1 {
            // not the last man alive
            return;
        }
        // emit a fence for sync with stores
        atomic::fence(ORD_ACQ);
        self.rc_drop_slow(dropfn);
    }
    #[cold]
    #[inline(never)]
    unsafe fn rc_drop_slow(&mut self, mut dropfn: impl FnMut()) {
        // deallocate object
        dropfn();
        // deallocate rc
        drop(Box::from_raw(self.rc_data.as_ptr()));
    }
}

impl<T: fmt::Debug> fmt::Debug for RawRC<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawRC")
            .field("rc_data", unsafe {
                // UNSAFE(@ohsayan): rc guard
                self.rc_data.as_ref()
            })
            .finish()
    }
}
