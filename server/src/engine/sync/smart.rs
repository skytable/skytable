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
    std::{
        alloc::{dealloc, Layout},
        borrow::Borrow,
        fmt,
        hash::{Hash, Hasher},
        mem::{self, ManuallyDrop},
        ops::Deref,
        process,
        ptr::{self, NonNull},
        slice, str,
        sync::atomic::{self, AtomicUsize},
    },
};

pub type BytesRC = SliceRC<u8>;

#[derive(Debug, Clone, Hash)]
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
    rc: NonNull<AtomicUsize>,
}

impl<T> SliceRC<T> {
    #[inline(always)]
    fn new(ptr: NonNull<T>, len: usize) -> Self {
        Self {
            ptr,
            len,
            rc: unsafe {
                // UNSAFE(@ohsayan): box would either fail or return a collect alloc
                NonNull::new_unchecked(Box::into_raw(Box::new(AtomicUsize::new(0))))
            },
        }
    }
    #[inline(always)]
    pub fn from_bx(b: Box<[T]>) -> Self {
        let mut b = ManuallyDrop::new(b);
        unsafe {
            // UNSAFE(@ohsayan): non-null from the slice as usual
            Self::new(NonNull::new_unchecked(b.as_mut_ptr()), b.len())
        }
    }
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe {
            // UNSAFE(@ohsayan): rc guard + ctor
            slice::from_raw_parts(self.ptr.as_ptr(), self.len)
        }
    }
    #[inline(always)]
    fn _rc(&self) -> &AtomicUsize {
        unsafe {
            // UNSAFE(@ohsayan): rc + ctor
            self.rc.as_ref()
        }
    }
    /// SAFETY: Synchronize last man alive
    #[inline(never)]
    unsafe fn drop_slow(&self) {
        // dtor
        if mem::needs_drop::<T>() {
            // UNSAFE(@ohsayan): dtor through, the ctor guarantees correct alignment and len
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(self.ptr.as_ptr(), self.len));
        }
        // dealloc
        // UNSAFE(@ohsayan): we allocated it
        let layout = Layout::array::<T>(self.len).unwrap_unchecked();
        // UNSAFE(@ohsayan): layout structure guaranteed by ctor
        dealloc(self.ptr.as_ptr() as *mut u8, layout);
        // UNSAFE(@ohsayan): well cmon, look for yourself
        drop(Box::from_raw(self.rc.as_ptr()));
    }
}

impl<T> Drop for SliceRC<T> {
    fn drop(&mut self) {
        if self._rc().fetch_sub(1, ORD_REL) != 1 {
            // not the last man alive
            return;
        }
        // emit a fence for sync with stores
        atomic::fence(ORD_ACQ);
        unsafe {
            // UNSAFE(@ohsayan): Confirmed, we're the last one alive
            self.drop_slow();
        }
    }
}

impl<T> Clone for SliceRC<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        let new_rc = self._rc().fetch_add(1, ORD_RLX);
        if new_rc > (isize::MAX) as usize {
            // some incredibly degenerate case; this won't ever happen but who knows if some fella decided to have atomic overflow fun?
            process::abort();
        }
        Self { ..*self }
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