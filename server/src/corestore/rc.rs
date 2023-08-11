/*
 * Created on Mon Aug 15 2022
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

use std::{
    alloc::{alloc, dealloc, Layout},
    borrow::Borrow,
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    ptr::{self, NonNull},
    slice,
    sync::atomic::{self, AtomicUsize, Ordering},
};

/// A [`SharedSlice`] is a dynamically sized, heap allocated slice that can be safely shared across threads. This
/// type can be cheaply cloned and the only major cost is initialization that does a memcpy from the source into
/// a new heap allocation. Once init is complete, cloning only increments an atomic counter and when no more owners
/// of this data exists, i.e the object is orphaned, it will call its destructor and clean up the heap allocation.
/// Do note that two heap allocations are made:
/// - One for the actual data
/// - One for the shared state
pub struct SharedSlice {
    inner: NonNull<SharedSliceInner>,
}

impl Debug for SharedSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSlice")
            .field("data", &self.as_slice())
            .finish()
    }
}

// UNSAFE(@ohsayan): This is completely safe because our impl guarantees this
unsafe impl Send for SharedSlice {}
unsafe impl Sync for SharedSlice {}

impl SharedSlice {
    #[inline(always)]
    /// Create a new [`SharedSlice`] using the given local slice
    pub fn new(slice: &[u8]) -> Self {
        Self {
            inner: unsafe {
                NonNull::new_unchecked(Box::leak(Box::new(SharedSliceInner::new(slice))))
            },
        }
    }
    #[inline(always)]
    /// Returns a reference to te inner heap allocation for shared state
    fn inner(&self) -> &SharedSliceInner {
        unsafe { &*self.inner.as_ptr() }
    }
    #[inline(never)]
    /// A slow-path to deallocating all the heap allocations
    unsafe fn slow_drop(&self) {
        if self.len() != 0 {
            // IMPORTANT: Do not use the aligned pointer as a sentinel
            let inner = self.inner();
            // heap array dtor
            ptr::drop_in_place(slice::from_raw_parts_mut(inner.data as *mut u8, inner.len));
            // dealloc heap array
            dealloc(
                inner.data as *mut u8,
                Layout::array::<u8>(inner.len).unwrap(),
            )
        }
        // destroy shared state alloc
        drop(Box::from_raw(self.inner.as_ptr()))
    }
    /// Returns a local slice for the shared slice
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            /*
                UNSAFE(@ohsayan): The dtor guarantees that:
                1. we will never end up shooting ourselves in the foot
                2. the ptr is either valid, or invalid but well aligned. this upholds the raw_parts contract
                3. the len is either valid, or zero
            */
            let inner = self.inner();
            slice::from_raw_parts(inner.data, inner.len)
        }
    }
}

impl Clone for SharedSlice {
    #[inline(always)]
    fn clone(&self) -> Self {
        // relaxed is fine. the fencing in the dtor decr ensures we don't mess things up
        let _new_refcount = self.inner().rc.fetch_add(1, Ordering::Relaxed);
        Self { inner: self.inner }
    }
}

impl Drop for SharedSlice {
    #[inline(always)]
    fn drop(&mut self) {
        if self.inner().rc.fetch_sub(1, Ordering::Release) != 1 {
            // not the last owner; return
            return;
        }
        // use fence for sync with stores
        atomic::fence(Ordering::Acquire);
        unsafe {
            // UNSAFE(@ohsayan): At this point, we can be sure that no one else is using the data
            self.slow_drop();
        }
    }
}

// trait impls
impl Hash for SharedSlice {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for SharedSlice {
    #[inline(always)]
    fn eq(&self, other: &T) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl PartialEq<str> for SharedSlice {
    #[inline(always)]
    fn eq(&self, other: &str) -> bool {
        self.as_slice() == other.as_bytes()
    }
}

impl AsRef<[u8]> for SharedSlice {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for SharedSlice {
    #[inline(always)]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for SharedSlice {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> From<&'a [u8]> for SharedSlice {
    #[inline(always)]
    fn from(s: &'a [u8]) -> Self {
        Self::new(s)
    }
}

impl<'a> From<&'a str> for SharedSlice {
    #[inline(always)]
    fn from(s: &'a str) -> Self {
        Self::new(s.as_bytes())
    }
}

impl From<String> for SharedSlice {
    #[inline(always)]
    fn from(s: String) -> Self {
        Self::new(s.as_bytes())
    }
}

impl From<Vec<u8>> for SharedSlice {
    #[inline(always)]
    fn from(v: Vec<u8>) -> Self {
        Self::new(v.as_slice())
    }
}

impl Eq for SharedSlice {}

/// The shared state structure
struct SharedSliceInner {
    /// data ptr
    data: *const u8,
    /// data len
    len: usize,
    /// ref count
    rc: AtomicUsize,
}

impl SharedSliceInner {
    #[inline(always)]
    fn new(slice: &[u8]) -> Self {
        let layout = Layout::array::<u8>(slice.len()).unwrap();
        let data = unsafe {
            if slice.is_empty() {
                // HACK(@ohsayan): Just ensure that the address is aligned for this
                layout.align() as *mut u8
            } else {
                // UNSAFE(@ohsayan): Come on, just a malloc and memcpy
                let array_ptr = alloc(layout);
                ptr::copy_nonoverlapping(slice.as_ptr(), array_ptr, slice.len());
                array_ptr
            }
        };
        Self {
            data,
            len: slice.len(),
            rc: AtomicUsize::new(1),
        }
    }
}

#[test]
fn basic() {
    let slice = SharedSlice::from("hello");
    assert_eq!(slice, b"hello");
}

#[test]
fn basic_cloned() {
    let slice_a = SharedSlice::from("hello");
    let slice_a_clone = slice_a.clone();
    drop(slice_a);
    assert_eq!(slice_a_clone, b"hello");
}

#[test]
fn basic_cloned_across_threads() {
    use std::thread;
    const ST: &str = "world";
    const THREADS: usize = 8;
    let slice = SharedSlice::from(ST);
    let mut handles = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let clone = slice.clone();
        handles.push(thread::spawn(move || assert_eq!(clone, ST)))
    }
    handles.into_iter().for_each(|h| h.join().unwrap());
    assert_eq!(slice, ST);
}
