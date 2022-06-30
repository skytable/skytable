/*
 * Created on Mon Feb 21 2022
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

use {
    core::{alloc::Layout, fmt, marker::PhantomData, mem::ManuallyDrop, ops::Deref, ptr, slice},
    std::alloc::dealloc,
};

/// A heap-allocated array
pub struct HeapArray<T> {
    ptr: *const T,
    len: usize,
    _marker: PhantomData<T>,
}

pub struct HeapArrayWriter<T> {
    base: Vec<T>,
}

impl<T> HeapArrayWriter<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            base: Vec::with_capacity(cap),
        }
    }
    /// ## Safety
    /// Caller must ensure that `idx <= cap`. If not, you'll corrupt your
    /// memory
    pub unsafe fn write_to_index(&mut self, idx: usize, element: T) {
        debug_assert!(idx <= self.base.capacity());
        ptr::write(self.base.as_mut_ptr().add(idx), element);
        self.base.set_len(self.base.len() + 1);
    }
    /// ## Safety
    /// This function can lead to memory unsafety in two ways:
    /// - Excess capacity: In that case, it will leak memory
    /// - Uninitialized elements: In that case, it will segfault while attempting to call
    /// `T`'s dtor
    pub unsafe fn finish(self) -> HeapArray<T> {
        let base = ManuallyDrop::new(self.base);
        HeapArray::new(base.as_ptr(), base.len())
    }
}

impl<T> HeapArray<T> {
    #[cfg(test)]
    pub fn new_from_vec(mut v: Vec<T>) -> Self {
        v.shrink_to_fit();
        let v = ManuallyDrop::new(v);
        unsafe { Self::new(v.as_ptr(), v.len()) }
    }
    pub unsafe fn new(ptr: *const T, len: usize) -> Self {
        Self {
            ptr,
            len,
            _marker: PhantomData,
        }
    }
    pub fn new_writer(cap: usize) -> HeapArrayWriter<T> {
        HeapArrayWriter::with_capacity(cap)
    }
    #[cfg(test)]
    pub fn as_slice(&self) -> &[T] {
        self
    }
}

impl<T> Drop for HeapArray<T> {
    fn drop(&mut self) {
        unsafe {
            // run dtor
            ptr::drop_in_place(ptr::slice_from_raw_parts_mut(self.ptr as *mut T, self.len));
            // deallocate
            let layout = Layout::array::<T>(self.len).unwrap();
            dealloc(self.ptr as *mut T as *mut u8, layout);
        }
    }
}

// totally fine because `u8`s can be safely shared across threads
unsafe impl<T: Send> Send for HeapArray<T> {}
unsafe impl<T: Sync> Sync for HeapArray<T> {}

impl<T> Deref for HeapArray<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T: fmt::Debug> fmt::Debug for HeapArray<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T: PartialEq> PartialEq for HeapArray<T> {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

#[test]
fn heaparray_impl() {
    // basically, this shouldn't segfault
    let heap_array = b"notasuperuser".to_vec();
    let heap_array = HeapArray::new_from_vec(heap_array);
    assert_eq!(heap_array.as_slice(), b"notasuperuser");
}
