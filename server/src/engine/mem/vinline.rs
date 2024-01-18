/*
 * Created on Mon Jan 23 2023
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
    super::unsafe_apis,
    std::{
        fmt,
        iter::FusedIterator,
        mem::{self, ManuallyDrop, MaybeUninit},
        ops::{Deref, DerefMut},
        ptr, slice,
    },
};

union VData<const N: usize, T> {
    s: ManuallyDrop<[MaybeUninit<T>; N]>,
    h: *mut T,
}

pub struct VInline<const N: usize, T> {
    d: VData<N, T>,
    l: usize,
    c: usize,
}

impl<const N: usize, T> VInline<N, T> {
    #[inline(always)]
    pub const fn new() -> Self {
        let _ = Self::_ENSURE_ALIGN;
        Self {
            d: VData {
                s: ManuallyDrop::new(Self::INLINE_NULL_STACK),
            },
            l: 0,
            c: N,
        }
    }
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        self.c
    }
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.l
    }
    #[inline(always)]
    pub fn push(&mut self, v: T) {
        self.grow();
        unsafe {
            // UNSAFE(@ohsayan): grow allocated the cap we needed
            self.push_unchecked(v);
        }
    }
    #[inline(always)]
    #[allow(unused)]
    pub fn clear(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): as_slice_mut will always give a valid ptr
            unsafe_apis::drop_slice_in_place_ref(self._as_slice_mut())
        }
        self.l = 0;
    }
    #[inline(always)]
    #[allow(unused)]
    pub fn remove(&mut self, idx: usize) -> T {
        if !(idx < self.len()) {
            panic!("index out of range");
        }
        unsafe {
            // UNSAFE(@ohsayan): Verified index is within range
            self.remove_unchecked(idx)
        }
    }
    #[inline(always)]
    #[allow(unused)]
    pub fn remove_compact(&mut self, idx: usize) -> T {
        let r = self.remove(idx);
        self.optimize_capacity();
        r
    }
    #[inline(always)]
    #[allow(unused)]
    /// SAFETY: `idx` must be < l
    unsafe fn remove_unchecked(&mut self, idx: usize) -> T {
        // UNSAFE(@ohsayan): idx is in range
        let ptr = self.as_mut_ptr().add(idx);
        // UNSAFE(@ohsayan): idx is in range and is valid
        let ret = ptr::read(ptr);
        // UNSAFE(@ohsayan): move all elements to the left
        ptr::copy(ptr.add(1), ptr, self.len() - idx - 1);
        // UNSAFE(@ohsayan): this is our new length
        self.set_len(self.len() - 1);
        ret
    }
    #[inline(always)]
    unsafe fn set_len(&mut self, len: usize) {
        self.l = len;
    }
}

impl<const N: usize, T> VInline<N, T> {
    const INLINE_NULL: MaybeUninit<T> = MaybeUninit::uninit();
    const INLINE_NULL_STACK: [MaybeUninit<T>; N] = [Self::INLINE_NULL; N];
    const ALLOC_MULTIPLIER: usize = 2;
    const _ENSURE_ALIGN: () =
        debug_assert!(mem::align_of::<Vec<String>>() == mem::align_of::<VInline<N, String>>());
    #[inline(always)]
    #[cfg(test)]
    pub fn on_heap(&self) -> bool {
        self.c > N
    }
    #[inline(always)]
    pub fn on_stack(&self) -> bool {
        self.c == N
    }
    #[inline(always)]
    fn _as_ptr(&self) -> *const T {
        unsafe {
            // UNSAFE(@ohsayan): We make legal accesses by checking state
            if self.on_stack() {
                self.d.s.as_ptr() as *const T
            } else {
                self.d.h as *const T
            }
        }
    }
    #[inline(always)]
    fn _as_mut_ptr(&mut self) -> *mut T {
        unsafe {
            // UNSAFE(@ohsayan): We make legal accesses by checking state
            if self.on_stack() {
                (&mut self.d).s.as_mut_ptr() as *mut T
            } else {
                (&mut self.d).h as *mut T
            }
        }
    }
    #[inline(always)]
    fn _as_slice(&self) -> &[T] {
        unsafe {
            // UNSAFE(@ohsayan): _as_ptr() will ensure correct addresses
            slice::from_raw_parts(self._as_ptr(), self.l)
        }
    }
    #[inline(always)]
    fn _as_slice_mut(&mut self) -> &mut [T] {
        unsafe {
            // UNSAFE(@ohsayan): _as_mut_ptr() will ensure correct addresses
            slice::from_raw_parts_mut(self._as_mut_ptr(), self.l)
        }
    }
    #[inline(always)]
    fn ncap(&self) -> usize {
        self.c * Self::ALLOC_MULTIPLIER
    }
    fn alloc_block(cap: usize) -> *mut T {
        unsafe {
            // UNSAFE(@ohsayan): this is a malloc
            unsafe_apis::alloc_array(cap)
        }
    }
    pub unsafe fn push_unchecked(&mut self, v: T) {
        self._as_mut_ptr().add(self.l).write(v);
        self.l += 1;
    }
    pub fn optimize_capacity(&mut self) {
        if self.on_stack() || self.len() == self.capacity() {
            return;
        }
        if self.l <= N {
            // the current can be fit into the stack, and we aren't on the stack. so copy data from heap and move it to the stack
            unsafe {
                // UNSAFE(@ohsayan): non-null heap
                self.mv_to_stack();
            }
        } else {
            // in this case, we can't move to stack but can optimize the heap size. so create a new heap, memcpy old heap and destroy old heap (NO dtor)
            let nb = Self::alloc_block(self.len());
            unsafe {
                // UNSAFE(@ohsayan): nonov; non-null
                ptr::copy_nonoverlapping(self.d.h, nb, self.len());
                // UNSAFE(@ohsayan): non-null heap
                self.dealloc_heap(self.d.h);
            }
            self.d.h = nb;
            self.c = self.len();
        }
    }
    /// SAFETY: (1) non-null heap
    unsafe fn mv_to_stack(&mut self) {
        let heap = self.d.h;
        // UNSAFE(@ohsayan): nonov; non-null (stack lol)
        ptr::copy_nonoverlapping(self.d.h, (&mut self.d).s.as_mut_ptr() as *mut T, self.len());
        // UNSAFE(@ohsayan): non-null heap
        self.dealloc_heap(heap);
        self.c = N;
    }
    #[inline]
    fn grow(&mut self) {
        if self.l == self.capacity() {
            // allocate new block because we've run out of capacity
            let nc = self.ncap();
            let nb = Self::alloc_block(nc);
            if self.on_stack() {
                // stack -> heap
                unsafe {
                    // UNSAFE(@ohsayan): non-null; valid len
                    ptr::copy_nonoverlapping(self.d.s.as_ptr() as *const T, nb, self.l);
                }
            } else {
                unsafe {
                    // UNSAFE(@ohsayan): non-null; valid len
                    ptr::copy_nonoverlapping(self.d.h.cast_const(), nb, self.l);
                    // UNSAFE(@ohsayan): non-null heap
                    self.dealloc_heap(self.d.h);
                }
            }
            self.d.h = nb;
            self.c = nc;
        }
    }
    #[inline(always)]
    unsafe fn dealloc_heap(&mut self, heap: *mut T) {
        unsafe_apis::dealloc_array(heap, self.capacity())
    }
}

impl<const N: usize, T> Deref for VInline<N, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self._as_slice()
    }
}

impl<const N: usize, T> DerefMut for VInline<N, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self._as_slice_mut()
    }
}

impl<const M: usize, const N: usize, T: PartialEq> PartialEq<VInline<M, T>> for VInline<N, T> {
    fn eq(&self, other: &VInline<M, T>) -> bool {
        self._as_slice() == other._as_slice()
    }
}

impl<const N: usize, T> Drop for VInline<N, T> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): correct ptr guaranteed by safe impl of _as_slice_mut()
            unsafe_apis::drop_slice_in_place_ref(self._as_slice_mut());
            if !self.on_stack() {
                // UNSAFE(@ohsayan): non-null heap
                self.dealloc_heap(self.d.h);
            }
        }
    }
}

impl<const N: usize, T: fmt::Debug> fmt::Debug for VInline<N, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T, const N: usize> Extend<T> for VInline<N, T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        // FIXME(@ohsayan): Optimize capacity match upfront
        iter.into_iter().for_each(|item| self.push(item))
    }
}

#[cfg(test)]
impl<T, const M: usize, const N: usize> From<[T; N]> for VInline<M, T> {
    fn from(a: [T; N]) -> Self {
        a.into_iter().collect()
    }
}

impl<T: Clone, const N: usize> Clone for VInline<N, T> {
    fn clone(&self) -> Self {
        self.iter().cloned().collect()
    }
}

impl<T, const N: usize> FromIterator<T> for VInline<N, T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let it = iter.into_iter();
        let mut slf = Self::new();
        slf.extend(it);
        slf
    }
}

pub struct IntoIter<const N: usize, T> {
    v: VInline<N, T>,
    l: usize,
    i: usize,
}

impl<const N: usize, T> IntoIter<N, T> {
    #[inline(always)]
    fn _next(&mut self) -> Option<T> {
        if self.i == self.l {
            return None;
        }
        unsafe {
            let current = self.i;
            self.i += 1;
            // UNSAFE(@ohsayan): i < l; so in all cases we are behind EOA
            ptr::read(self.v._as_ptr().add(current).cast())
        }
    }
    #[inline(always)]
    fn _next_back(&mut self) -> Option<T> {
        if self.i == self.l {
            return None;
        }
        unsafe {
            // UNSAFE(@ohsayan): we get the back pointer and move back; always behind EOA so we're chill
            self.l -= 1;
            ptr::read(self.v._as_ptr().add(self.l).cast())
        }
    }
}

impl<const N: usize, T> Drop for IntoIter<N, T> {
    fn drop(&mut self) {
        if self.i < self.l {
            // sweet
            unsafe {
                // UNSAFE(@ohsayan): Safe because we maintain the EOA cond; second, the l is the remaining part
                unsafe_apis::drop_slice_in_place(self.v._as_mut_ptr().add(self.i), self.l - self.i)
            }
        }
    }
}

impl<const N: usize, T> Iterator for IntoIter<N, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self._next()
    }
}
impl<const N: usize, T> ExactSizeIterator for IntoIter<N, T> {}
impl<const N: usize, T> FusedIterator for IntoIter<N, T> {}
impl<const N: usize, T> DoubleEndedIterator for IntoIter<N, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self._next_back()
    }
}

impl<const N: usize, T> IntoIterator for VInline<N, T> {
    type Item = T;

    type IntoIter = IntoIter<N, T>;

    fn into_iter(mut self) -> Self::IntoIter {
        let real = self.len();
        unsafe {
            // UNSAFE(@ohsayan): drop work for intoiter
            // HACK(@ohsayan): same juicy drop hack
            self.set_len(0);
        }
        Self::IntoIter {
            v: self,
            l: real,
            i: 0,
        }
    }
}
