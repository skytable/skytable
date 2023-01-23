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

use {
    core::{
        alloc::Layout,
        fmt,
        mem::{self, ManuallyDrop, MaybeUninit},
        ops::{Deref, DerefMut},
        ptr, slice,
    },
    std::alloc::{alloc, dealloc},
};

union VData<const N: usize, T> {
    s: ManuallyDrop<[MaybeUninit<T>; N]>,
    h: *mut T,
}

struct VInline<const N: usize, T> {
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
        self.l += 1;
    }
    #[inline(always)]
    pub fn clear(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): as_slice_mut will always give a valid ptr
            ptr::drop_in_place(self._as_slice_mut());
        }
        self.l = 0;
    }
    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> T {
        if idx >= self.len() {
            panic!("index out of range");
        }
        unsafe {
            // UNSAFE(@ohsayan): Verified index is within range
            self.remove_unchecked(idx)
        }
    }
    #[inline(always)]
    pub fn remove_compact(&mut self, idx: usize) -> T {
        let r = self.remove(idx);
        self.optimize_capacity();
        r
    }
    #[inline(always)]
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
    fn on_heap(&self) -> bool {
        self.c > N
    }
    #[inline(always)]
    fn on_stack(&self) -> bool {
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
    fn layout(cap: usize) -> Layout {
        Layout::array::<T>(cap).unwrap()
    }
    #[inline(always)]
    fn ncap(&self) -> usize {
        self.c * Self::ALLOC_MULTIPLIER
    }
    fn alloc_block(cap: usize) -> *mut T {
        unsafe {
            // UNSAFE(@ohsayan): malloc bro
            let p = alloc(Self::layout(cap));
            assert!(!p.is_null(), "alloc,0");
            p as *mut T
        }
    }
    unsafe fn push_unchecked(&mut self, v: T) {
        self._as_mut_ptr().add(self.l).write(v);
    }
    pub fn optimize_capacity(&mut self) {
        if self.on_stack() || self.len() == self.capacity() {
            return;
        }
        if self.l <= N {
            unsafe {
                // UNSAFE(@ohsayan): non-null heap
                self.mv_to_stack();
            }
        } else {
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
        if !(self.l == self.capacity()) {
            return;
        }
        // allocate new block
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
    #[inline(always)]
    unsafe fn dealloc_heap(&mut self, heap: *mut T) {
        dealloc(heap as *mut u8, Self::layout(self.capacity()))
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

impl<const N: usize, T> Drop for VInline<N, T> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): correct ptr guaranteed by safe impl of _as_slice_mut()
            ptr::drop_in_place(self._as_slice_mut());
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

impl<T: Clone, const N: usize> Clone for VInline<N, T> {
    fn clone(&self) -> Self {
        unsafe {
            if self.on_stack() {
                // simple stack copy
                let mut new_stack = Self::INLINE_NULL_STACK;
                ptr::copy_nonoverlapping(self.d.s.as_ptr(), new_stack.as_mut_ptr(), self.l);
                Self {
                    d: VData {
                        s: ManuallyDrop::new(new_stack),
                    },
                    l: self.l,
                    c: N,
                }
            } else {
                // new allocation
                let nb = Self::alloc_block(self.len());
                ptr::copy_nonoverlapping(self._as_ptr(), nb, self.l);
                Self {
                    d: VData { h: nb },
                    l: self.l,
                    c: self.l,
                }
            }
        }
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

pub struct UArray<const N: usize, T> {
    a: [MaybeUninit<T>; N],
    l: usize,
}

impl<const N: usize, T> UArray<N, T> {
    const NULL: MaybeUninit<T> = MaybeUninit::uninit();
    const NULLED_ARRAY: [MaybeUninit<T>; N] = [Self::NULL; N];
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            a: Self::NULLED_ARRAY,
            l: 0,
        }
    }
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.l
    }
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        N
    }
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    unsafe fn incr_len(&mut self) {
        self.l += 1;
    }
    #[inline(always)]
    pub fn push(&mut self, v: T) {
        if self.l == N {
            panic!("stack,capof");
        }
        unsafe {
            // UNSAFE(@ohsayan): verified length is smaller
            self.push_unchecked(v);
        }
    }
    pub fn remove(&mut self, idx: usize) -> T {
        if idx >= self.len() {
            panic!("out of range. idx is `{idx}` but len is `{}`", self.len());
        }
        unsafe {
            // UNSAFE(@ohsayan): verified idx < l
            self.remove_unchecked(idx)
        }
    }
    /// SAFETY: idx < self.l
    unsafe fn remove_unchecked(&mut self, idx: usize) -> T {
        // UNSAFE(@ohsayan): Verified idx
        let target = self.a.as_mut_ptr().add(idx).cast::<T>();
        // UNSAFE(@ohsayan): Verified idx
        let ret = ptr::read(target);
        // UNSAFE(@ohsayan): ov; not-null; correct len
        ptr::copy(target.add(1), target, self.len() - idx - 1);
        ret
    }
    #[inline(always)]
    /// SAFETY: self.l < N
    unsafe fn push_unchecked(&mut self, v: T) {
        // UNSAFE(@ohsayan): verified correct offsets (N)
        self.a.as_mut_ptr().add(self.l).write(MaybeUninit::new(v));
        // UNSAFE(@ohsayan): all G since l =< N
        self.incr_len();
    }
    pub fn as_slice(&self) -> &[T] {
        unsafe {
            // UNSAFE(@ohsayan): ptr is always valid and len is correct, due to push impl
            slice::from_raw_parts(self.a.as_ptr() as *const T, self.l)
        }
    }
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe {
            // UNSAFE(@ohsayan): ptr is always valid and len is correct, due to push impl
            slice::from_raw_parts_mut(self.a.as_mut_ptr() as *mut T, self.l)
        }
    }
}

impl<const N: usize, T> Drop for UArray<N, T> {
    fn drop(&mut self) {
        if !self.is_empty() {
            unsafe {
                // UNSAFE(@ohsayan): as_slice_mut returns a correct offset
                ptr::drop_in_place(self.as_slice_mut())
            }
        }
    }
}

impl<const N: usize, T> Deref for UArray<N, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<const N: usize, T> DerefMut for UArray<N, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}

impl<const N: usize, T> FromIterator<T> for UArray<N, T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut slf = Self::new();
        iter.into_iter().for_each(|v| slf.push(v));
        slf
    }
}

impl<const N: usize, T> Extend<T> for UArray<N, T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        iter.into_iter().for_each(|v| self.push(v))
    }
}
