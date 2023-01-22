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
            self._as_mut_ptr().add(self.l).write(v);
        }
        self.l += 1;
    }
}

impl<const N: usize, T> VInline<N, T> {
    const INLINE_NULL: MaybeUninit<T> = MaybeUninit::uninit();
    const INLINE_NULL_STACK: [MaybeUninit<T>; N] = [Self::INLINE_NULL; N];
    const ALLOC_MULTIPLIER: usize = 2;
    const _ENSURE_ALIGN: () =
        debug_assert!(mem::align_of::<Vec<String>>() == mem::align_of::<VInline<N, String>>());
    #[cfg(test)]
    fn will_be_on_stack(&self) -> bool {
        N >= self.l + 1
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
                self.dealloc_heap();
            }
        }
        self.d.h = nb;
        self.c = nc;
    }
    #[inline(always)]
    unsafe fn dealloc_heap(&mut self) {
        dealloc(self.d.h as *mut u8, Self::layout(self.capacity()))
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
                self.dealloc_heap();
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
