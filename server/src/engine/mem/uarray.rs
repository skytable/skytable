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

use core::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr, slice,
};

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
