/*
 * Created on Sat Jan 20 2024
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

#![allow(dead_code)]

use {
    super::unsafe_apis,
    std::{
        fmt, ops,
        ptr::{self, NonNull},
        slice,
    },
};

/// A fixed capacity vector
///
/// - This is useful for situations where the stack is heavily used (such as during a recursive call, or too
/// many stack variables) so much that even though we have a fixed capacity, pushing an array on the stack
/// would cause an overflow.
/// - Also useful when it doesn't make sense to use the stack at all
pub struct FixedVec<T, const CAP: usize> {
    p: NonNull<T>,
    l: usize,
}

unsafe impl<T: Send, const CAP: usize> Send for FixedVec<T, CAP> {}
unsafe impl<T: Sync, const CAP: usize> Sync for FixedVec<T, CAP> {}

impl<T, const CAP: usize> Default for FixedVec<T, CAP> {
    fn default() -> Self {
        Self::allocate()
    }
}

impl<T, const CAP: usize> FixedVec<T, CAP> {
    const IS_ZERO: () = assert!(CAP != 0);
    pub fn allocate() -> Self {
        let _ = Self::IS_ZERO;
        Self {
            p: unsafe {
                // UNSAFE(@ohsayan): simple malloc
                NonNull::new_unchecked(unsafe_apis::alloc_array(CAP))
            },
            l: 0,
        }
    }
    pub fn len(&self) -> usize {
        self.l
    }
    pub fn remaining_capacity(&self) -> usize {
        CAP - self.len()
    }
    pub fn at_capacity(&self) -> bool {
        self.remaining_capacity() == 0
    }
    pub unsafe fn set_len(&mut self, l: usize) {
        self.l = l;
    }
    pub unsafe fn decr_len_by(&mut self, by: usize) {
        self.set_len(self.len() - by)
    }
}

impl<T, const CAP: usize> FixedVec<T, CAP> {
    pub fn try_push(&mut self, v: T)
    where
        T: fmt::Debug,
    {
        self.try_push_result(v).unwrap()
    }
    pub fn try_push_result(&mut self, v: T) -> Result<(), T> {
        if self.remaining_capacity() == 0 {
            Err(v)
        } else {
            unsafe {
                // UNSAFE(@ohsayan): verified capacity
                self.push(v);
                Ok(())
            }
        }
    }
    pub unsafe fn extend_from_slice(&mut self, block: &[T])
    where
        T: Copy,
    {
        debug_assert!(block.len() <= self.remaining_capacity(), "reached capacity");
        ptr::copy_nonoverlapping(block.as_ptr(), self.cptr(), block.len());
        self.l += block.len();
    }
    pub unsafe fn push(&mut self, v: T) {
        debug_assert_ne!(self.remaining_capacity(), 0, "reached capacity");
        self.cptr().write(v);
        self.l += 1;
    }
    pub fn clear(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): completely fine as we have the correct length
            unsafe_apis::drop_slice_in_place_ref(self.slice_mut());
            self.l = 0;
        }
    }
    pub unsafe fn clear_start(&mut self, cnt: usize) {
        debug_assert!(cnt < self.len(), "`cnt` is greater than vector length");
        // drop
        unsafe_apis::drop_slice_in_place(self.p.as_ptr(), cnt);
        // move block
        ptr::copy(self.p.as_ptr().add(cnt), self.p.as_ptr(), self.l - cnt);
        self.l -= cnt;
    }
}

impl<T, const CAP: usize> Drop for FixedVec<T, CAP> {
    fn drop(&mut self) {
        // dtor
        self.clear();
        unsafe {
            // UNSAFE(@ohsayan): dealloc
            unsafe_apis::dealloc_array(self.p.as_ptr(), self.len());
        }
    }
}

impl<T, const CAP: usize> FixedVec<T, CAP> {
    unsafe fn cptr(&self) -> *mut T {
        self.p.as_ptr().add(self.l)
    }
    fn slice(&self) -> &[T] {
        unsafe {
            // UNSAFE(@ohsayan): correct ptrs and len based on push impl and clear impls
            slice::from_raw_parts(self.p.as_ptr(), self.l)
        }
    }
    fn slice_mut(&mut self) -> &mut [T] {
        unsafe {
            // UNSAFE(@ohsayan): correct ptrs and len based on push impl and clear impls
            slice::from_raw_parts_mut(self.p.as_ptr(), self.l)
        }
    }
}

impl<T: PartialEq, A, const CAP: usize> PartialEq<A> for FixedVec<T, CAP>
where
    A: ops::Deref<Target = [T]>,
{
    fn eq(&self, other: &A) -> bool {
        self.slice() == ops::Deref::deref(other)
    }
}

impl<T, const CAP: usize> ops::Deref for FixedVec<T, CAP> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.slice()
    }
}

impl<T, const CAP: usize> ops::DerefMut for FixedVec<T, CAP> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slice_mut()
    }
}

impl<T: fmt::Debug, const CAP: usize> fmt::Debug for FixedVec<T, CAP> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.slice()).finish()
    }
}

#[test]
fn empty() {
    let x = FixedVec::<String, 100>::allocate();
    drop(x);
}

#[test]
fn push_clear() {
    let mut x: FixedVec<_, 100> = FixedVec::allocate();
    for v in 0..50 {
        x.try_push(format!("{v}"));
    }
    assert_eq!(
        x,
        (0..50)
            .into_iter()
            .map(|v| format!("{v}"))
            .collect::<Vec<String>>()
    );
    assert_eq!(x.len(), 50);
}

#[test]
fn clear_range() {
    let mut x: FixedVec<_, 100> = FixedVec::allocate();
    for v in 0..100 {
        x.try_push(format!("{v}"));
    }
    assert_eq!(x.len(), 100);
    unsafe { x.clear_start(50) }
    assert_eq!(
        x,
        (50..100)
            .into_iter()
            .map(|i| ToString::to_string(&i))
            .collect::<Vec<String>>()
    );
    assert_eq!(x.len(), 50);
}
