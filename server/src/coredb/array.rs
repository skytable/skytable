/*
 * Created on Tue Jul 06 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

use core::borrow::Borrow;
use core::borrow::BorrowMut;
use core::cmp::Ordering;
use core::fmt;
use core::hash::Hash;
use core::hash::Hasher;
use core::iter::FromIterator;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::ops;
use core::ptr;
use core::slice;
use serde::{de::SeqAccess, de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

pub struct Array<T, const N: usize> {
    stack: [MaybeUninit<T>; N],
    /// no stack should be more than 16 bytes
    init_len: u16,
}

pub struct LenScopeGuard<'a, T: Copy> {
    real_ref: &'a mut T,
    temp: T,
}

impl<'a, T: ops::AddAssign + Copy> LenScopeGuard<'a, T> {
    pub fn new(real_ref: &'a mut T) -> Self {
        let ret = *real_ref;
        Self {
            real_ref,
            temp: ret,
        }
    }
    pub fn incr(&mut self, val: T) {
        self.temp += val;
    }
    pub fn get_temp(&self) -> T {
        self.temp
    }
}

impl<'a, T: Copy> Drop for LenScopeGuard<'a, T> {
    fn drop(&mut self) {
        *self.real_ref = self.temp;
    }
}

// defy the compiler
struct UninitArray<T, const N: usize>(PhantomData<fn() -> T>);

impl<T, const N: usize> UninitArray<T, N> {
    const VALUE: MaybeUninit<T> = MaybeUninit::uninit();
    const ARRAY: [MaybeUninit<T>; N] = [Self::VALUE; N];
}

impl<T, const N: usize> Array<T, N> {
    pub const fn new() -> Self {
        Array {
            stack: UninitArray::ARRAY,
            init_len: 0,
        }
    }
    pub const fn len(&self) -> usize {
        self.init_len as usize
    }
    pub const fn capacity(&self) -> usize {
        N
    }
    pub const fn is_full(&self) -> bool {
        N == self.len()
    }
    pub const fn remaining_cap(&self) -> usize {
        self.capacity() - self.len()
    }
    pub unsafe fn set_len(&mut self, len: usize) {
        self.init_len = len as u16; // lossy cast, we maintain all invariants
    }
    unsafe fn as_mut_ptr(&mut self) -> *mut T {
        self.stack.as_mut_ptr() as *mut _
    }
    unsafe fn as_ptr(&self) -> *const T {
        self.stack.as_ptr() as *const _
    }
    pub unsafe fn push_unchecked(&mut self, element: T) {
        let len = self.len();
        ptr::write(self.as_mut_ptr().add(len), element);
        self.set_len(len + 1);
    }
    pub fn push(&mut self, element: T) -> Result<(), ()> {
        if self.capacity() < self.len() {
            // so we can push it in
            unsafe { self.push_unchecked(element) };
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn push_panic(&mut self, element: T) {
        self.push(element).unwrap();
    }
    pub fn pop(&mut self) -> Option<T> {
        if self.len() == 0 {
            // nothing here
            None
        } else {
            unsafe {
                let new_len = self.len() - 1;
                self.set_len(new_len);
                // len - 1 == offset
                Some(ptr::read(self.as_ptr().add(new_len)))
            }
        }
    }
    pub fn truncate(&mut self, new_len: usize) {
        let len = self.len();
        if new_len < len {
            // we need to drop off a part of the array
            unsafe {
                // drop_in_place will handle the ZST invariant for us
                ptr::drop_in_place(slice::from_raw_parts_mut(
                    self.as_mut_ptr().add(new_len),
                    len - new_len,
                ))
            }
        }
    }
    pub fn clear(&mut self) {
        self.truncate(0)
    }
    pub fn extend_from_slice(&mut self, slice: &[T]) -> Result<(), ()>
    where
        T: Copy,
    {
        if self.remaining_cap() < slice.len() {
            // no more space here
            return Err(());
        }
        let self_len = self.len();
        let other_len = slice.len();
        unsafe {
            ptr::copy_nonoverlapping(slice.as_ptr(), self.as_mut_ptr().add(self_len), other_len);
            self.set_len(self_len + other_len);
        }
        Ok(())
    }
    pub fn into_array(self) -> Result<[T; N], Self> {
        if self.len() < self.capacity() {
            // not fully initialized
            Err(self)
        } else {
            unsafe {
                Ok({
                    // make sure we don't do a double free or end up deleting the elements
                    let _self = ManuallyDrop::new(self);
                    ptr::read(_self.as_ptr() as *const [T; N])
                })
            }
        }
    }
    // these operations are incredibly safe because we only pass the initialized part
    // of the array
    fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
    fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
}

impl<T, const N: usize> ops::Deref for Array<T, N> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const N: usize> ops::DerefMut for Array<T, N> {
    fn deref_mut(&mut self) -> &mut [T] {
        self.as_slice_mut()
    }
}

impl<T, const N: usize> From<[T; N]> for Array<T, N> {
    fn from(array: [T; N]) -> Self {
        // do not double-free or destroy the elements
        let array = ManuallyDrop::new(array);
        let mut arr = Array::<T, N>::new();
        unsafe {
            // copy it over
            let ptr = &*array as *const [T; N] as *const [MaybeUninit<T>; N];
            ptr.copy_to_nonoverlapping(&mut arr.stack as *mut [MaybeUninit<T>; N], 1);
            arr.set_len(N);
        }
        arr
    }
}

impl<T, const N: usize> Drop for Array<T, N> {
    fn drop(&mut self) {
        self.clear()
    }
}

pub struct ArrayIntoIter<T, const N: usize> {
    state: usize,
    a: Array<T, N>,
}

impl<T, const N: usize> Iterator for ArrayIntoIter<T, N> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.state == self.a.len() {
            // reached end
            None
        } else {
            let idx = self.state;
            self.state += 1;
            Some(unsafe { ptr::read(self.a.as_ptr().add(idx)) })
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = self.a.len() - self.state;
        (l, Some(l))
    }
}

impl<T, const N: usize> IntoIterator for Array<T, N> {
    type Item = T;
    type IntoIter = ArrayIntoIter<T, N>;
    fn into_iter(self) -> Self::IntoIter {
        ArrayIntoIter { state: 0, a: self }
    }
}

impl<T, const N: usize> Array<T, N> {
    pub unsafe fn extend_from_iter_unchecked<I>(&mut self, iterable: I)
    where
        I: IntoIterator<Item = T>,
    {
        // the ptr to start writing from
        let mut ptr = Self::as_mut_ptr(self).add(self.len());
        let mut guard = LenScopeGuard::new(&mut self.init_len);
        let mut iter = iterable.into_iter();
        loop {
            if let Some(element) = iter.next() {
                // write the element
                ptr.write(element);
                // move to the next location
                ptr = ptr.add(1);
                // tell the guard to increment
                guard.incr(1);
            } else {
                return;
            }
        }
    }
}

impl<T, const N: usize> Extend<T> for Array<T, N> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        unsafe { self.extend_from_iter_unchecked(iter) }
    }
}

impl<T, const N: usize> FromIterator<T> for Array<T, N> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut arr = Array::new();
        arr.extend(iter);
        arr
    }
}

impl<T, const N: usize> Clone for Array<T, N>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        self.iter().cloned().collect()
    }
}

impl<T, const N: usize> Hash for Array<T, N>
where
    T: Hash,
{
    fn hash<H>(&self, hasher: &mut H)
    where
        H: Hasher,
    {
        Hash::hash(&**self, hasher)
    }
}

impl<T, const N: usize> PartialEq for Array<T, N>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T, const N: usize> Eq for Array<T, N> where T: Eq {}

impl<T, const N: usize> PartialOrd for Array<T, N>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl<T, const N: usize> Ord for Array<T, N>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<T, const CAP: usize> Borrow<[T]> for Array<T, CAP> {
    fn borrow(&self) -> &[T] {
        self
    }
}

impl<T, const CAP: usize> BorrowMut<[T]> for Array<T, CAP> {
    fn borrow_mut(&mut self) -> &mut [T] {
        self
    }
}
impl<T, const CAP: usize> AsRef<[T]> for Array<T, CAP> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T, const CAP: usize> AsMut<[T]> for Array<T, CAP> {
    fn as_mut(&mut self) -> &mut [T] {
        self
    }
}

impl<T, const CAP: usize> fmt::Debug for Array<T, CAP>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl<T, const N: usize> Serialize for Array<T, N>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

struct ArrayVisitor<T, const N: usize> {
    _marker: PhantomData<Array<T, N>>,
}

impl<'de, T, const N: usize> Visitor<'de> for ArrayVisitor<T, N>
where
    T: Deserialize<'de>,
{
    type Value = Array<T, N>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence")
    }
    fn visit_seq<B>(self, mut seq: B) -> Result<Self::Value, B::Error>
    where
        B: SeqAccess<'de>,
    {
        let len = seq.size_hint().unwrap_or(0);
        if len > N {
            return Err(serde::de::Error::custom("Bad length"));
        }
        let mut array = Array::new();
        while let Some(item) = seq.next_element()? {
            unsafe {
                // UNSAFE(@ohsayan): This is completely safe because we have checked len
                array.push_unchecked(item)
            }
        }
        Ok(array)
    }
}

impl<'de, T, const N: usize> Deserialize<'de> for Array<T, N>
where
    T: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(ArrayVisitor {
            _marker: PhantomData,
        })
    }
}

#[test]
fn test_basic() {
    let mut b: Array<u8, 11> = Array::new();
    b.extend_from_slice("Hello World".as_bytes()).unwrap();
    assert_eq!(
        b,
        Array::from([b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd'])
    );
}
