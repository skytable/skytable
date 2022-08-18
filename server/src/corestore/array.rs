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

use core::{
    any,
    borrow::{Borrow, BorrowMut},
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    iter::FromIterator,
    mem::{ManuallyDrop, MaybeUninit},
    ops, ptr, slice, str,
};

/// A compile-time, fixed size array that can have unintialized memory. This array is as
/// efficient as you'd expect a normal array to be, but with the added benefit that you
/// don't have to initialize all the elements. This was inspired by the arrayvec crate.
/// Safe abstractions are made available enabling us to not enter uninitialized space and
/// read the _available_ elements. The array size is limited to 16 bits or 2 bytes to
/// prevent stack overflows.
///
/// ## Panics
/// To avoid stack corruption among other crazy things, several implementations like [`Extend`]
/// can panic. There are _silently corrupting_ methods too which can be used if you can uphold
/// the guarantees
pub struct Array<T, const N: usize> {
    /// the maybe bad stack
    stack: [MaybeUninit<T>; N],
    /// the initialized length
    /// no stack should be more than 16 bytes
    init_len: u16,
}

/// The len scopeguard is like a scopeguard that provides panic safety incase an append-like
/// operation involving iterators causes the iterator to panic. This makes sure that we still
/// set the len on panic
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

macro_rules! impl_zeroed_nm {
    ($($ty:ty),* $(,)?) => {
        $(
            impl<const N: usize> Array<$ty, N> {
                pub const fn new_zeroed() -> Self {
                    Self {
                        stack: [MaybeUninit::new(0); N],
                        init_len: N as u16,
                    }
                }
            }
        )*
    };
}

impl_zeroed_nm! {
    u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, usize, isize
}

impl<T, const N: usize> Array<T, N> {
    // just some silly hackery here because uninit_array isn't stabilized -- move on
    const VALUE: MaybeUninit<T> = MaybeUninit::uninit();
    const ARRAY: [MaybeUninit<T>; N] = [Self::VALUE; N];
    /// Create a new array
    pub const fn new() -> Self {
        Array {
            stack: Self::ARRAY,
            init_len: 0,
        }
    }
    /// This is very safe from the ctor point of view, but the correctness of `init_len`
    /// may be a bad assumption and might make us read garbage
    pub const unsafe fn from_const(array: [MaybeUninit<T>; N], init_len: u16) -> Self {
        Self {
            stack: array,
            init_len,
        }
    }
    pub unsafe fn bump_init_len(&mut self, bump: u16) {
        self.init_len += bump
    }
    /// This literally turns [T; M] into [T; N]. How can you expect it to be safe?
    /// This function is extremely unsafe. I mean, I don't even know how to call it safe.
    /// There's one way though: make M == N. This will panic in debug mode if M > N. In
    /// release mode, good luck
    unsafe fn from_const_array<const M: usize>(arr: [T; M]) -> Self {
        debug_assert!(
            N >= M,
            "Provided const array exceeds size limit of initialized array"
        );
        // do not double-free or destroy the elements
        let array = ManuallyDrop::new(arr);
        let mut arr = Array::<T, N>::new();
        // copy it over
        let ptr = &*array as *const [T; M] as *const [MaybeUninit<T>; N];
        ptr.copy_to_nonoverlapping(&mut arr.stack as *mut [MaybeUninit<T>; N], 1);
        arr.set_len(N);
        arr
    }
    /// Get the apparent length of the array
    pub const fn len(&self) -> usize {
        self.init_len as usize
    }
    /// Get the capacity of the array
    pub const fn capacity(&self) -> usize {
        N
    }
    /// Check if the array is full
    pub const fn is_full(&self) -> bool {
        N == self.len()
    }
    /// Get the remaining capacity of the array
    pub const fn remaining_cap(&self) -> usize {
        self.capacity() - self.len()
    }
    /// Set the length of the array
    ///
    /// ## Safety
    /// This is one of those, use to leak memory functions. If you change the length,
    /// you'll be reading random garbage from the memory and doing a double-free on drop
    pub unsafe fn set_len(&mut self, len: usize) {
        self.init_len = len as u16; // lossy cast, we maintain all invariants
    }
    /// Get the array as a mut ptr
    unsafe fn as_mut_ptr(&mut self) -> *mut T {
        self.stack.as_mut_ptr() as *mut _
    }
    /// Get the array as a const ptr
    unsafe fn as_ptr(&self) -> *const T {
        self.stack.as_ptr() as *const _
    }
    /// Push an element into the array **without any bounds checking**.
    ///
    /// ## Safety
    /// This function is **so unsafe** that you possibly don't want to call it, or
    /// even think about calling it. You can end up corrupting your own stack or
    /// other's valuable data
    pub unsafe fn push_unchecked(&mut self, element: T) {
        let len = self.len();
        ptr::write(self.as_mut_ptr().add(len), element);
        self.set_len(len + 1);
    }
    /// This is a nice version of a push that does bound checks
    pub fn push_panic(&mut self, element: T) -> Result<(), ()> {
        if self.len() < N {
            // so we can push it in
            unsafe { self.push_unchecked(element) };
            Ok(())
        } else {
            Err(())
        }
    }
    /// This is a _panicky_ but safer alternative to `push_unchecked` that panics on
    /// incorrect lengths
    pub fn push(&mut self, element: T) {
        self.push_panic(element).unwrap();
    }
    /// Pop an item off the array
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
    /// Truncate the array to a given size. This is super safe and doesn't even panic
    /// if you provide a silly `new_len`.
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
    /// Empty the internal array
    pub fn clear(&mut self) {
        self.truncate(0)
    }
    /// Extend self from a slice
    pub fn extend_from_slice(&mut self, slice: &[T]) -> Result<(), ()>
    where
        T: Copy,
    {
        if self.remaining_cap() < slice.len() {
            // no more space here
            return Err(());
        }
        unsafe {
            self.extend_from_slice_unchecked(slice);
        }
        Ok(())
    }
    /// Extend self from a slice without doing a single check
    ///
    /// ## Safety
    /// This function is just very very and. You can write giant things into your own
    /// stack corrupting it, corrupting other people's things and creating undefined
    /// behavior like no one else.
    pub unsafe fn extend_from_slice_unchecked(&mut self, slice: &[T]) {
        let self_len = self.len();
        let other_len = slice.len();
        ptr::copy_nonoverlapping(slice.as_ptr(), self.as_mut_ptr().add(self_len), other_len);
        self.set_len(self_len + other_len);
    }
    /// Returns self as a `[T; N]` array if it is fully initialized. Else it will again return
    /// itself
    pub fn into_array(self) -> Result<[T; N], Self> {
        if self.len() < self.capacity() {
            // not fully initialized
            Err(self)
        } else {
            unsafe { Ok(self.into_array_unchecked()) }
        }
    }
    pub unsafe fn into_array_unchecked(self) -> [T; N] {
        // make sure we don't do a double free or end up deleting the elements
        let _self = ManuallyDrop::new(self);
        ptr::read(_self.as_ptr() as *const [T; N])
    }
    pub fn try_from_slice(slice: impl AsRef<[T]>) -> Option<Self> {
        let slice = slice.as_ref();
        if slice.len() > N {
            None
        } else {
            Some(unsafe { Self::from_slice(slice) })
        }
    }
    /// Extend self from a slice
    ///
    /// ## Safety
    /// The same danger as in from_slice_unchecked
    pub unsafe fn from_slice(slice_ref: impl AsRef<[T]>) -> Self {
        let mut slf = Self::new();
        slf.extend_from_slice_unchecked(slice_ref.as_ref());
        slf
    }
    // these operations are incredibly safe because we only pass the initialized part
    // of the array
    /// Get self as a slice. Super safe because we guarantee that all the other invarians
    /// are upheld
    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
    /// Get self as a mutable slice. Super safe (see comment above)
    fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
}

impl<const N: usize> Array<u8, N> {
    /// This isn't _unsafe_ but it can cause functions expecting pure unicode to
    /// crash if the array contains invalid unicode
    pub unsafe fn as_str(&self) -> &str {
        str::from_utf8_unchecked(self)
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
        unsafe { Array::from_const_array::<N>(array) }
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
    /// Extend self using an iterator.
    ///
    /// ## Safety
    /// This function can cause undefined damage to your application's stack and/or other's
    /// data. Only use if you know what you're doing. If you don't use `extend_from_iter`
    /// instead
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
    pub fn extend_from_iter<I>(&mut self, iterable: I)
    where
        I: IntoIterator<Item = T>,
    {
        unsafe {
            // the ptr to start writing from
            let mut ptr = Self::as_mut_ptr(self).add(self.len());
            let end_ptr = Self::as_ptr(self).add(self.capacity());
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
                    if end_ptr < ptr {
                        // our current ptr points to the end of the allocation
                        // oh no, time for corruption, if the user says so
                        panic!("Overflowed stack area.")
                    }
                } else {
                    return;
                }
            }
        }
    }
}

impl<T, const N: usize> Extend<T> for Array<T, N> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        {
            self.extend_from_iter::<_>(iter)
        }
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

impl<const N: usize> PartialEq<[u8]> for Array<u8, N> {
    fn eq(&self, oth: &[u8]) -> bool {
        **self == *oth
    }
}

impl<const N: usize> PartialEq<Array<u8, N>> for [u8] {
    fn eq(&self, oth: &Array<u8, N>) -> bool {
        oth.as_slice() == self
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
        if any::type_name::<T>().eq(any::type_name::<u8>()) {
            let slf = unsafe {
                // UNSAFE(@ohsayan): Guaranteed by the above invariant
                &*(self as *const Array<T, CAP> as *const Array<u8, CAP>)
            };
            match String::from_utf8(slf.to_vec()) {
                Ok(st) => write!(f, "{:#?}", st),
                Err(_) => (**self).fmt(f),
            }
        } else {
            (**self).fmt(f)
        }
    }
}

impl<const N: usize> Borrow<str> for Array<u8, N> {
    fn borrow(&self) -> &str {
        unsafe { self.as_str() }
    }
}

unsafe impl<T, const N: usize> Send for Array<T, N> where T: Send {}
unsafe impl<T, const N: usize> Sync for Array<T, N> where T: Sync {}

#[test]
fn test_basic() {
    let mut b: Array<u8, 11> = Array::new();
    b.extend_from_slice("Hello World".as_bytes()).unwrap();
    assert_eq!(
        b,
        Array::from([b'H', b'e', b'l', b'l', b'o', b' ', b'W', b'o', b'r', b'l', b'd'])
    );
}

#[test]
fn test_uninitialized() {
    let mut b: Array<u8, 16> = Array::new();
    b.push(b'S');
    assert_eq!(b.iter().count(), 1);
}

#[test]
#[should_panic]
fn test_array_overflow() {
    let mut arr: Array<u8, 5> = Array::new();
    arr.extend_from_slice("123456".as_bytes()).unwrap();
}

#[test]
#[should_panic]
fn test_array_overflow_iter() {
    let mut arr: Array<char, 5> = Array::new();
    arr.extend("123456".chars());
}

#[test]
fn test_array_clone() {
    let mut arr: Array<u8, 64> = Array::new();
    arr.extend(
        "qHwRsmyBYHbqyHfdShOfVSayVUmeKlEagvJoGuTyvaCqpsfFkZabeuqmVeiKbJxV"
            .as_bytes()
            .to_owned(),
    );
    let myclone = arr.clone();
    assert_eq!(arr, myclone);
}

#[test]
fn test_array_extend_okay() {
    let mut arr: Array<u8, 64> = Array::new();
    arr.extend(
        "qHwRsmyBYHbqyHfdShOfVSayVUmeKlEagvJoGuTyvaCqpsfFkZabeuqmVeiKbJxV"
            .as_bytes()
            .to_owned(),
    );
}

#[test]
#[should_panic]
fn test_array_extend_fail() {
    let mut arr: Array<u8, 64> = Array::new();
    arr.extend(
        "qHwRsmyBYHbqyHfdShOfVSayVUmeKlEagvJoGuTyvaCqpsfFkZabeuqmVeiKbJxV_"
            .as_bytes()
            .to_owned(),
    );
}
