/*
 * Created on Sat Aug 21 2021
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

use crate::corestore::SharedSlice;

use {
    super::UnsafeSlice,
    core::{hint::unreachable_unchecked, iter::FusedIterator, ops::Deref, slice::Iter},
};

/// An iterator over an [`AnyArray`] (an [`UnsafeSlice`]). The validity of the iterator is
/// left to the caller who has to guarantee:
/// - Source pointers for the unsafe slice are valid
/// - Source pointers exist as long as this iterator is used
pub struct AnyArrayIter<'a> {
    iter: Iter<'a, UnsafeSlice>,
}

/// Same as [`AnyArrayIter`] with the exception that it directly dereferences to the actual
/// slice iterator
pub struct BorrowedAnyArrayIter<'a> {
    iter: Iter<'a, UnsafeSlice>,
}

impl<'a> Deref for BorrowedAnyArrayIter<'a> {
    type Target = Iter<'a, UnsafeSlice>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}

impl<'a> AnyArrayIter<'a> {
    /// Create a new `AnyArrayIter`.
    ///
    /// ## Safety
    /// - Valid source pointers
    /// - Source pointers exist as long as the iterator is used
    #[inline(always)]
    pub const unsafe fn new(iter: Iter<'a, UnsafeSlice>) -> AnyArrayIter<'a> {
        Self { iter }
    }
    /// Check if the iter is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        ExactSizeIterator::len(self) == 0
    }
    /// Returns a borrowed iterator => simply put, advancing the returned iterator does not
    /// affect the base iterator owned by this object
    #[inline(always)]
    pub fn as_ref(&'a self) -> BorrowedAnyArrayIter<'a> {
        BorrowedAnyArrayIter {
            iter: self.iter.as_ref().iter(),
        }
    }
    /// Returns the starting ptr of the `AnyArray`
    #[inline(always)]
    pub unsafe fn as_ptr(&self) -> *const UnsafeSlice {
        self.iter.as_ref().as_ptr()
    }
    /// Returns the next value in uppercase
    #[inline(always)]
    pub fn next_uppercase(&mut self) -> Option<Box<[u8]>> {
        self.iter.next().map(|v| {
            unsafe {
                // UNSAFE(@ohsayan): The ctor of `Self` allows us to "assume" this is safe
                v.as_slice()
            }
            .to_ascii_uppercase()
            .into_boxed_slice()
        })
    }
    #[inline(always)]
    pub fn next_lowercase(&mut self) -> Option<Box<[u8]>> {
        self.iter.next().map(|v| {
            unsafe {
                // UNSAFE(@ohsayan): The ctor of `Self` allows us to "assume" this is safe
                v.as_slice()
            }
            .to_ascii_lowercase()
            .into_boxed_slice()
        })
    }
    #[inline(always)]
    pub unsafe fn next_lowercase_unchecked(&mut self) -> Box<[u8]> {
        self.next_lowercase().unwrap_or_else(|| impossible!())
    }
    #[inline(always)]
    pub unsafe fn next_uppercase_unchecked(&mut self) -> Box<[u8]> {
        match self.next_uppercase() {
            Some(s) => s,
            None => {
                impossible!()
            }
        }
    }
    #[inline(always)]
    /// Returns the next value without any checks
    pub unsafe fn next_unchecked(&mut self) -> &'a [u8] {
        match self.next() {
            Some(s) => s,
            None => unreachable_unchecked(),
        }
    }
    #[inline(always)]
    /// Returns the next value without any checks as an owned copy of [`Bytes`]
    pub unsafe fn next_unchecked_bytes(&mut self) -> SharedSlice {
        SharedSlice::new(self.next_unchecked())
    }
    #[inline(always)]
    pub fn map_next<T>(&mut self, cls: fn(&[u8]) -> T) -> Option<T> {
        self.next().map(cls)
    }
    #[inline(always)]
    pub fn next_string_owned(&mut self) -> Option<String> {
        self.map_next(|v| String::from_utf8_lossy(v).to_string())
    }
    #[inline(always)]
    pub unsafe fn into_inner(self) -> Iter<'a, UnsafeSlice> {
        self.iter
    }
}

/// # Safety
/// Caller must ensure validity of the slice returned
pub unsafe trait DerefUnsafeSlice {
    unsafe fn deref_slice(&self) -> &[u8];
}

unsafe impl DerefUnsafeSlice for UnsafeSlice {
    #[inline(always)]
    unsafe fn deref_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
unsafe impl DerefUnsafeSlice for SharedSlice {
    #[inline(always)]
    unsafe fn deref_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<'a> Iterator for AnyArrayIter<'a> {
    type Item = &'a [u8];
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| unsafe {
            // UNSAFE(@ohsayan): The ctor of `Self` allows us to "assume" this is safe
            v.as_slice()
        })
    }
    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a> DoubleEndedIterator for AnyArrayIter<'a> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iter.next_back().map(|v| unsafe {
            // UNSAFE(@ohsayan): The ctor of `Self` allows us to "assume" this is safe
            v.as_slice()
        })
    }
}

impl<'a> ExactSizeIterator for AnyArrayIter<'a> {}
impl<'a> FusedIterator for AnyArrayIter<'a> {}

impl<'a> Iterator for BorrowedAnyArrayIter<'a> {
    type Item = &'a [u8];
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| unsafe {
            // UNSAFE(@ohsayan): The ctor of `AnyArrayIter` allows us to "assume" this is safe
            v.as_slice()
        })
    }
}

impl<'a> DoubleEndedIterator for BorrowedAnyArrayIter<'a> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iter.next_back().map(|v| unsafe {
            // UNSAFE(@ohsayan): The ctor of `AnyArrayIter` allows us to "assume" this is safe
            v.as_slice()
        })
    }
}

impl<'a> ExactSizeIterator for BorrowedAnyArrayIter<'a> {}
impl<'a> FusedIterator for BorrowedAnyArrayIter<'a> {}
