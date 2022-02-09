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

#[cfg(test)]
use super::element::UnsafeElement;
use super::UnsafeSlice;
use bytes::Bytes;
use core::hint::unreachable_unchecked;
use core::iter::FusedIterator;
use core::ops::Deref;
use core::slice::ChunksExact;
use core::slice::Iter;

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
    pub const unsafe fn new(iter: Iter<'a, UnsafeSlice>) -> AnyArrayIter<'a> {
        Self { iter }
    }
    /// Returns a [`ChunksExact`] (similar to [`ChunksExact` provided by core::slice](core::slice::ChunksExact))
    pub fn chunks_exact(&'a self, chunks_exact: usize) -> ChunksExact<'a, UnsafeSlice> {
        self.iter.as_ref().chunks_exact(chunks_exact)
    }
    /// Check if the iter is empty
    pub fn is_empty(&self) -> bool {
        ExactSizeIterator::len(self) == 0
    }
    /// Returns a borrowed iterator => simply put, advancing the returned iterator does not
    /// affect the base iterator owned by this object
    pub fn as_ref(&'a self) -> BorrowedAnyArrayIter<'a> {
        BorrowedAnyArrayIter {
            iter: self.iter.as_ref().iter(),
        }
    }
    /// Returns the starting ptr of the `AnyArray`
    pub unsafe fn as_ptr(&self) -> *const UnsafeSlice {
        self.iter.as_ref().as_ptr()
    }
    /// Returns the next value in uppercase
    pub fn next_uppercase(&mut self) -> Option<Box<[u8]>> {
        self.iter.next().map(|v| unsafe {
            // SAFETY: Only construction is unsafe, forwarding is not
            v.as_slice().to_ascii_uppercase().into_boxed_slice()
        })
    }
    pub unsafe fn next_uppercase_unchecked(&mut self) -> Box<[u8]> {
        match self.next_uppercase() {
            Some(s) => s,
            None => {
                impossible!()
            }
        }
    }
    /// Returns the next value without any checks
    pub unsafe fn next_unchecked(&mut self) -> &'a [u8] {
        match self.next() {
            Some(s) => s,
            None => unreachable_unchecked(),
        }
    }
    /// Returns the next value without any checks as an owned copy of [`Bytes`]
    pub unsafe fn next_unchecked_bytes(&mut self) -> Bytes {
        Bytes::copy_from_slice(self.next_unchecked())
    }
    pub fn map_next<T>(&mut self, cls: fn(&[u8]) -> T) -> Option<T> {
        self.next().map(|v| cls(v))
    }
    pub fn next_string_owned(&mut self) -> Option<String> {
        self.map_next(|v| String::from_utf8_lossy(&v).to_string())
    }
}

impl<'a> Iterator for AnyArrayIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| unsafe { v.as_slice() })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a> DoubleEndedIterator for AnyArrayIter<'a> {
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iter.next_back().map(|v| unsafe { v.as_slice() })
    }
}

impl<'a> ExactSizeIterator for AnyArrayIter<'a> {}
impl<'a> FusedIterator for AnyArrayIter<'a> {}

impl<'a> Iterator for BorrowedAnyArrayIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| unsafe { v.as_slice() })
    }
}

impl<'a> DoubleEndedIterator for BorrowedAnyArrayIter<'a> {
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        self.iter.next_back().map(|v| unsafe { v.as_slice() })
    }
}

impl<'a> ExactSizeIterator for BorrowedAnyArrayIter<'a> {}
impl<'a> FusedIterator for BorrowedAnyArrayIter<'a> {}

#[test]
fn test_iter() {
    use super::{Parser, Query};
    let (q, _fwby) = Parser::new(b"*1\n~3\n3\nset\n1\nx\n3\n100\n")
        .parse()
        .unwrap();
    let r = match q {
        Query::SimpleQuery(q) => q,
        _ => panic!("Wrong query"),
    };
    let arr = unsafe {
        match r.into_inner() {
            UnsafeElement::AnyArray(arr) => arr,
            _ => panic!("Wrong type"),
        }
    };
    let it = arr.iter();
    let mut iter = unsafe { AnyArrayIter::new(it) };
    assert_eq!(iter.next_uppercase().unwrap().as_ref(), "SET".as_bytes());
    assert_eq!(iter.next().unwrap(), "x".as_bytes());
    assert_eq!(iter.next().unwrap(), "100".as_bytes());
}
