/*
 * Created on Tue Aug 31 2021
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

/*
 UNSAFE(@ohsayan): Everything done here is unsafely safe. We
 reinterpret bits of one type as another. What could be worse?
 nah, it's not that bad. We know that the byte representations
 would be in the way we expect. If the data is corrupted, we
 can guarantee that we won't ever read incorrect lengths of data
 and we won't read into others' memory (or corrupt our own).
*/

use crate::storage::Data;
use core::mem;
use core::ptr;
use core::slice;

const SIZE_64BIT: usize = mem::size_of::<u64>();
const SIZE_128BIT: usize = SIZE_64BIT * 2;

/// This contains the fn ptr to decode bytes wrt to the host's endian. For example, if you're on an LE machine and
/// you're reading data from a BE machine, then simply set the endian to big. This only affects the first read and not
/// subsequent ones (unless you switch between machines of different endian, obviously)
static mut NATIVE_ENDIAN_READER: unsafe fn(*const u8) -> usize = super::de::transmute_len;

/// Use this to set the current endian to LE.
///
/// ## Safety
/// Make sure this is run from a single thread only! If not, good luck
pub(super) unsafe fn endian_set_little() {
    NATIVE_ENDIAN_READER = super::de::transmute_len_le;
}

/// Use this to set the current endian to BE.
///
/// ## Safety
/// Make sure this is run from a single thread only! If not, good luck
pub(super) unsafe fn endian_set_big() {
    NATIVE_ENDIAN_READER = super::de::transmute_len_be;
}

/// A raw slice iterator by using raw pointers
#[derive(Debug)]
pub struct RawSliceIter<'a> {
    _base: &'a [u8],
    cursor: *const u8,
    terminal: *const u8,
}

impl<'a> RawSliceIter<'a> {
    /// Create a new slice iterator
    pub fn new(slice: &'a [u8]) -> Self {
        Self {
            cursor: slice.as_ptr(),
            terminal: unsafe { slice.as_ptr().add(slice.len()) },
            _base: slice,
        }
    }
    /// Check the number of remaining bytes in the buffer
    fn remaining(&self) -> usize {
        unsafe { self.terminal.offset_from(self.cursor) as usize }
    }
    /// Increment the cursor by one
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    /// Check if the buffer was exhausted
    fn exhausted(&self) -> bool {
        self.cursor > self.terminal
    }
    /// Increment the cursor by the provided length
    unsafe fn incr_cursor_by(&mut self, ahead: usize) {
        {
            self.cursor = self.cursor.add(ahead)
        }
    }
    /// Get the next 64-bit integer, casting it to an `usize`, respecting endianness
    pub fn next_64bit_integer_to_usize(&mut self) -> Option<usize> {
        if self.remaining() < 8 {
            // we need 8 bytes to read a 64-bit integer, so nope
            None
        } else {
            unsafe {
                // sweet, something is left
                let l = NATIVE_ENDIAN_READER(self.cursor);
                // now forward the cursor
                self.incr_cursor_by(SIZE_64BIT);
                Some(l)
            }
        }
    }
    /// Get a borrowed slice for the given length. The lifetime is important!
    pub fn next_borrowed_slice(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.remaining() < len {
            None
        } else {
            unsafe {
                let d = slice::from_raw_parts(self.cursor, len);
                self.incr_cursor_by(len);
                Some(d)
            }
        }
    }
    /// Get the next 64-bit usize
    pub fn next_64bit_integer_pair_to_usize(&mut self) -> Option<(usize, usize)> {
        if self.remaining() < SIZE_128BIT {
            None
        } else {
            unsafe {
                let v1 = NATIVE_ENDIAN_READER(self.cursor);
                self.incr_cursor_by(SIZE_64BIT);
                let v2 = NATIVE_ENDIAN_READER(self.cursor);
                self.incr_cursor_by(SIZE_64BIT);
                Some((v1, v2))
            }
        }
    }
    /// Get the next owned [`Data`] with the provided length
    pub fn next_owned_data(&mut self, len: usize) -> Option<Data> {
        if self.remaining() < len {
            // not enough left
            None
        } else {
            // we have something to look at
            unsafe {
                let d = slice::from_raw_parts(self.cursor, len);
                let d = Some(Data::copy_from_slice(d));
                self.incr_cursor_by(len);
                d
            }
        }
    }
    /// Get the next 8-bit unsigned integer
    pub fn next_8bit_integer(&mut self) -> Option<u8> {
        if self.exhausted() {
            None
        } else {
            unsafe {
                let x = ptr::read(self.cursor);
                self.incr_cursor();
                Some(x)
            }
        }
    }
    /// Check if the cursor has reached end-of-allocation
    pub fn end_of_allocation(&self) -> bool {
        self.cursor == self.terminal
    }
    /// Get a borrowed iterator. This is super safe, funny enough, because of the lifetime
    /// bound that we add to the iterator object
    pub fn get_borrowed_iter(&mut self) -> RawSliceIterBorrowed<'_> {
        RawSliceIterBorrowed::new(self.cursor, self.terminal, &mut self.cursor)
    }
}

#[derive(Debug)]
pub struct RawSliceIterBorrowed<'a> {
    cursor: *const u8,
    end_ptr: *const u8,
    mut_ptr: &'a mut *const u8,
}

impl<'a> RawSliceIterBorrowed<'a> {
    fn new(
        cursor: *const u8,
        end_ptr: *const u8,
        mut_ptr: &'a mut *const u8,
    ) -> RawSliceIterBorrowed<'a> {
        Self {
            cursor,
            end_ptr,
            mut_ptr,
        }
    }
    /// Check the number of remaining bytes in the buffer
    fn remaining(&self) -> usize {
        unsafe { self.end_ptr.offset_from(self.cursor) as usize }
    }
    /// Increment the cursor by the provided length
    unsafe fn incr_cursor_by(&mut self, ahead: usize) {
        {
            self.cursor = self.cursor.add(ahead)
        }
    }
    pub fn next_64bit_integer_to_usize(&mut self) -> Option<usize> {
        if self.remaining() < 8 {
            None
        } else {
            unsafe {
                let size = NATIVE_ENDIAN_READER(self.cursor);
                self.incr_cursor_by(SIZE_64BIT);
                Some(size)
            }
        }
    }
    pub fn next_owned_data(&mut self, len: usize) -> Option<Data> {
        if self.remaining() < len {
            None
        } else {
            unsafe {
                let d = slice::from_raw_parts(self.cursor, len);
                let d = Some(Data::copy_from_slice(d));
                self.incr_cursor_by(len);
                d
            }
        }
    }
}

impl<'a> Drop for RawSliceIterBorrowed<'a> {
    fn drop(&mut self) {
        *self.mut_ptr = self.cursor;
    }
}
