/*
 * Created on Tue Apr 12 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

#![allow(unused)] // TODO(@ohsayan): Remove this once we're done

use crate::corestore::heap_array::HeapArray;
use crate::protocol::{ParseError, ParseResult, UnsafeSlice};
use core::{marker::PhantomData, mem::transmute};
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Query {
    forward: usize,
    data: QueryType,
}

impl Query {
    const fn new(forward: usize, data: QueryType) -> Self {
        Self { forward, data }
    }
}

#[derive(Debug)]
pub enum QueryType {
    Simple(SimpleQuery),
    Pipelined(PipelinedQuery),
}

#[derive(Debug)]
pub struct SimpleQuery {
    data: HeapArray<UnsafeSlice>,
}

impl SimpleQuery {
    #[cfg(test)]
    fn into_owned(self) -> OwnedSimpleQuery {
        OwnedSimpleQuery {
            data: self
                .data
                .iter()
                .map(|v| unsafe { v.as_slice().to_owned() })
                .collect(),
        }
    }
}

#[cfg(test)]
struct OwnedSimpleQuery {
    data: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub struct PipelinedQuery {
    data: HeapArray<HeapArray<UnsafeSlice>>,
}

impl PipelinedQuery {
    #[cfg(test)]
    fn into_owned(self) -> OwnedPipelinedQuery {
        OwnedPipelinedQuery {
            data: self
                .data
                .iter()
                .map(|v| {
                    v.iter()
                        .map(|v| unsafe { v.as_slice().to_owned() })
                        .collect()
                })
                .collect(),
        }
    }
}

#[cfg(test)]
struct OwnedPipelinedQuery {
    data: Vec<Vec<Vec<u8>>>,
}

/// A parser for Skyhash 2.0
pub struct Parser<'a> {
    end: *const u8,
    cursor: *const u8,
    _lt: PhantomData<&'a ()>,
}

impl<'a> Parser<'a> {
    /// Initialize a new parser
    pub fn new(slice: &[u8]) -> Self {
        unsafe {
            Self {
                end: slice.as_ptr().add(slice.len()),
                cursor: slice.as_ptr(),
                _lt: PhantomData,
            }
        }
    }
}

// basic methods
impl<'a> Parser<'a> {
    /// Returns a ptr one byte past the allocation of the buffer
    const fn data_end_ptr(&self) -> *const u8 {
        self.end
    }
    /// Returns the position of the cursor
    /// WARNING: Deref might led to a segfault
    const fn cursor_ptr(&self) -> *const u8 {
        self.cursor
    }
    /// Check how many bytes we have left
    fn remaining(&self) -> usize {
        self.data_end_ptr() as usize - self.cursor_ptr() as usize
    }
    /// Check if we have `size` bytes remaining
    fn has_remaining(&self, size: usize) -> bool {
        self.remaining() >= size
    }
    /// Check if we have exhausted the buffer
    fn exhausted(&self) -> bool {
        self.cursor_ptr() >= self.data_end_ptr()
    }
    /// Check if the buffer is not exhausted
    fn not_exhausted(&self) -> bool {
        self.cursor_ptr() < self.data_end_ptr()
    }
    /// Attempts to return the byte pointed at by the cursor.
    /// WARNING: The same segfault warning
    const unsafe fn get_byte_at_cursor(&self) -> u8 {
        *self.cursor_ptr()
    }
}

// mutable refs
impl<'a> Parser<'a> {
    /// Increment the cursor by `by` positions
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        self.cursor = self.cursor.add(by);
    }
    /// Increment the position of the cursor by one position
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1);
    }
}

// higher level abstractions
impl<'a> Parser<'a> {
    /// Attempt to read `len` bytes
    fn read_until(&mut self, len: usize) -> ParseResult<UnsafeSlice> {
        if self.has_remaining(len) {
            unsafe {
                // UNSAFE(@ohsayan): Already verified lengths
                let slice = UnsafeSlice::new(self.cursor_ptr(), len);
                self.incr_cursor_by(len);
                Ok(slice)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// Attempt to read a byte slice terminated by an LF
    fn read_line(&mut self) -> ParseResult<UnsafeSlice> {
        let start_ptr = self.cursor_ptr();
        unsafe {
            while self.not_exhausted() && self.get_byte_at_cursor() != b'\n' {
                self.incr_cursor();
            }
            if self.not_exhausted() && self.get_byte_at_cursor() == b'\n' {
                let len = self.cursor_ptr() as usize - start_ptr as usize;
                self.incr_cursor(); // skip LF
                Ok(UnsafeSlice::new(start_ptr, len))
            } else {
                Err(ParseError::NotEnough)
            }
        }
    }
    /// Attempt to read a line, **rejecting an empty payload**
    fn read_line_pedantic(&mut self) -> ParseResult<UnsafeSlice> {
        let start_ptr = self.cursor_ptr();
        unsafe {
            while self.not_exhausted() && self.get_byte_at_cursor() != b'\n' {
                self.incr_cursor();
            }
            let len = self.cursor_ptr() as usize - start_ptr as usize;
            let has_lf = self.not_exhausted() && self.get_byte_at_cursor() == b'\n';
            if has_lf && len != 0 {
                self.incr_cursor(); // skip LF
                Ok(UnsafeSlice::new(start_ptr, len))
            } else {
                // just some silly hackery
                Err(transmute(has_lf))
            }
        }
    }
    /// Attempt to read an `usize` from the buffer
    fn read_usize(&mut self) -> ParseResult<usize> {
        let line = self.read_line_pedantic()?;
        let bytes = unsafe {
            // UNSAFE(@ohsayan): We just extracted the slice
            line.as_slice()
        };
        let mut ret = 0usize;
        for byte in bytes {
            if byte.is_ascii_digit() {
                ret = match ret.checked_mul(10) {
                    Some(r) => r,
                    None => return Err(ParseError::DatatypeParseFailure),
                };
                ret = match ret.checked_add((byte & 0x0F) as _) {
                    Some(r) => r,
                    None => return Err(ParseError::DatatypeParseFailure),
                };
            } else {
                return Err(ParseError::DatatypeParseFailure);
            }
        }
        Ok(ret)
    }
}

// query impls
impl<'a> Parser<'a> {
    /// Parse the next simple query. This should have passed the `*` tsymbol
    ///
    /// Simple query structure (tokenized line-by-line):
    /// ```text
    /// *      -> Simple Query Header
    /// <n>\n  -> Count of elements in the simple query
    /// <l0>\n -> Length of element 1
    /// <e0>   -> element 1 itself
    /// <l1>\n -> Length of element 2
    /// <e1>   -> element 2 itself
    /// ...
    /// ```
    fn _next_simple_query(&mut self) -> ParseResult<HeapArray<UnsafeSlice>> {
        let element_count = self.read_usize()?;
        unsafe {
            let mut data = HeapArray::new_writer(element_count);
            for i in 0..element_count {
                let element_size = self.read_usize()?;
                let element = self.read_until(element_size)?;
                data.write_to_index(i, element);
            }
            Ok(data.finish())
        }
    }
    /// Parse a simple query
    fn next_simple_query(&mut self) -> ParseResult<SimpleQuery> {
        Ok(SimpleQuery {
            data: self._next_simple_query()?,
        })
    }
    /// Parse a pipelined query. This should have passed the `$` tsymbol
    ///
    /// Pipelined query structure (tokenized line-by-line):
    /// ```text
    /// $          -> Pipeline
    /// <n>\n      -> Pipeline has n queries
    /// <lq0>\n    -> Query 1 has 3 elements
    /// <lq0e0>\n  -> Q1E1 has 3 bytes
    /// <q0e0>     -> Q1E1 itself
    /// <lq0e1>\n  -> Q1E2 has 1 byte
    /// <q0e1>     -> Q1E2 itself
    /// <lq0e2>\n  -> Q1E3 has 3 bytes
    /// <q0e2>     -> Q1E3 itself
    /// <lq1>\n    -> Query 2 has 2 elements
    /// <lq1e0>\n  -> Q2E1 has 3 bytes
    /// <q1e0>     -> Q2E1 itself
    /// <lq1e1>\n  -> Q2E2 has 1 byte
    /// <q1e1>     -> Q2E2 itself
    /// ...
    /// ```
    ///
    /// Example:
    /// ```text
    /// $    -> Pipeline
    /// 2\n  -> Pipeline has 2 queries
    /// 3\n  -> Query 1 has 3 elements
    /// 3\n  -> Q1E1 has 3 bytes
    /// SET  -> Q1E1 itself
    /// 1\n  -> Q1E2 has 1 byte
    /// x    -> Q1E2 itself
    /// 3\n  -> Q1E3 has 3 bytes
    /// 100  -> Q1E3 itself
    /// 2\n  -> Query 2 has 2 elements
    /// 3\n  -> Q2E1 has 3 bytes
    /// GET  -> Q2E1 itself
    /// 1\n  -> Q2E2 has 1 byte
    /// x    -> Q2E2 itself
    /// ```
    fn next_pipeline(&mut self) -> ParseResult<PipelinedQuery> {
        let query_count = self.read_usize()?;
        unsafe {
            let mut queries = HeapArray::new_writer(query_count);
            for i in 0..query_count {
                let sq = self._next_simple_query()?;
                queries.write_to_index(i, sq);
            }
            Ok(PipelinedQuery {
                data: queries.finish(),
            })
        }
    }
    fn _parse(&mut self) -> ParseResult<QueryType> {
        if self.not_exhausted() {
            unsafe {
                let first_byte = self.get_byte_at_cursor();
                self.incr_cursor();
                let data = match first_byte {
                    b'*' => {
                        // a simple query
                        QueryType::Simple(self.next_simple_query()?)
                    }
                    b'$' => {
                        // a pipelined query
                        QueryType::Pipelined(self.next_pipeline()?)
                    }
                    _ => return Err(ParseError::UnexpectedByte),
                };
                Ok(data)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    pub fn parse(buf: &[u8]) -> ParseResult<Query> {
        let mut slf = Self::new(buf);
        let body = slf._parse()?;
        let consumed = slf.cursor_ptr() as usize - buf.as_ptr() as usize;
        Ok(Query::new(consumed, body))
    }
}
