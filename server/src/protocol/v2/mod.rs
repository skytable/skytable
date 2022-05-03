/*
 * Created on Fri Apr 29 2022
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

mod interface_impls;

use {
    super::{
        raw_parser::{RawParser, RawParserExt, RawParserMeta},
        ParseError, ParseResult, PipelinedQuery, Query, SimpleQuery, UnsafeSlice,
    },
    crate::{corestore::heap_array::HeapArray, dbnet::connection::QueryWithAdvance},
};

#[cfg(feature = "nightly")]
mod benches;
#[cfg(test)]
mod tests;

/// A parser for Skyhash 2.0
pub struct Parser {
    end: *const u8,
    cursor: *const u8,
}

unsafe impl RawParser for Parser {
    fn cursor_ptr(&self) -> *const u8 {
        self.cursor
    }
    fn cursor_ptr_mut(&mut self) -> &mut *const u8 {
        &mut self.cursor
    }
    fn data_end_ptr(&self) -> *const u8 {
        self.end
    }
}

unsafe impl Sync for Parser {}
unsafe impl Send for Parser {}

impl Parser {
    /// Initialize a new parser
    fn new(slice: &[u8]) -> Self {
        unsafe {
            Self {
                end: slice.as_ptr().add(slice.len()),
                cursor: slice.as_ptr(),
            }
        }
    }
}

// query impls
impl Parser {
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
        Ok(SimpleQuery::new(self._next_simple_query()?))
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
    fn _parse(&mut self) -> ParseResult<Query> {
        if self.not_exhausted() {
            unsafe {
                let first_byte = self.get_byte_at_cursor();
                self.incr_cursor();
                let data = match first_byte {
                    b'*' => {
                        // a simple query
                        Query::Simple(self.next_simple_query()?)
                    }
                    b'$' => {
                        // a pipelined query
                        Query::Pipelined(self.next_pipeline()?)
                    }
                    _ => return Err(ParseError::UnexpectedByte),
                };
                Ok(data)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    // only expose this. don't expose Self::new since that'll be _relatively easier_ to
    // invalidate invariants for
    pub fn parse(buf: &[u8]) -> ParseResult<QueryWithAdvance> {
        let mut slf = Self::new(buf);
        let body = slf._parse()?;
        let consumed = slf.cursor_ptr() as usize - buf.as_ptr() as usize;
        Ok((body, consumed))
    }
}
