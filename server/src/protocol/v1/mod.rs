/*
 * Created on Sat Apr 30 2022
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

use {
    super::{
        raw_parser::{RawParser, RawParserExt, RawParserMeta},
        ParseError, ParseResult, PipelinedQuery, Query, SimpleQuery, UnsafeSlice,
    },
    crate::{
        corestore::heap_array::{HeapArray, HeapArrayWriter},
        dbnet::QueryWithAdvance,
    },
};

mod interface_impls;
// test and bench modules
#[cfg(feature = "nightly")]
mod benches;
#[cfg(test)]
mod tests;

/// A parser for Skyhash 1.0
///
/// Packet structure example (simple query):
/// ```text
/// *1\n
/// ~3\n
/// 3\n
/// SET\n
/// 1\n
/// x\n
/// 3\n
/// 100\n
/// ```
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

unsafe impl Send for Parser {}
unsafe impl Sync for Parser {}

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

// utility methods
impl Parser {
    /// Returns true if the cursor will give a char, but if `this_if_nothing_ahead` is set
    /// to true, then if no byte is ahead, it will still return true
    fn will_cursor_give_char(&self, ch: u8, true_if_nothing_ahead: bool) -> ParseResult<bool> {
        if self.exhausted() {
            // nothing left
            if true_if_nothing_ahead {
                Ok(true)
            } else {
                Err(ParseError::NotEnough)
            }
        } else if unsafe { self.get_byte_at_cursor().eq(&ch) } {
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Check if the current cursor will give an LF
    fn will_cursor_give_linefeed(&self) -> ParseResult<bool> {
        self.will_cursor_give_char(b'\n', false)
    }
    /// Gets the _next element. **The cursor should be at the tsymbol (passed)**
    fn _next(&mut self) -> ParseResult<UnsafeSlice> {
        let element_size = self.read_usize()?;
        self.read_until(element_size)
    }
}

// higher level abstractions
impl Parser {
    /// Parse the next blob. **The cursor should be at the tsymbol (passed)**
    fn parse_next_blob(&mut self) -> ParseResult<UnsafeSlice> {
        {
            let chunk = self._next()?;
            if self.will_cursor_give_linefeed()? {
                unsafe {
                    // UNSAFE(@ohsayan): We know that the buffer is not exhausted
                    // due to the above condition
                    self.incr_cursor();
                }
                Ok(chunk)
            } else {
                Err(ParseError::UnexpectedByte)
            }
        }
    }
}

// query abstractions
impl Parser {
    /// The buffer should resemble the below structure:
    /// ```
    /// ~<count>\n
    /// <e0l0>\n
    /// <e0>\n
    /// <e1l1>\n
    /// <e1>\n
    /// ...
    /// ```
    fn _parse_simple_query(&mut self) -> ParseResult<HeapArray<UnsafeSlice>> {
        if self.not_exhausted() {
            if unsafe { self.get_byte_at_cursor() } != b'~' {
                // we need an any array
                return Err(ParseError::WrongType);
            }
            unsafe {
                // UNSAFE(@ohsayan): Just checked length
                self.incr_cursor();
            }
            let query_count = self.read_usize()?;
            let mut writer = HeapArrayWriter::with_capacity(query_count);
            for i in 0..query_count {
                unsafe {
                    // UNSAFE(@ohsayan): The index of the for loop ensures that
                    // we never attempt to write to a bad memory location
                    writer.write_to_index(i, self.parse_next_blob()?);
                }
            }
            Ok(unsafe {
                // UNSAFE(@ohsayan): If we've reached here, then we have initialized
                // all the queries
                writer.finish()
            })
        } else {
            Err(ParseError::NotEnough)
        }
    }
    fn parse_simple_query(&mut self) -> ParseResult<SimpleQuery> {
        Ok(SimpleQuery::new(self._parse_simple_query()?))
    }
    /// The buffer should resemble the following structure:
    /// ```text
    /// # query 1
    /// ~<count>\n
    /// <e0l0>\n
    /// <e0>\n
    /// <e1l1>\n
    /// <e1>\n
    /// # query 2
    /// ~<count>\n
    /// <e0l0>\n
    /// <e0>\n
    /// <e1l1>\n
    /// <e1>\n
    /// ...
    /// ```
    fn parse_pipelined_query(&mut self, length: usize) -> ParseResult<PipelinedQuery> {
        let mut writer = HeapArrayWriter::with_capacity(length);
        for i in 0..length {
            unsafe {
                // UNSAFE(@ohsayan): The above condition guarantees that the index
                // never causes an overflow
                writer.write_to_index(i, self._parse_simple_query()?);
            }
        }
        unsafe {
            // UNSAFE(@ohsayan): if we reached here, then we have inited everything
            Ok(PipelinedQuery::new(writer.finish()))
        }
    }
    fn _parse(&mut self) -> ParseResult<Query> {
        if self.not_exhausted() {
            let first_byte = unsafe {
                // UNSAFE(@ohsayan): Just checked if buffer is exhausted or not
                self.get_byte_at_cursor()
            };
            if first_byte != b'*' {
                // unknown query scheme, so it's a bad packet
                return Err(ParseError::BadPacket);
            }
            unsafe {
                // UNSAFE(@ohsayan): Checked buffer len and incremented, so we're good
                self.incr_cursor()
            };
            let query_count = self.read_usize()?; // get the length
            if query_count == 1 {
                Ok(Query::Simple(self.parse_simple_query()?))
            } else {
                Ok(Query::Pipelined(self.parse_pipelined_query(query_count)?))
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    pub fn parse(buf: &[u8]) -> ParseResult<QueryWithAdvance> {
        let mut slf = Self::new(buf);
        let body = slf._parse()?;
        let consumed = slf.cursor_ptr() as usize - buf.as_ptr() as usize;
        Ok((body, consumed))
    }
}
