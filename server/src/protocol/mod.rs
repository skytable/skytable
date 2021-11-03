/*
 * Created on Mon May 10 2021
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

//! # The Skyhash Protocol
//!
//! ## Introduction
//! The Skyhash Protocol is a serialization protocol that is used by Skytable for client/server communication.
//! It works in a query/response action similar to HTTP's request/response action. Skyhash supersedes the Terrapipe
//! protocol as a more simple, reliable, robust and scalable protocol.
//!
//! This module contains the [`Parser`] for the Skyhash protocol and it's enough to just pass a query packet as
//! a slice of unsigned 8-bit integers and the parser will do everything else. The Skyhash protocol was designed
//! and implemented by the Author (Sayan Nandan)
//!

// modules
pub mod element;
pub mod iter;
pub mod responses;
bench! {
    mod benches;
}
#[cfg(test)]
mod tests;
// endof modules
// test imports
#[cfg(test)]
use self::element::OwnedElement;
// endof test imports
use self::element::{UnsafeElement, UnsafeFlatElement};
use crate::util::Unwrappable;
use core::fmt;
use core::hint::unreachable_unchecked;
use core::ops;
use core::slice;

const ASCII_UNDERSCORE: u8 = b'_';
const ASCII_AMPERSAND: u8 = b'&';
const ASCII_COLON: u8 = b':';
const ASCII_PLUS_SIGN: u8 = b'+';
const ASCII_TILDE_SIGN: u8 = b'~';

#[derive(Debug)]
/// # Skyhash Deserializer (Parser)
///
/// The [`Parser`] object can be used to deserialized a packet serialized by Skyhash which in turn serializes
/// it into data structures native to the Rust Language (and some Compound Types built on top of them).
///
/// ## Safety
///
/// The results returned by the parser are not bound by any lifetime and instead return raw
/// pointers to parts of the source buffer. This means that the caller must ensure that the
/// source buffer remains valid for as long as the result is used.
///
/// ## Evaluation
///
/// The parser is pessimistic in most cases and will readily throw out any errors. On non-recusrive types
/// there is no recursion, but the parser will use implicit recursion for nested arrays. The parser will
/// happily not report any errors if some part of the next query was passed. This is very much a possibility
/// and so has been accounted for
///
/// ## Important note
///
/// All developers willing to modify the deserializer must keep this in mind: the cursor is always Ahead-Of-Position
/// that is the cursor should always point at the next character that can be read.
///
pub struct Parser<'a> {
    /// the cursor ptr
    cursor: *const u8,
    /// the data end ptr
    data_end_ptr: *const u8,
    /// the buffer
    buffer: &'a [u8],
}

#[derive(PartialEq)]
/// As its name says, an [`UnsafeSlice`] is a terribly unsafe slice. It's guarantess are
/// very C-like, your ptr goes dangling -- and everything is unsafe.
///
/// ## Safety contracts
/// - The `start_ptr` is valid
/// - The `len` is correct
/// - `start_ptr` remains valid as long as the object is used
///
pub struct UnsafeSlice {
    start_ptr: *const u8,
    len: usize,
}

// we know we won't let the ptrs go out of scope
unsafe impl Send for UnsafeSlice {}
unsafe impl Sync for UnsafeSlice {}

// The debug impl is unsafe, but since we know we'll only ever use it within this module
// and that it can be only returned by this module, we'll keep it here
impl fmt::Debug for UnsafeSlice {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe { f.write_str(core::str::from_utf8_unchecked(self.as_slice())) }
    }
}

impl UnsafeSlice {
    /// Create a new `UnsafeSlice`
    pub const unsafe fn new(start_ptr: *const u8, len: usize) -> Self {
        Self { start_ptr, len }
    }
    /// Return self as a slice
    pub unsafe fn as_slice(&self) -> &[u8] {
        slice::from_raw_parts(self.start_ptr, self.len)
    }
    /// Destruct self, and return a slice, lifetime bound by whoever calls it
    pub unsafe fn into_slice<'a>(self) -> &'a [u8] {
        slice::from_raw_parts(self.start_ptr, self.len)
    }
    #[cfg(test)]
    pub unsafe fn to_owned(&self) -> Vec<u8> {
        self.as_slice().to_owned()
    }
    /// Check if a certain idx has a certain byte. This can be _thought of something
    /// like_:
    /// ```notest
    /// *(slice.get_unchecked(pos)).eq(pos)
    /// ```
    pub unsafe fn unsafe_eq(&self, byte: u8, pos: usize) -> bool {
        *self.start_ptr.add(pos) == byte
    }
    /// Turns self into a slice, lifetime bound by caller's lifetime with the provided `chop`.
    /// This is roughly equivalent to:
    /// ```notest
    /// &slice[..slice.len() - chop]
    /// ```
    pub unsafe fn into_slice_with_start_and_end<'a>(self, len: usize, chop: usize) -> &'a [u8] {
        debug_assert!(len <= self.len);
        slice::from_raw_parts(self.start_ptr.add(len), self.len - chop)
    }
}

#[derive(Debug, PartialEq)]
/// # Parser Errors
///
/// Several errors can arise during parsing and this enum accounts for them
pub enum ParseError {
    /// Didn't get the number of expected bytes
    NotEnough,
    /// The query contains an unexpected byte
    UnexpectedByte,
    /// The packet simply contains invalid data
    ///
    /// This is rarely returned and only in the special cases where a bad client sends `0` as
    /// the query count
    BadPacket,
    /// A data type was given but the parser failed to serialize it into this type
    ///
    /// This can happen not just for elements but can also happen for their sizes ([`Self::parse_into_u64`])
    DatatypeParseFailure,
    /// A data type that the server doesn't know was passed into the query
    ///
    /// This is a frequent problem that can arise between different server editions as more data types
    /// can be added with changing server versions
    UnknownDatatype,
    /// The query is empty
    ///
    /// The **parser will never return this**, but instead it is provided for convenience with [`dbnet`]
    Empty,
}

#[derive(Debug, PartialEq)]
/// A simple query object. This object is **not bound to any lifetime!** That's
/// why, merely _having_ it is not unsafe, but all methods on it are unsafe and
/// the caller has to uphold the guarantee of keeping the source buffer's pointers
/// valid
///
/// ## Safety Contracts
///
/// - The provided `UnsafeElement` is valid and generated _legally_
pub struct SimpleQuery {
    /// the inner unsafe element
    inner: UnsafeElement,
}

impl SimpleQuery {
    /// Create a new `SimpleQuery`
    ///
    /// ## Safety
    ///
    /// This is unsafe because the caller must guarantee the sanctity of
    /// the provided element
    const unsafe fn new(inner: UnsafeElement) -> SimpleQuery {
        Self { inner }
    }
    /// Decomposes self into an [`UnsafeElement`]
    ///
    /// ## Safety
    ///
    /// Caller must ensure that the UnsafeElement's pointers are still valid
    pub unsafe fn into_inner(self) -> UnsafeElement {
        self.inner
    }
    pub const fn is_any_array(&self) -> bool {
        matches!(self.inner, UnsafeElement::AnyArray(_))
    }
}

#[derive(Debug, PartialEq)]
/// A pipelined query object. This is bound to an _anonymous lifetime_ which is to be bound by
/// the instantiator
///
/// ## Safety Contracts
///
/// - The provided `UnsafeElement` is valid and generated _legally_
/// - The source pointers for the `UnsafeElement` is valid
pub struct PipelineQuery {
    inner: Box<[UnsafeElement]>,
}

impl PipelineQuery {
    /// Create a new `PipelineQuery`
    ///
    /// ## Safety
    ///
    /// The caller has the responsibility to uphold the guarantee of keeping the source
    /// pointers valid
    const unsafe fn new(inner: Box<[UnsafeElement]>) -> PipelineQuery {
        Self { inner }
    }
    pub const fn len(&self) -> usize {
        self.inner.len()
    }
}

impl ops::Deref for PipelineQuery {
    type Target = [UnsafeElement];
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, PartialEq)]
/// # Queries
///
/// This enum has two variants: Simple and Pipelined. Both hold two **terribly `unsafe`**
/// objects:
/// - A [`SimpleQuery`] or
/// - A [`PipelineQuery`]
///
/// Again, **both objects hold raw pointers and guarantee nothing about the source's
/// validity**.
/// ## Safety
///
/// This object holds zero ownership on the actual data, but only holds a collection
/// of pointers. This means that you have to **ensure that the source outlives the Query**
/// object. Using it otherwise is very **`unsafe`**! The caller who creates the instance or the
/// one who uses it is responsible for ensuring any validity guarantee
///
pub enum Query {
    /// A simple query will just hold one element
    SimpleQuery(SimpleQuery),
    /// A pipelined/batch query will hold multiple elements
    PipelineQuery(PipelineQuery),
}

impl Query {
    #[cfg(test)]
    /// Turns self into an onwed query (see [`OwnedQuery`])
    ///
    /// ## Safety contracts
    ///
    /// - The query itself is valid
    /// - The query is correctly bound to the lifetime
    unsafe fn into_owned_query(self) -> OwnedQuery {
        match self {
            Self::SimpleQuery(SimpleQuery { inner, .. }) => {
                OwnedQuery::SimpleQuery(inner.as_owned_element())
            }
            Self::PipelineQuery(PipelineQuery { inner, .. }) => {
                OwnedQuery::PipelineQuery(inner.iter().map(|v| v.as_owned_element()).collect())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
#[cfg(test)]
/// An owned query with direct ownership of the data rather than through
/// pointers
enum OwnedQuery {
    SimpleQuery(OwnedElement),
    PipelineQuery(Vec<OwnedElement>),
}

/// A generic result to indicate parsing errors thorugh the [`ParseError`] enum
pub type ParseResult<T> = Result<T, ParseError>;

impl<'a> Parser<'a> {
    /// Create a new parser instance, bound to the lifetime of the source buffer
    pub fn new(buffer: &'a [u8]) -> Self {
        unsafe {
            let cursor = buffer.as_ptr();
            let data_end_ptr = cursor.add(buffer.len());
            Self {
                cursor,
                data_end_ptr,
                buffer,
            }
        }
    }
    /// Returns what we have consumed
    /// ```text
    /// [*****************************]
    ///  ^    ^                      ^
    /// start cursor             data end ptr
    /// ```
    fn consumed(&self) -> usize {
        unsafe { self.cursor.offset_from(self.buffer.as_ptr()) as usize }
    }
    /// Returns what we have left
    /// ```text
    /// [*****************************]
    ///    ^                         ^
    ///  cursor             data end ptr
    /// ```
    fn remaining(&self) -> usize {
        unsafe { self.data_end_ptr().offset_from(self.cursor) as usize }
    }
    fn exhausted(&self) -> bool {
        self.cursor >= self.data_end_ptr()
    }
    /// Returns a ptr to one byte past the last non-null ptr
    const fn data_end_ptr(&self) -> *const u8 {
        self.data_end_ptr
    }
    /// Move the cursor ptr ahead by the provided amount
    fn incr_cursor_by(&mut self, by: usize) {
        unsafe { self.cursor = self.cursor.add(by) }
    }
    /// Increment the cursor by 1
    fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    /// Read `until` bytes from source
    fn read_until(&mut self, until: usize) -> ParseResult<UnsafeSlice> {
        if self.remaining() < until {
            Err(ParseError::NotEnough)
        } else {
            let start_ptr = self.cursor;
            self.incr_cursor_by(until);
            unsafe { Ok(UnsafeSlice::new(start_ptr, until)) }
        }
    }
    /// Read a line. This will place the cursor at a point just ahead of
    /// the LF
    fn read_line(&mut self) -> ParseResult<UnsafeSlice> {
        if self.exhausted() {
            Err(ParseError::NotEnough)
        } else {
            let start_ptr = self.cursor;
            let end_ptr = self.data_end_ptr();
            let mut len = 0usize;
            unsafe {
                while end_ptr > self.cursor {
                    if *self.cursor == b'\n' {
                        self.incr_cursor();
                        break;
                    }
                    len += 1;
                    self.incr_cursor();
                }
                Ok(UnsafeSlice::new(start_ptr, len))
            }
        }
    }
}

impl<'a> Parser<'a> {
    /// Returns true if the cursor will give a char, but if `this_if_nothing_ahead` is set
    /// to true, then if no byte is ahead, it will still return true
    fn will_cursor_give_char(&self, ch: u8, this_if_nothing_ahead: bool) -> ParseResult<bool> {
        if self.exhausted() {
            // nothing left
            if this_if_nothing_ahead {
                Ok(true)
            } else {
                Err(ParseError::NotEnough)
            }
        } else if unsafe { (*self.cursor).eq(&ch) } {
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Check if the current cursor will give an LF
    fn will_cursor_give_linefeed(&self) -> ParseResult<bool> {
        self.will_cursor_give_char(b'\n', false)
    }
}

impl<'a> Parser<'a> {
    /// Parse a stream of bytes into [`usize`]
    fn parse_into_usize(bytes: &[u8]) -> ParseResult<usize> {
        if bytes.is_empty() {
            return Err(ParseError::NotEnough);
        }
        let byte_iter = bytes.iter();
        let mut item_usize = 0usize;
        for dig in byte_iter {
            if !dig.is_ascii_digit() {
                // dig has to be an ASCII digit
                return Err(ParseError::DatatypeParseFailure);
            }
            // 48 is the ASCII code for 0, and 57 is the ascii code for 9
            // so if 0 is given, the subtraction should give 0; similarly
            // if 9 is given, the subtraction should give us 9!
            let curdig: usize = unsafe {
                // UNSAFE(@ohsayan): We already know that dig is an ASCII digit
                dig.checked_sub(48).unsafe_unwrap()
            }
            .into();
            // The usize can overflow; check that case
            let product = match item_usize.checked_mul(10) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DatatypeParseFailure),
            };
            let sum = match product.checked_add(curdig) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DatatypeParseFailure),
            };
            item_usize = sum;
        }
        Ok(item_usize)
    }
    /// Pasre a stream of bytes into an [`u64`]
    fn parse_into_u64(bytes: &[u8]) -> ParseResult<u64> {
        if bytes.is_empty() {
            return Err(ParseError::NotEnough);
        }
        let byte_iter = bytes.iter();
        let mut item_u64 = 0u64;
        for dig in byte_iter {
            if !dig.is_ascii_digit() {
                // dig has to be an ASCII digit
                return Err(ParseError::DatatypeParseFailure);
            }
            // 48 is the ASCII code for 0, and 57 is the ascii code for 9
            // so if 0 is given, the subtraction should give 0; similarly
            // if 9 is given, the subtraction should give us 9!
            let curdig: u64 = unsafe {
                // UNSAFE(@ohsayan): We already know that dig is an ASCII digit
                dig.checked_sub(48).unsafe_unwrap()
            }
            .into();
            // Now the entire u64 can overflow, so let's attempt to check it
            let product = match item_u64.checked_mul(10) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DatatypeParseFailure),
            };
            let sum = match product.checked_add(curdig) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DatatypeParseFailure),
            };
            item_u64 = sum;
        }
        Ok(item_u64)
    }
}

impl<'a> Parser<'a> {
    /// Parse the metaframe to get the number of queries, i.e the datagroup
    /// count
    fn parse_metaframe_get_datagroup_count(&mut self) -> ParseResult<usize> {
        if self.buffer.len() < 3 {
            // the smallest query we can have is: *1\n or 3 chars
            Err(ParseError::NotEnough)
        } else {
            unsafe {
                let our_chunk = self.read_line()?;
                if our_chunk.unsafe_eq(b'*', 0) {
                    Ok(Self::parse_into_usize(
                        our_chunk.into_slice_with_start_and_end(1, 1),
                    )?)
                } else {
                    Err(ParseError::UnexpectedByte)
                }
            }
        }
    }
}

impl<'a> Parser<'a> {
    /// Gets the _next element. **The cursor should be at the tsymbol (passed)**
    fn _next(&mut self) -> ParseResult<UnsafeSlice> {
        let sizeline = self.read_line()?;
        unsafe {
            let element_size = Self::parse_into_usize(sizeline.into_slice())?;
            self.read_until(element_size)
        }
    }
    /// Gets the next string (`+`). **The cursor should be at the tsymbol (passed)**
    fn parse_next_string(&mut self) -> ParseResult<UnsafeSlice> {
        {
            let chunk = self._next()?;
            let haslf = self.will_cursor_give_linefeed()?;
            if haslf {
                self.incr_cursor();
                Ok(chunk)
            } else {
                Err(ParseError::NotEnough)
            }
        }
    }
    /// Gets the next `u64`. **The cursor should be at the tsymbol (passed)**
    fn parse_next_u64(&mut self) -> ParseResult<u64> {
        let chunk = self._next()?;
        unsafe {
            let ret = Self::parse_into_u64(chunk.into_slice())?;
            if self.will_cursor_give_linefeed()? {
                self.incr_cursor();
                Ok(ret)
            } else {
                Err(ParseError::NotEnough)
            }
        }
    }
    /// Gets the next element. **The cursor should be at the tsymbol (_not_ passed)**
    fn parse_next_element(&mut self) -> ParseResult<UnsafeElement> {
        if self.exhausted() {
            Err(ParseError::NotEnough)
        } else {
            unsafe {
                let tsymbol = *self.cursor;
                // got tsymbol, now incr
                self.incr_cursor();
                let ret = match tsymbol {
                    ASCII_PLUS_SIGN => UnsafeElement::String(self.parse_next_string()?),
                    ASCII_COLON => UnsafeElement::UnsignedInt(self.parse_next_u64()?),
                    ASCII_AMPERSAND => UnsafeElement::Array(self.parse_next_array()?),
                    ASCII_TILDE_SIGN => UnsafeElement::AnyArray(self.parse_next_any_array()?),
                    ASCII_UNDERSCORE => UnsafeElement::FlatArray(self.parse_next_flat_array()?),
                    _ => {
                        return Err(ParseError::UnknownDatatype);
                    }
                };
                Ok(ret)
            }
        }
    }
    /// Parse the next blob. **The cursor should be at the tsymbol (passed)**
    fn parse_next_blob(&mut self) -> ParseResult<UnsafeSlice> {
        {
            let chunk = self._next()?;
            if self.will_cursor_give_linefeed()? {
                self.incr_cursor();
                Ok(chunk)
            } else {
                Err(ParseError::UnexpectedByte)
            }
        }
    }
    /// Parse the next `AnyArray`. **The cursor should be at the tsymbol (passed)**
    fn parse_next_any_array(&mut self) -> ParseResult<Box<[UnsafeSlice]>> {
        unsafe {
            let size_line = self.read_line()?;
            let size = Self::parse_into_usize(size_line.into_slice())?;
            let mut array = Vec::with_capacity(size);
            for _ in 0..size {
                array.push(self.parse_next_blob()?);
            }
            Ok(array.into_boxed_slice())
        }
    }
    /// The cursor should have passed the tsymbol
    fn parse_next_flat_array(&mut self) -> ParseResult<Box<[UnsafeFlatElement]>> {
        unsafe {
            let flat_array_sizeline = self.read_line()?;
            let array_size = Self::parse_into_usize(flat_array_sizeline.into_slice())?;
            let mut array = Vec::with_capacity(array_size);
            for _ in 0..array_size {
                if self.exhausted() {
                    return Err(ParseError::NotEnough);
                } else {
                    let tsymb = *self.cursor;
                    // good, there is a tsymbol; move the cursor ahead
                    self.incr_cursor();
                    let ret = match tsymb {
                        b'+' => self.parse_next_string().map(UnsafeFlatElement::String)?,
                        _ => return Err(ParseError::UnknownDatatype),
                    };
                    array.push(ret);
                }
            }
            Ok(array.into_boxed_slice())
        }
    }
    /// Parse the next array. **The cursor should be at the tsymbol (passed)**
    fn parse_next_array(&mut self) -> ParseResult<Box<[UnsafeElement]>> {
        unsafe {
            let size_of_array_chunk = self.read_line()?;
            let size_of_array = Self::parse_into_usize(size_of_array_chunk.into_slice())?;
            let mut array = Vec::with_capacity(size_of_array);
            for _ in 0..size_of_array {
                array.push(self.parse_next_element()?);
            }
            Ok(array.into_boxed_slice())
        }
    }
}

impl<'a> Parser<'a> {
    /// Try to parse the provided buffer (lt: 'a) into a Query (lt: 'a). The
    /// parser itself can (or must) go out of scope, but the buffer can't!
    pub fn parse(&mut self) -> Result<(Query, usize), ParseError> {
        let number_of_queries = self.parse_metaframe_get_datagroup_count()?;
        if number_of_queries == 0 {
            return Err(ParseError::BadPacket);
        }
        if number_of_queries == 1 {
            let query = self.parse_next_element()?;
            if unsafe {
                self.will_cursor_give_char(b'*', true)
                    .unwrap_or_else(|_| unreachable_unchecked())
            } {
                unsafe {
                    // SAFETY: Contract upheld
                    Ok((Query::SimpleQuery(SimpleQuery::new(query)), self.consumed()))
                }
            } else {
                Err(ParseError::UnexpectedByte)
            }
        } else {
            // pipelined query
            let mut queries = Vec::with_capacity(number_of_queries);
            for _ in 0..number_of_queries {
                queries.push(self.parse_next_element()?);
            }
            if unsafe {
                self.will_cursor_give_char(b'*', true)
                    .unwrap_or_else(|_| unreachable_unchecked())
            } {
                unsafe {
                    // SAFETY: Contract upheld
                    Ok((
                        Query::PipelineQuery(PipelineQuery::new(queries.into_boxed_slice())),
                        self.consumed(),
                    ))
                }
            } else {
                Err(ParseError::UnexpectedByte)
            }
        }
    }
}
