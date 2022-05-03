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

#[cfg(test)]
use self::interface::ProtocolSpec;
use {
    crate::corestore::heap_array::HeapArray,
    core::{fmt, slice},
};
// pub mods
pub mod interface;
pub mod iter;
// internal mods
mod raw_parser;
// versions
mod v1;
mod v2;
// endof pub mods

pub type Skyhash2 = v2::Parser;
pub type Skyhash1 = v1::Parser;
#[cfg(test)]
/// The latest protocol version supported by this version
pub const LATEST_PROTOCOL_VERSION: f32 = Skyhash2::PROTOCOL_VERSION;
#[cfg(test)]
/// The latest protocol version supported by this version (`Skyhash-x.y`)
pub const LATEST_PROTOCOL_VERSIONSTRING: &str = Skyhash2::PROTOCOL_VERSIONSTRING;

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
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            // UNSAFE(@ohsayan): Just like core::slice, we resemble the same idea:
            // we assume that the unsafe construction was correct and hence *assume*
            // that calling this is safe
            slice::from_raw_parts(self.start_ptr, self.len)
        }
    }
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
/// # Parser Errors
///
/// Several errors can arise during parsing and this enum accounts for them
pub enum ParseError {
    /// Didn't get the number of expected bytes
    NotEnough = 0u8,
    /// The packet simply contains invalid data
    BadPacket = 1u8,
    /// The query contains an unexpected byte
    UnexpectedByte = 2u8,
    /// A data type was given but the parser failed to serialize it into this type
    ///
    /// This can happen not just for elements but can also happen for their sizes ([`Self::parse_into_u64`])
    DatatypeParseFailure = 3u8,
    /// The client supplied the wrong query data type for the given query
    WrongType = 4u8,
}

/// A generic result to indicate parsing errors thorugh the [`ParseError`] enum
pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Debug)]
pub enum Query {
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
            data: self.data.iter().map(|v| v.as_slice().to_owned()).collect(),
        }
    }
    pub const fn new(data: HeapArray<UnsafeSlice>) -> Self {
        Self { data }
    }
    pub fn as_slice(&self) -> &[UnsafeSlice] {
        &self.data
    }
}

#[cfg(test)]
struct OwnedSimpleQuery {
    pub data: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub struct PipelinedQuery {
    data: HeapArray<HeapArray<UnsafeSlice>>,
}

impl PipelinedQuery {
    pub const fn new(data: HeapArray<HeapArray<UnsafeSlice>>) -> Self {
        Self { data }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn into_inner(self) -> HeapArray<HeapArray<UnsafeSlice>> {
        self.data
    }
    #[cfg(test)]
    fn into_owned(self) -> OwnedPipelinedQuery {
        OwnedPipelinedQuery {
            data: self
                .data
                .iter()
                .map(|v| v.iter().map(|v| v.as_slice().to_owned()).collect())
                .collect(),
        }
    }
}

#[cfg(test)]
struct OwnedPipelinedQuery {
    pub data: Vec<Vec<Vec<u8>>>,
}
