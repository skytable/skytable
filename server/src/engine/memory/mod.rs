/*
 * Created on Wed Oct 12 2022
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

// TODO(@ohsayan): Change the underlying structures, there are just rudimentary ones used during integration with the QL

use super::ql::RawSlice;

/// A [`DataType`] represents the underlying data-type, although this enumeration when used in a collection will always
/// be of one type.
#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub enum DataType {
    /// An UTF-8 string
    String(String),
    /// Bytes
    Binary(Vec<u8>),
    /// An unsigned integer
    ///
    /// **NOTE:** This is the default evaluated type for unsigned integers by the query processor. It is the
    /// responsibility of the executor to ensure integrity checks depending on actual type width in the declared
    /// schema (if any)
    UnsignedInt(u64),
    /// A signed integer
    ///
    /// **NOTE:** This is the default evaluated type for signed integers by the query processor. It is the
    /// responsibility of the executor to ensure integrity checks depending on actual type width in the declared
    /// schema (if any)
    SignedInt(i64),
    /// A boolean
    Boolean(bool),
    /// A single-type list. Note, you **need** to keep up the invariant that the [`DataType`] disc. remains the same for all
    /// elements to ensure correctness in this specific context
    /// FIXME(@ohsayan): Try enforcing this somehow
    List(Vec<Self>),
    /// **☢ WARNING:** Not an actual data type but MUST be translated into an actual data type
    AnonymousTypeNeedsEval(RawSlice),
}

enum_impls! {
    DataType => {
        String as String,
        Vec<u8> as Binary,
        u64 as UnsignedInt,
        bool as Boolean,
        Vec<Self> as List,
        &'static str as String,
    }
}

impl<const N: usize> From<[DataType; N]> for DataType {
    fn from(f: [DataType; N]) -> Self {
        Self::List(f.into())
    }
}

#[repr(u8, align(1))]
pub enum DataKind {
    // primitive: integer unsigned
    UInt8 = 0,
    UInt16 = 1,
    Uint32 = 2,
    UInt64 = 3,
    // primitive: integer unsigned
    SInt8 = 4,
    SInt16 = 5,
    SInt32 = 6,
    SInt64 = 7,
    // primitive: misc
    Bool = 8,
    // compound: flat
    String = 9,
    Binary = 10,
    // compound: recursive
    List = 11,
}
