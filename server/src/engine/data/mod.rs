/*
 * Created on Sat Feb 04 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

#[macro_use]
mod macros;
pub mod lit;
pub mod md_dict;
pub mod spec;
pub mod tag;
#[cfg(test)]
mod tests;

pub use md_dict::{DictEntryGeneric, DictGeneric, MetaDict};
use {
    self::lit::Lit,
    crate::engine::{data::lit::LitIR, mem::AStr},
    std::mem::{self, Discriminant},
};

const IDENT_MX: usize = 64;
pub type ItemID = AStr<IDENT_MX>;

/// A [`DataType`] represents the underlying data-type, although this enumeration when used in a collection will always
/// be of one type.
// TODO(@ohsayan): Change the underlying structures, there are just rudimentary ones used during integration with the QL
#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(Clone))]
#[repr(u8)]
pub enum HSData {
    /// An UTF-8 string
    String(Box<str>) = DataKind::STR_BX.d(),
    /// Bytes
    Binary(Box<[u8]>) = DataKind::BIN_BX.d(),
    /// An unsigned integer
    ///
    /// **NOTE:** This is the default evaluated type for unsigned integers by the query processor. It is the
    /// responsibility of the executor to ensure integrity checks depending on actual type width in the declared
    /// schema (if any)
    UnsignedInt(u64) = DataKind::UINT64.d(),
    /// A signed integer
    ///
    /// **NOTE:** This is the default evaluated type for signed integers by the query processor. It is the
    /// responsibility of the executor to ensure integrity checks depending on actual type width in the declared
    /// schema (if any)
    SignedInt(i64) = DataKind::SINT64.d(),
    /// A boolean
    Boolean(bool) = DataKind::BOOL.d(),
    /// A float (64-bit)
    Float(f64) = DataKind::FLOAT64.d(),
    /// A single-type list. Note, you **need** to keep up the invariant that the [`DataType`] disc. remains the same for all
    /// elements to ensure correctness in this specific context
    /// FIXME(@ohsayan): Try enforcing this somehow
    List(Vec<Self>) = DataKind::LIST.d(),
}

direct_from! {
    HSData => {
        String as String,
        Vec<u8> as Binary,
        u64 as UnsignedInt,
        bool as Boolean,
        Vec<Self> as List,
        &'static str as String,
    }
}

impl HSData {
    #[inline(always)]
    pub(super) fn clone_from_lit(lit: Lit) -> Self {
        match_data!(match lit {
            Lit::Str(s) => HSData::String(s.to_string().into_boxed_str()),
            Lit::Bool(b) => HSData::Boolean(b),
            Lit::UnsignedInt(u) => HSData::UnsignedInt(u),
            Lit::SignedInt(i) => HSData::SignedInt(i),
            Lit::Float(f) => HSData::Float(f),
            Lit::Bin(l) => HSData::Binary(l.to_vec().into_boxed_slice()),
            TagClass::List(_) => unreachable!("found 2D data in 1D"),
        })
    }
    #[inline(always)]
    pub(super) fn clone_from_litir<'a>(lit: LitIR<'a>) -> Self {
        match_data!(match lit {
            LitIR::Str(s) => Self::String(s.to_owned().into_boxed_str()),
            LitIR::Bin(b) => Self::Binary(b.to_owned().into_boxed_slice()),
            LitIR::Float(f) => Self::Float(f),
            LitIR::SignedInt(s) => Self::SignedInt(s),
            LitIR::UnsignedInt(u) => Self::UnsignedInt(u),
            LitIR::Bool(b) => Self::Boolean(b),
            TagClass::List(_) => unreachable!("found 2D data in 1D"),
        })
    }
    fn kind(&self) -> Discriminant<Self> {
        mem::discriminant(&self)
    }
}

impl<'a> From<Lit<'a>> for HSData {
    fn from(l: Lit<'a>) -> Self {
        Self::clone_from_lit(l)
    }
}

impl<'a> From<LitIR<'a>> for HSData {
    fn from(l: LitIR<'a>) -> Self {
        Self::clone_from_litir(l)
    }
}

impl<const N: usize> From<[HSData; N]> for HSData {
    fn from(f: [HSData; N]) -> Self {
        Self::List(f.into())
    }
}

flags! {
    #[derive(PartialEq, Eq, Clone, Copy)]
    pub struct DataKind: u8 {
        // primitive: integer unsigned
        UINT8 = 0,
        UINT16 = 1,
        UINT32 = 2,
        UINT64 = 3,
        // primitive: integer unsigned
        SINT8 = 4,
        SINT16 = 5,
        SINT32 = 6,
        SINT64 = 7,
        // primitive: misc
        BOOL = 8,
        // primitive: floating point
        FLOAT32 = 9,
        FLOAT64 = 10,
        // compound: flat
        STR = 11,
        STR_BX = DataKind::_BASE_HB | DataKind::STR.d(),
        BIN = 12,
        BIN_BX = DataKind::_BASE_HB | DataKind::BIN.d(),
        // compound: recursive
        LIST = 13,
    }
}
