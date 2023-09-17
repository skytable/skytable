/*
 * Created on Fri Aug 04 2023
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

//! High level interfaces

use crate::engine::idx::STIndex;

pub mod map;
pub mod obj;
// tests
#[cfg(test)]
mod tests;

use {
    crate::engine::{
        data::{
            dict::DictEntryGeneric,
            tag::{DataTag, TagClass},
        },
        idx::{AsKey, AsValue},
        mem::BufferedScanner,
        storage::v1::{SDSSError, SDSSResult},
    },
    std::mem,
};

type VecU8 = Vec<u8>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, sky_macros::EnumMethods)]
#[repr(u8)]
/// Disambiguation for data
pub enum PersistTypeDscr {
    Null = 0,
    Bool = 1,
    UnsignedInt = 2,
    SignedInt = 3,
    Float = 4,
    Bin = 5,
    Str = 6,
    List = 7,
    Dict = 8,
}

impl PersistTypeDscr {
    /// translates the tag class definition into the dscr definition
    pub const fn translate_from_class(class: TagClass) -> Self {
        unsafe { Self::from_raw(class.value_u8() + 1) }
    }
    pub const fn try_from_raw(v: u8) -> Option<Self> {
        if v > Self::MAX {
            None
        } else {
            unsafe { Some(Self::from_raw(v)) }
        }
    }
    pub const unsafe fn from_raw(v: u8) -> Self {
        core::mem::transmute(v)
    }
    pub fn new_from_dict_gen_entry(e: &DictEntryGeneric) -> Self {
        match e {
            DictEntryGeneric::Map(_) => Self::Dict,
            DictEntryGeneric::Data(dc) => Self::translate_from_class(dc.tag().tag_class()),
        }
    }
    /// The data in question is null (well, can we call that data afterall?)
    pub const fn is_null(&self) -> bool {
        self.value_u8() == Self::Null.value_u8()
    }
    /// The data in question is a scalar
    pub const fn is_scalar(&self) -> bool {
        self.value_u8() <= Self::Float.value_u8()
    }
    /// The data is composite
    pub const fn is_composite(&self) -> bool {
        self.value_u8() > Self::Float.value_u8()
    }
    /// Recursive data
    pub const fn is_recursive(&self) -> bool {
        self.value_u8() >= Self::List.value_u8()
    }
    fn into_class(&self) -> TagClass {
        debug_assert!(*self != Self::Null);
        unsafe { mem::transmute(self.value_u8() - 1) }
    }
}

/*
    obj spec
*/

/// Any object that can be persisted
pub trait PersistObject {
    // const
    /// Size of the metadata region
    const METADATA_SIZE: usize;
    // types
    /// Input type for enc operations
    type InputType: Copy;
    /// Output type for dec operations
    type OutputType;
    /// Metadata type
    type Metadata;
    // pretest
    /// Pretest to see if the src has the required data for metadata dec. Defaults to the metadata size
    fn pretest_can_dec_metadata(scanner: &BufferedScanner) -> bool {
        scanner.has_left(Self::METADATA_SIZE)
    }
    /// Pretest to see if the src has the required data for object dec
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool;
    // meta
    /// metadata enc
    fn meta_enc(buf: &mut VecU8, data: Self::InputType);
    /// metadata dec
    ///
    /// ## Safety
    ///
    /// Must pass the [`PersistObject::pretest_can_dec_metadata`] assertion
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata>;
    // obj
    /// obj enc
    fn obj_enc(buf: &mut VecU8, data: Self::InputType);
    /// obj dec
    ///
    /// ## Safety
    ///
    /// Must pass the [`PersistObject::pretest_can_dec_object`] assertion
    unsafe fn obj_dec(s: &mut BufferedScanner, md: Self::Metadata) -> SDSSResult<Self::OutputType>;
    // default
    /// Default routine to encode an object + its metadata
    fn default_full_enc(buf: &mut VecU8, data: Self::InputType) {
        Self::meta_enc(buf, data);
        Self::obj_enc(buf, data);
    }
    /// Default routine to decode an object + its metadata (however, the metadata is used and not returned)
    fn default_full_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::OutputType> {
        if !Self::pretest_can_dec_metadata(scanner) {
            return Err(SDSSError::InternalDecodeStructureCorrupted);
        }
        let md = unsafe {
            // UNSAFE(@ohsayan): +pretest
            Self::meta_dec(scanner)?
        };
        if !Self::pretest_can_dec_object(scanner, &md) {
            return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
        }
        unsafe {
            // UNSAFE(@ohsayan): +obj pretest
            Self::obj_dec(scanner, md)
        }
    }
}

/*
    map spec
*/

/// specification for a persist map
pub trait PersistMapSpec {
    /// map type
    type MapType: STIndex<Self::Key, Self::Value>;
    /// map iter
    type MapIter<'a>: Iterator<Item = (&'a Self::Key, &'a Self::Value)>
    where
        Self: 'a;
    /// metadata type
    type EntryMD;
    /// key type (NOTE: set this to the true key type; handle any differences using the spec unless you have an entirely different
    /// wrapper type)
    type Key: AsKey;
    /// value type (NOTE: see [`PersistMapSpec::Key`])
    type Value: AsValue;
    /// coupled enc
    const ENC_COUPLED: bool;
    /// coupled dec
    const DEC_COUPLED: bool;
    // collection misc
    fn _get_iter<'a>(map: &'a Self::MapType) -> Self::MapIter<'a>;
    // collection meta
    /// pretest before jmp to routine for entire collection
    fn pretest_collection_using_size(_: &BufferedScanner, _: usize) -> bool {
        true
    }
    /// pretest before jmp to entry dec routine
    fn pretest_entry_metadata(scanner: &BufferedScanner) -> bool;
    /// pretest the src before jmp to entry data dec routine
    fn pretest_entry_data(scanner: &BufferedScanner, md: &Self::EntryMD) -> bool;
    // entry meta
    /// enc the entry meta
    fn entry_md_enc(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// dec the entry meta
    /// SAFETY: ensure that all pretests have passed (we expect the caller to not be stupid)
    unsafe fn entry_md_dec(scanner: &mut BufferedScanner) -> Option<Self::EntryMD>;
    // independent packing
    /// enc key (non-packed)
    fn enc_key(buf: &mut VecU8, key: &Self::Key);
    /// enc val (non-packed)
    fn enc_val(buf: &mut VecU8, key: &Self::Value);
    /// dec key (non-packed)
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Key>;
    /// dec val (non-packed)
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Value>;
    // coupled packing
    /// entry packed enc
    fn enc_entry(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// entry packed dec
    unsafe fn dec_entry(
        scanner: &mut BufferedScanner,
        md: Self::EntryMD,
    ) -> Option<(Self::Key, Self::Value)>;
}

// enc
pub mod enc {
    use super::{map, PersistMapSpec, PersistObject, VecU8};
    // obj
    pub fn enc_full<Obj: PersistObject>(obj: Obj::InputType) -> Vec<u8> {
        let mut v = vec![];
        enc_full_into_buffer::<Obj>(&mut v, obj);
        v
    }
    pub fn enc_full_into_buffer<Obj: PersistObject>(buf: &mut VecU8, obj: Obj::InputType) {
        Obj::default_full_enc(buf, obj)
    }
    pub fn enc_full_self<Obj: PersistObject<InputType = Obj>>(obj: Obj) -> Vec<u8> {
        enc_full::<Obj>(obj)
    }
    // dict
    pub fn enc_dict_full<PM: PersistMapSpec>(dict: &PM::MapType) -> Vec<u8> {
        let mut v = vec![];
        enc_dict_full_into_buffer::<PM>(&mut v, dict);
        v
    }
    pub fn enc_dict_full_into_buffer<PM: PersistMapSpec>(buf: &mut VecU8, dict: &PM::MapType) {
        <map::PersistMapImpl<PM> as PersistObject>::default_full_enc(buf, dict)
    }
}

// dec
pub mod dec {
    use {
        super::{map, PersistMapSpec, PersistObject},
        crate::engine::{mem::BufferedScanner, storage::v1::SDSSResult},
    };
    // obj
    pub fn dec_full<Obj: PersistObject>(data: &[u8]) -> SDSSResult<Obj::OutputType> {
        let mut scanner = BufferedScanner::new(data);
        dec_full_from_scanner::<Obj>(&mut scanner)
    }
    pub fn dec_full_from_scanner<Obj: PersistObject>(
        scanner: &mut BufferedScanner,
    ) -> SDSSResult<Obj::OutputType> {
        Obj::default_full_dec(scanner)
    }
    pub fn dec_full_self<Obj: PersistObject<OutputType = Obj>>(data: &[u8]) -> SDSSResult<Obj> {
        dec_full::<Obj>(data)
    }
    // dec
    pub fn dec_dict_full<PM: PersistMapSpec>(data: &[u8]) -> SDSSResult<PM::MapType> {
        let mut scanner = BufferedScanner::new(data);
        dec_dict_full_from_scanner::<PM>(&mut scanner)
    }
    fn dec_dict_full_from_scanner<PM: PersistMapSpec>(
        scanner: &mut BufferedScanner,
    ) -> SDSSResult<PM::MapType> {
        <map::PersistMapImpl<PM> as PersistObject>::default_full_dec(scanner)
    }
    pub mod utils {
        use crate::engine::{
            mem::BufferedScanner,
            storage::v1::{SDSSError, SDSSResult},
        };
        pub unsafe fn decode_string(s: &mut BufferedScanner, len: usize) -> SDSSResult<String> {
            String::from_utf8(s.next_chunk_variable(len).to_owned())
                .map_err(|_| SDSSError::InternalDecodeStructureCorruptedPayload)
        }
    }
}
