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

//! High level interfaces (r1)
//!
//! This is revision 1 of high-level interface encoding.
//!

pub mod impls;
pub mod map;
pub mod obj;
// tests
#[cfg(test)]
mod tests;

use crate::engine::{
    error::{RuntimeResult, StorageError},
    idx::{AsKey, AsValue, STIndex},
    mem::{BufferedScanner, StatelessLen},
};

type VecU8 = Vec<u8>;

pub trait DataSource {
    type Error;
    fn has_remaining(&self, cnt: usize) -> bool;
    unsafe fn read_next_byte(&mut self) -> Result<u8, Self::Error>;
    unsafe fn read_next_block<const N: usize>(&mut self) -> Result<[u8; N], Self::Error>;
    unsafe fn read_next_u64_le(&mut self) -> Result<u64, Self::Error>;
    unsafe fn read_next_variable_block(&mut self, size: usize) -> Result<Vec<u8>, Self::Error>;
}

impl<'a> DataSource for BufferedScanner<'a> {
    type Error = ();
    fn has_remaining(&self, cnt: usize) -> bool {
        self.has_left(cnt)
    }
    unsafe fn read_next_byte(&mut self) -> Result<u8, Self::Error> {
        Ok(self.next_byte())
    }
    unsafe fn read_next_block<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        Ok(self.next_chunk())
    }
    unsafe fn read_next_u64_le(&mut self) -> Result<u64, Self::Error> {
        Ok(self.next_u64_le())
    }
    unsafe fn read_next_variable_block(&mut self, size: usize) -> Result<Vec<u8>, Self::Error> {
        Ok(self.next_chunk_variable(size).into())
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
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata>;
    // obj
    /// obj enc
    fn obj_enc(buf: &mut VecU8, data: Self::InputType);
    /// obj dec
    ///
    /// ## Safety
    ///
    /// Must pass the [`PersistObject::pretest_can_dec_object`] assertion
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType>;
    // default
    /// Default routine to encode an object + its metadata
    fn default_full_enc(buf: &mut VecU8, data: Self::InputType) {
        Self::meta_enc(buf, data);
        Self::obj_enc(buf, data);
    }
    /// Default routine to decode an object + its metadata (however, the metadata is used and not returned)
    fn default_full_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::OutputType> {
        if !Self::pretest_can_dec_metadata(scanner) {
            return Err(StorageError::InternalDecodeStructureCorrupted.into());
        }
        let md = unsafe {
            // UNSAFE(@ohsayan): +pretest
            Self::meta_dec(scanner)?
        };
        if !Self::pretest_can_dec_object(scanner, &md) {
            return Err(StorageError::InternalDecodeStructureCorruptedPayload.into());
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

pub trait AbstractMap<K, V> {
    fn map_new() -> Self;
    fn map_insert(&mut self, k: K, v: V) -> bool;
    fn map_length(&self) -> usize;
}

impl<K: AsKey, V: AsValue, M: STIndex<K, V>> AbstractMap<K, V> for M {
    fn map_new() -> Self {
        Self::idx_init()
    }
    fn map_insert(&mut self, k: K, v: V) -> bool {
        self.st_insert(k, v)
    }
    fn map_length(&self) -> usize {
        self.st_len()
    }
}

pub trait MapStorageSpec {
    // in memory
    type InMemoryMap: StatelessLen;
    type InMemoryKey: ?Sized;
    type InMemoryVal;
    type InMemoryMapIter<'a>: Iterator<Item = (&'a Self::InMemoryKey, &'a Self::InMemoryVal)>
    where
        Self: 'a,
        Self::InMemoryKey: 'a,
        Self::InMemoryVal: 'a;
    // from disk
    type RestoredKey: AsKey;
    type RestoredVal: AsValue;
    type RestoredMap: AbstractMap<Self::RestoredKey, Self::RestoredVal>;
    // metadata
    type EntryMetadata;
    // settings
    const ENC_AS_ENTRY: bool;
    const DEC_AS_ENTRY: bool;
    // iterator
    fn get_iter_from_memory<'a>(map: &'a Self::InMemoryMap) -> Self::InMemoryMapIter<'a>;
    // encode
    fn encode_entry_meta(buf: &mut VecU8, key: &Self::InMemoryKey, val: &Self::InMemoryVal);
    fn encode_entry_data(buf: &mut VecU8, key: &Self::InMemoryKey, val: &Self::InMemoryVal);
    fn encode_entry_key(buf: &mut VecU8, key: &Self::InMemoryKey);
    fn encode_entry_val(buf: &mut VecU8, val: &Self::InMemoryVal);
    // decode
    fn decode_pretest_for_map(_: &BufferedScanner, _: usize) -> bool {
        true
    }
    fn decode_pretest_for_entry_meta(scanner: &mut BufferedScanner) -> bool;
    fn decode_pretest_for_entry_data(s: &mut BufferedScanner, md: &Self::EntryMetadata) -> bool;
    unsafe fn decode_entry_meta(s: &mut BufferedScanner) -> Option<Self::EntryMetadata>;
    unsafe fn decode_entry_data(
        s: &mut BufferedScanner,
        md: Self::EntryMetadata,
    ) -> Option<(Self::RestoredKey, Self::RestoredVal)>;
    unsafe fn decode_entry_key(
        s: &mut BufferedScanner,
        md: &Self::EntryMetadata,
    ) -> Option<Self::RestoredKey>;
    unsafe fn decode_entry_val(
        s: &mut BufferedScanner,
        md: &Self::EntryMetadata,
    ) -> Option<Self::RestoredVal>;
}

// enc
pub mod enc {
    #[cfg(test)]
    use super::{map, MapStorageSpec};
    use super::{PersistObject, VecU8};
    // obj
    pub fn full<Obj: PersistObject>(obj: Obj::InputType) -> Vec<u8> {
        let mut v = vec![];
        full_into_buffer::<Obj>(&mut v, obj);
        v
    }
    pub fn full_into_buffer<Obj: PersistObject>(buf: &mut VecU8, obj: Obj::InputType) {
        Obj::default_full_enc(buf, obj)
    }
    #[cfg(test)]
    pub fn full_self<Obj: PersistObject<InputType = Obj>>(obj: Obj) -> Vec<u8> {
        full::<Obj>(obj)
    }
    // dict
    #[cfg(test)]
    pub fn full_dict<PM: MapStorageSpec>(dict: &PM::InMemoryMap) -> Vec<u8> {
        let mut v = vec![];
        full_dict_into_buffer::<PM>(&mut v, dict);
        v
    }
    #[cfg(test)]
    pub fn full_dict_into_buffer<PM: MapStorageSpec>(buf: &mut VecU8, dict: &PM::InMemoryMap) {
        <map::PersistMapImpl<PM> as PersistObject>::default_full_enc(buf, dict)
    }
}

// dec
pub mod dec {
    use {
        super::{map, MapStorageSpec, PersistObject},
        crate::engine::{error::RuntimeResult, mem::BufferedScanner},
    };
    // obj
    pub fn full<Obj: PersistObject>(data: &[u8]) -> RuntimeResult<Obj::OutputType> {
        let mut scanner = BufferedScanner::new(data);
        full_from_scanner::<Obj>(&mut scanner)
    }
    pub fn full_from_scanner<Obj: PersistObject>(
        scanner: &mut BufferedScanner,
    ) -> RuntimeResult<Obj::OutputType> {
        Obj::default_full_dec(scanner)
    }
    // dec
    pub fn dict_full<PM: MapStorageSpec>(data: &[u8]) -> RuntimeResult<PM::RestoredMap> {
        let mut scanner = BufferedScanner::new(data);
        dict_full_from_scanner::<PM>(&mut scanner)
    }
    fn dict_full_from_scanner<PM: MapStorageSpec>(
        scanner: &mut BufferedScanner,
    ) -> RuntimeResult<PM::RestoredMap> {
        <map::PersistMapImpl<PM> as PersistObject>::default_full_dec(scanner)
    }
    pub mod utils {
        use crate::engine::{
            error::{RuntimeResult, StorageError},
            mem::{unsafe_apis::BoxStr, BufferedScanner},
        };
        pub unsafe fn decode_string(s: &mut BufferedScanner, len: usize) -> RuntimeResult<String> {
            self::decode_string_into(s, len, |s| String::from(s))
        }
        pub unsafe fn decode_box_str(s: &mut BufferedScanner, len: usize) -> RuntimeResult<BoxStr> {
            self::decode_string_into(s, len, |s| BoxStr::new(s))
        }
        pub unsafe fn decode_string_into<T>(
            s: &mut BufferedScanner,
            l: usize,
            f: impl Fn(&str) -> T,
        ) -> RuntimeResult<T> {
            let r = core::str::from_utf8(s.next_chunk_variable(l))
                .map_err(|_| StorageError::InternalDecodeStructureCorruptedPayload)?;
            Ok(f(r))
        }
    }
}
