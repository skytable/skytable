/*
 * Created on Wed Aug 16 2023
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

use {
    super::{
        obj::{
            cell::{self, CanYieldDict, StorageCellTypeID},
            FieldMD,
        },
        AbstractMap, MapStorageSpec, PersistObject, VecU8,
    },
    crate::{
        engine::{
            core::model::Field,
            data::dict::DictEntryGeneric,
            error::{RuntimeResult, StorageError},
            idx::{IndexSTSeqCns, STIndexSeq},
            mem::{unsafe_apis::BoxStr, BufferedScanner, StatelessLen},
        },
        util::{compiler::TaggedEnum, copy_slice_to_array as memcpy, EndianQW},
    },
    std::{collections::HashMap, marker::PhantomData},
};

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct MapIndexSizeMD(pub usize);

/// This is more of a lazy hack than anything sensible. Just implement a spec and then use this wrapper for any enc/dec operations
pub struct PersistMapImpl<'a, M: MapStorageSpec>(PhantomData<&'a M::InMemoryMap>);

impl<'a, M: MapStorageSpec> PersistObject for PersistMapImpl<'a, M> {
    const METADATA_SIZE: usize = sizeof!(u64);
    type InputType = &'a M::InMemoryMap;
    type OutputType = M::RestoredMap;
    type Metadata = MapIndexSizeMD;
    fn pretest_can_dec_object(
        s: &BufferedScanner,
        MapIndexSizeMD(dict_size): &Self::Metadata,
    ) -> bool {
        M::decode_pretest_for_map(s, *dict_size)
    }
    fn meta_enc(buf: &mut VecU8, data: Self::InputType) {
        buf.extend(data.stateless_len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(MapIndexSizeMD(scanner.next_u64_le() as usize))
    }
    fn obj_enc(buf: &mut VecU8, map: Self::InputType) {
        for (key, val) in M::get_iter_from_memory(map) {
            M::encode_entry_meta(buf, key, val);
            if M::ENC_AS_ENTRY {
                M::encode_entry_data(buf, key, val);
            } else {
                M::encode_entry_key(buf, key);
                M::encode_entry_val(buf, val);
            }
        }
    }
    unsafe fn obj_dec(
        scanner: &mut BufferedScanner,
        MapIndexSizeMD(dict_size): Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let mut dict = M::RestoredMap::map_new();
        let decode_pretest_for_entry_meta = M::decode_pretest_for_entry_meta(scanner);
        while decode_pretest_for_entry_meta & (dict.map_length() != dict_size) {
            let md = unsafe {
                // UNSAFE(@ohsayan): +pretest
                M::decode_entry_meta(scanner).ok_or::<StorageError>(
                    StorageError::InternalDecodeStructureCorruptedPayload.into(),
                )?
            };
            if !M::decode_pretest_for_entry_data(scanner, &md) {
                return Err(StorageError::InternalDecodeStructureCorruptedPayload.into());
            }
            let key;
            let val;
            unsafe {
                if M::DEC_AS_ENTRY {
                    match M::decode_entry_data(scanner, md) {
                        Some((_k, _v)) => {
                            key = _k;
                            val = _v;
                        }
                        None => {
                            return Err(StorageError::InternalDecodeStructureCorruptedPayload.into())
                        }
                    }
                } else {
                    let _k = M::decode_entry_key(scanner, &md);
                    let _v = M::decode_entry_val(scanner, &md);
                    match (_k, _v) {
                        (Some(_k), Some(_v)) => {
                            key = _k;
                            val = _v;
                        }
                        _ => {
                            return Err(StorageError::InternalDecodeStructureCorruptedPayload.into())
                        }
                    }
                }
            }
            if !dict.map_insert(key, val) {
                return Err(StorageError::InternalDecodeStructureIllegalData.into());
            }
        }
        if dict.map_length() == dict_size {
            Ok(dict)
        } else {
            Err(StorageError::InternalDecodeStructureIllegalData.into())
        }
    }
}

/// generic dict spec (simple spec for [DictGeneric](crate::engine::data::dict::DictGeneric))
pub struct GenericDictSpec;

/// generic dict entry metadata
pub struct GenericDictEntryMetadata {
    pub(crate) klen: usize,
    pub(crate) dscr: u8,
}

impl GenericDictEntryMetadata {
    /// decode md (no need for any validation since that has to be handled later and can only produce incorrect results
    /// if unsafe code is used to translate an incorrect dscr)
    pub(crate) fn decode(data: [u8; 9]) -> Self {
        Self {
            klen: u64::from_le_bytes(memcpy(&data[..8])) as usize,
            dscr: data[8],
        }
    }
}

impl MapStorageSpec for GenericDictSpec {
    type InMemoryMap = HashMap<Self::InMemoryKey, Self::InMemoryVal>;
    type InMemoryKey = BoxStr;
    type InMemoryVal = DictEntryGeneric;
    type InMemoryMapIter<'a> =
        std::collections::hash_map::Iter<'a, Self::InMemoryKey, Self::InMemoryVal>;
    type RestoredKey = Self::InMemoryKey;
    type RestoredMap = Self::InMemoryMap;
    type RestoredVal = Self::InMemoryVal;
    type EntryMetadata = GenericDictEntryMetadata;
    const DEC_AS_ENTRY: bool = false;
    const ENC_AS_ENTRY: bool = true;
    fn get_iter_from_memory<'a>(map: &'a Self::InMemoryMap) -> Self::InMemoryMapIter<'a> {
        map.iter()
    }
    fn encode_entry_meta(buf: &mut VecU8, key: &Self::InMemoryKey, _: &Self::InMemoryVal) {
        buf.extend(key.len().u64_bytes_le());
    }
    fn encode_entry_data(buf: &mut VecU8, key: &Self::InMemoryKey, val: &Self::InMemoryVal) {
        match val {
            DictEntryGeneric::Map(map) => {
                buf.push(StorageCellTypeID::Dict.value_u8());
                buf.extend(key.as_bytes());
                <PersistMapImpl<Self> as PersistObject>::default_full_enc(buf, map);
            }
            DictEntryGeneric::Data(dc) => {
                buf.push(cell::encode_tag(dc));
                buf.extend(key.as_bytes());
                cell::encode_cell(buf, dc);
            }
        }
    }
    fn encode_entry_key(_: &mut VecU8, _: &Self::InMemoryKey) {
        unimplemented!()
    }
    fn encode_entry_val(_: &mut VecU8, _: &Self::InMemoryVal) {
        unimplemented!()
    }
    fn decode_pretest_for_entry_meta(scanner: &mut BufferedScanner) -> bool {
        // we just need to see if we can decode the entry metadata
        scanner.has_left(9)
    }
    fn decode_pretest_for_entry_data(s: &mut BufferedScanner, md: &Self::EntryMetadata) -> bool {
        StorageCellTypeID::is_valid(md.dscr)
            & s.has_left(StorageCellTypeID::expect_atleast(md.dscr))
    }
    unsafe fn decode_entry_meta(s: &mut BufferedScanner) -> Option<Self::EntryMetadata> {
        Some(Self::EntryMetadata::decode(s.next_chunk()))
    }
    unsafe fn decode_entry_data(
        _: &mut BufferedScanner,
        _: Self::EntryMetadata,
    ) -> Option<(Self::RestoredKey, Self::RestoredVal)> {
        unimplemented!()
    }
    unsafe fn decode_entry_key(
        s: &mut BufferedScanner,
        md: &Self::EntryMetadata,
    ) -> Option<Self::RestoredKey> {
        super::dec::utils::decode_string_into(s, md.klen as usize, BoxStr::new).ok()
    }
    unsafe fn decode_entry_val(
        scanner: &mut BufferedScanner,
        md: &Self::EntryMetadata,
    ) -> Option<Self::RestoredVal> {
        Some(
            match cell::decode_element::<CanYieldDict, BufferedScanner>(
                scanner,
                StorageCellTypeID::from_raw(md.dscr),
            )
            .ok()?
            {
                CanYieldDict::Data(d) => DictEntryGeneric::Data(d),
                CanYieldDict::Dict => DictEntryGeneric::Map(
                    <PersistMapImpl<GenericDictSpec> as PersistObject>::default_full_dec(scanner)
                        .ok()?,
                ),
            },
        )
    }
}

pub struct FieldMapEntryMetadata {
    field_id_l: u64,
    field_prop_c: u64,
    field_layer_c: u64,
    null: u8,
}

impl FieldMapEntryMetadata {
    const fn new(field_id_l: u64, field_prop_c: u64, field_layer_c: u64, null: u8) -> Self {
        Self {
            field_id_l,
            field_prop_c,
            field_layer_c,
            null,
        }
    }
}

pub trait FieldMapAny: StatelessLen {
    type Iterator<'a>: Iterator<Item = (&'a str, &'a Field)>
    where
        Self: 'a;
    fn get_iter<'a>(&'a self) -> Self::Iterator<'a>
    where
        Self: 'a;
}

impl FieldMapAny for HashMap<BoxStr, Field> {
    type Iterator<'a> = std::iter::Map<
        std::collections::hash_map::Iter<'a, BoxStr, Field>,
        fn((&BoxStr, &Field)) -> (&'a str, &'a Field),
    >;
    fn get_iter<'a>(&'a self) -> Self::Iterator<'a>
    where
        Self: 'a,
    {
        self.iter()
            .map(|(a, b)| unsafe { core::mem::transmute((a.as_ref(), b)) })
    }
}
impl FieldMapAny for IndexSTSeqCns<crate::engine::mem::RawStr, Field> {
    type Iterator<'a> = std::iter::Map<
    crate::engine::idx::stdord_iter::IndexSTSeqDllIterOrdKV<'a, crate::engine::mem::RawStr, Field>,
    fn((&crate::engine::mem::RawStr, &Field)) -> (&'a str, &'a Field)>
    where
        Self: 'a;

    fn get_iter<'a>(&'a self) -> Self::Iterator<'a>
    where
        Self: 'a,
    {
        self.stseq_ord_kv()
            .map(|(k, v)| unsafe { core::mem::transmute((k.as_str(), v)) })
    }
}
impl FieldMapAny for IndexSTSeqCns<BoxStr, Field> {
    type Iterator<'a> = std::iter::Map<
    crate::engine::idx::stdord_iter::IndexSTSeqDllIterOrdKV<'a, BoxStr, Field>,
    fn((&BoxStr, &Field)) -> (&'a str, &'a Field)>
    where
        Self: 'a;

    fn get_iter<'a>(&'a self) -> Self::Iterator<'a>
    where
        Self: 'a,
    {
        self.stseq_ord_kv()
            .map(|(k, v)| unsafe { core::mem::transmute((k.as_ref(), v)) })
    }
}

pub struct FieldMapSpec<FM>(PhantomData<FM>);
impl<FM: FieldMapAny> MapStorageSpec for FieldMapSpec<FM> {
    type InMemoryMap = FM;
    type InMemoryKey = str;
    type InMemoryVal = Field;
    type InMemoryMapIter<'a> = FM::Iterator<'a> where FM: 'a;
    type RestoredKey = BoxStr;
    type RestoredVal = Field;
    type RestoredMap = IndexSTSeqCns<BoxStr, Field>;
    type EntryMetadata = FieldMapEntryMetadata;
    const ENC_AS_ENTRY: bool = false;
    const DEC_AS_ENTRY: bool = false;
    fn get_iter_from_memory<'a>(map: &'a Self::InMemoryMap) -> Self::InMemoryMapIter<'a> {
        map.get_iter()
    }
    fn encode_entry_meta(buf: &mut VecU8, key: &Self::InMemoryKey, val: &Self::InMemoryVal) {
        buf.extend(key.len().u64_bytes_le());
        buf.extend(0u64.to_le_bytes()); // TODO(@ohsayan): props
        buf.extend(val.layers().len().u64_bytes_le());
        buf.push(val.is_nullable() as u8);
    }
    fn encode_entry_data(_: &mut VecU8, _: &Self::InMemoryKey, _: &Self::InMemoryVal) {
        unimplemented!()
    }
    fn encode_entry_key(buf: &mut VecU8, key: &Self::InMemoryKey) {
        buf.extend(key.as_bytes());
    }
    fn encode_entry_val(buf: &mut VecU8, val: &Self::InMemoryVal) {
        for layer in val.layers() {
            super::obj::LayerRef::default_full_enc(buf, super::obj::LayerRef(layer))
        }
    }
    fn decode_pretest_for_entry_meta(scanner: &mut BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64, 3) + 1)
    }
    fn decode_pretest_for_entry_data(s: &mut BufferedScanner, md: &Self::EntryMetadata) -> bool {
        s.has_left(md.field_id_l as usize) // TODO(@ohsayan): we can enforce way more here such as atleast one field etc
    }
    unsafe fn decode_entry_meta(scanner: &mut BufferedScanner) -> Option<Self::EntryMetadata> {
        Some(FieldMapEntryMetadata::new(
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_byte(),
        ))
    }
    unsafe fn decode_entry_data(
        _: &mut BufferedScanner,
        _: Self::EntryMetadata,
    ) -> Option<(Self::RestoredKey, Self::RestoredVal)> {
        unimplemented!()
    }
    unsafe fn decode_entry_key(
        scanner: &mut BufferedScanner,
        md: &Self::EntryMetadata,
    ) -> Option<Self::RestoredKey> {
        super::dec::utils::decode_string_into(scanner, md.field_id_l as usize, BoxStr::new).ok()
    }
    unsafe fn decode_entry_val(
        scanner: &mut BufferedScanner,
        md: &Self::EntryMetadata,
    ) -> Option<Self::RestoredVal> {
        super::obj::FieldRef::obj_dec(
            scanner,
            FieldMD::new(md.field_prop_c, md.field_layer_c, md.null),
        )
        .ok()
    }
}
