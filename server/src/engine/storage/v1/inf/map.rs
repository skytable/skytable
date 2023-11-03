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
        PersistMapSpec, PersistObject, VecU8,
    },
    crate::{
        engine::{
            core::model::Field,
            data::{dict::DictEntryGeneric, DictGeneric},
            error::{RuntimeResult, StorageError},
            idx::{IndexBaseSpec, IndexSTSeqCns, STIndex, STIndexSeq},
            mem::BufferedScanner,
            storage::v1::inf,
        },
        util::{copy_slice_to_array as memcpy, EndianQW},
    },
    core::marker::PhantomData,
};

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct MapIndexSizeMD(pub usize);

/// This is more of a lazy hack than anything sensible. Just implement a spec and then use this wrapper for any enc/dec operations
pub struct PersistMapImpl<'a, M: PersistMapSpec>(PhantomData<&'a M::MapType>);

impl<'a, M: PersistMapSpec> PersistObject for PersistMapImpl<'a, M>
where
    M::MapType: 'a + STIndex<M::Key, M::Value>,
{
    const METADATA_SIZE: usize = sizeof!(u64);
    type InputType = &'a M::MapType;
    type OutputType = M::MapType;
    type Metadata = MapIndexSizeMD;
    fn pretest_can_dec_object(
        s: &BufferedScanner,
        MapIndexSizeMD(dict_size): &Self::Metadata,
    ) -> bool {
        M::pretest_collection_using_size(s, *dict_size)
    }
    fn meta_enc(buf: &mut VecU8, data: Self::InputType) {
        buf.extend(data.st_len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(MapIndexSizeMD(scanner.next_u64_le() as usize))
    }
    fn obj_enc(buf: &mut VecU8, map: Self::InputType) {
        for (key, val) in M::_get_iter(map) {
            M::entry_md_enc(buf, key, val);
            if M::ENC_COUPLED {
                M::enc_entry(buf, key, val);
            } else {
                M::enc_key(buf, key);
                M::enc_val(buf, val);
            }
        }
    }
    unsafe fn obj_dec(
        scanner: &mut BufferedScanner,
        MapIndexSizeMD(dict_size): Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let mut dict = M::MapType::idx_init();
        while M::pretest_entry_metadata(scanner) & (dict.st_len() != dict_size) {
            let md = unsafe {
                // UNSAFE(@ohsayan): +pretest
                M::entry_md_dec(scanner).ok_or::<StorageError>(
                    StorageError::InternalDecodeStructureCorruptedPayload.into(),
                )?
            };
            if !M::pretest_entry_data(scanner, &md) {
                return Err(StorageError::InternalDecodeStructureCorruptedPayload.into());
            }
            let key;
            let val;
            unsafe {
                if M::DEC_COUPLED {
                    match M::dec_entry(scanner, md) {
                        Some((_k, _v)) => {
                            key = _k;
                            val = _v;
                        }
                        None => {
                            return Err(StorageError::InternalDecodeStructureCorruptedPayload.into())
                        }
                    }
                } else {
                    let _k = M::dec_key(scanner, &md);
                    let _v = M::dec_val(scanner, &md);
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
            if !dict.st_insert(key, val) {
                return Err(StorageError::InternalDecodeStructureIllegalData.into());
            }
        }
        if dict.st_len() == dict_size {
            Ok(dict)
        } else {
            Err(StorageError::InternalDecodeStructureIllegalData.into())
        }
    }
}

/// generic dict spec (simple spec for [DictGeneric](crate::engine::data::dict::DictGeneric))
pub struct GenericDictSpec;

/// generic dict entry metadata
pub struct GenericDictEntryMD {
    pub(crate) dscr: u8,
    pub(crate) klen: usize,
}

impl GenericDictEntryMD {
    /// decode md (no need for any validation since that has to be handled later and can only produce incorrect results
    /// if unsafe code is used to translate an incorrect dscr)
    pub(crate) fn decode(data: [u8; 9]) -> Self {
        Self {
            klen: u64::from_le_bytes(memcpy(&data[..8])) as usize,
            dscr: data[8],
        }
    }
}

impl PersistMapSpec for GenericDictSpec {
    type MapIter<'a> = std::collections::hash_map::Iter<'a, Box<str>, DictEntryGeneric>;
    type MapType = DictGeneric;
    type Key = Box<str>;
    type Value = DictEntryGeneric;
    type EntryMD = GenericDictEntryMD;
    const DEC_COUPLED: bool = false;
    const ENC_COUPLED: bool = true;
    fn _get_iter<'a>(map: &'a Self::MapType) -> Self::MapIter<'a> {
        map.iter()
    }
    fn pretest_entry_metadata(scanner: &BufferedScanner) -> bool {
        // we just need to see if we can decode the entry metadata
        scanner.has_left(9)
    }
    fn pretest_entry_data(scanner: &BufferedScanner, md: &Self::EntryMD) -> bool {
        StorageCellTypeID::is_valid(md.dscr)
            & scanner.has_left(StorageCellTypeID::expect_atleast(md.dscr))
    }
    fn entry_md_enc(buf: &mut VecU8, key: &Self::Key, _: &Self::Value) {
        buf.extend(key.len().u64_bytes_le());
    }
    unsafe fn entry_md_dec(scanner: &mut BufferedScanner) -> Option<Self::EntryMD> {
        Some(Self::EntryMD::decode(scanner.next_chunk()))
    }
    fn enc_entry(buf: &mut VecU8, key: &Self::Key, val: &Self::Value) {
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
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Key> {
        inf::dec::utils::decode_string(scanner, md.klen as usize)
            .map(|s| s.into_boxed_str())
            .ok()
    }
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Value> {
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
    // not implemented
    fn enc_key(_: &mut VecU8, _: &Self::Key) {
        unimplemented!()
    }
    fn enc_val(_: &mut VecU8, _: &Self::Value) {
        unimplemented!()
    }
    unsafe fn dec_entry(
        _: &mut BufferedScanner,
        _: Self::EntryMD,
    ) -> Option<(Self::Key, Self::Value)> {
        unimplemented!()
    }
}

pub struct FieldMapSpec;
pub struct FieldMapEntryMD {
    field_id_l: u64,
    field_prop_c: u64,
    field_layer_c: u64,
    null: u8,
}

impl FieldMapEntryMD {
    const fn new(field_id_l: u64, field_prop_c: u64, field_layer_c: u64, null: u8) -> Self {
        Self {
            field_id_l,
            field_prop_c,
            field_layer_c,
            null,
        }
    }
}

impl PersistMapSpec for FieldMapSpec {
    type MapIter<'a> = crate::engine::idx::IndexSTSeqDllIterOrdKV<'a, Box<str>, Field>;
    type MapType = IndexSTSeqCns<Self::Key, Self::Value>;
    type EntryMD = FieldMapEntryMD;
    type Key = Box<str>;
    type Value = Field;
    const ENC_COUPLED: bool = false;
    const DEC_COUPLED: bool = false;
    fn _get_iter<'a>(m: &'a Self::MapType) -> Self::MapIter<'a> {
        m.stseq_ord_kv()
    }
    fn pretest_entry_metadata(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64, 3) + 1)
    }
    fn pretest_entry_data(scanner: &BufferedScanner, md: &Self::EntryMD) -> bool {
        scanner.has_left(md.field_id_l as usize) // TODO(@ohsayan): we can enforce way more here such as atleast one field etc
    }
    fn entry_md_enc(buf: &mut VecU8, key: &Self::Key, val: &Self::Value) {
        buf.extend(key.len().u64_bytes_le());
        buf.extend(0u64.to_le_bytes()); // TODO(@ohsayan): props
        buf.extend(val.layers().len().u64_bytes_le());
        buf.push(val.is_nullable() as u8);
    }
    unsafe fn entry_md_dec(scanner: &mut BufferedScanner) -> Option<Self::EntryMD> {
        Some(FieldMapEntryMD::new(
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_byte(),
        ))
    }
    fn enc_key(buf: &mut VecU8, key: &Self::Key) {
        buf.extend(key.as_bytes());
    }
    fn enc_val(buf: &mut VecU8, val: &Self::Value) {
        for layer in val.layers() {
            super::obj::LayerRef::default_full_enc(buf, super::obj::LayerRef(layer))
        }
    }
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Key> {
        inf::dec::utils::decode_string(scanner, md.field_id_l as usize)
            .map(|s| s.into_boxed_str())
            .ok()
    }
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Value> {
        super::obj::FieldRef::obj_dec(
            scanner,
            FieldMD::new(md.field_prop_c, md.field_layer_c, md.null),
        )
        .ok()
    }
    // unimplemented
    fn enc_entry(_: &mut VecU8, _: &Self::Key, _: &Self::Value) {
        unimplemented!()
    }
    unsafe fn dec_entry(
        _: &mut BufferedScanner,
        _: Self::EntryMD,
    ) -> Option<(Self::Key, Self::Value)> {
        unimplemented!()
    }
}

// TODO(@ohsayan): common trait for k/v associations, independent of underlying maptype
pub struct FieldMapSpecST;
impl PersistMapSpec for FieldMapSpecST {
    type MapIter<'a> = std::collections::hash_map::Iter<'a, Box<str>, Field>;
    type MapType = std::collections::HashMap<Box<str>, Field>;
    type EntryMD = FieldMapEntryMD;
    type Key = Box<str>;
    type Value = Field;
    const ENC_COUPLED: bool = false;
    const DEC_COUPLED: bool = false;
    fn _get_iter<'a>(m: &'a Self::MapType) -> Self::MapIter<'a> {
        m.iter()
    }
    fn pretest_entry_metadata(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64, 3) + 1)
    }
    fn pretest_entry_data(scanner: &BufferedScanner, md: &Self::EntryMD) -> bool {
        scanner.has_left(md.field_id_l as usize) // TODO(@ohsayan): we can enforce way more here such as atleast one field etc
    }
    fn entry_md_enc(buf: &mut VecU8, key: &Self::Key, val: &Self::Value) {
        buf.extend(key.len().u64_bytes_le());
        buf.extend(0u64.to_le_bytes()); // TODO(@ohsayan): props
        buf.extend(val.layers().len().u64_bytes_le());
        buf.push(val.is_nullable() as u8);
    }
    unsafe fn entry_md_dec(scanner: &mut BufferedScanner) -> Option<Self::EntryMD> {
        Some(FieldMapEntryMD::new(
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_byte(),
        ))
    }
    fn enc_key(buf: &mut VecU8, key: &Self::Key) {
        buf.extend(key.as_bytes());
    }
    fn enc_val(buf: &mut VecU8, val: &Self::Value) {
        for layer in val.layers() {
            super::obj::LayerRef::default_full_enc(buf, super::obj::LayerRef(layer))
        }
    }
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Key> {
        inf::dec::utils::decode_string(scanner, md.field_id_l as usize)
            .map(|s| s.into_boxed_str())
            .ok()
    }
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Value> {
        super::obj::FieldRef::obj_dec(
            scanner,
            FieldMD::new(md.field_prop_c, md.field_layer_c, md.null),
        )
        .ok()
    }
    // unimplemented
    fn enc_entry(_: &mut VecU8, _: &Self::Key, _: &Self::Value) {
        unimplemented!()
    }
    unsafe fn dec_entry(
        _: &mut BufferedScanner,
        _: Self::EntryMD,
    ) -> Option<(Self::Key, Self::Value)> {
        unimplemented!()
    }
}
