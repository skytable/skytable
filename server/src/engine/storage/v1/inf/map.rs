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
    super::{obj::FieldMD, PersistDictEntryDscr, PersistMapSpec, PersistObject, VecU8},
    crate::{
        engine::{
            core::model::Field,
            data::{
                cell::Datacell,
                dict::DictEntryGeneric,
                tag::{CUTag, DataTag, TagClass, TagUnique},
                DictGeneric,
            },
            idx::{IndexBaseSpec, IndexSTSeqCns, STIndex, STIndexSeq},
            storage::v1::{inf, rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::{copy_slice_to_array as memcpy, EndianQW},
    },
    core::marker::PhantomData,
    std::cmp,
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
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
        Ok(MapIndexSizeMD(
            u64::from_le_bytes(scanner.next_chunk()) as usize
        ))
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
    ) -> SDSSResult<Self::OutputType> {
        let mut dict = M::MapType::idx_init();
        while M::pretest_entry_metadata(scanner) & (dict.st_len() != dict_size) {
            let md = unsafe {
                // UNSAFE(@ohsayan): +pretest
                M::entry_md_dec(scanner)
                    .ok_or(SDSSError::InternalDecodeStructureCorruptedPayload)?
            };
            if !M::pretest_entry_data(scanner, &md) {
                return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
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
                        None => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
                    }
                } else {
                    let _k = M::dec_key(scanner, &md);
                    let _v = M::dec_val(scanner, &md);
                    match (_k, _v) {
                        (Some(_k), Some(_v)) => {
                            key = _k;
                            val = _v;
                        }
                        _ => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
                    }
                }
            }
            if !dict.st_insert(key, val) {
                return Err(SDSSError::InternalDecodeStructureIllegalData);
            }
        }
        if dict.st_len() == dict_size {
            Ok(dict)
        } else {
            Err(SDSSError::InternalDecodeStructureIllegalData)
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
    /// encode md
    pub(crate) fn encode(klen: usize, dscr: u8) -> [u8; 9] {
        let mut ret = [0u8; 9];
        ret[..8].copy_from_slice(&klen.u64_bytes_le());
        ret[8] = dscr;
        ret
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
        static EXPECT_ATLEAST: [u8; 4] = [0, 1, 8, 8]; // PAD to align
        let lbound_rem = md.klen + EXPECT_ATLEAST[cmp::min(md.dscr, 3) as usize] as usize;
        scanner.has_left(lbound_rem) & (md.dscr <= PersistDictEntryDscr::Dict.value_u8())
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
                buf.push(PersistDictEntryDscr::Dict.value_u8());
                buf.extend(key.as_bytes());
                <PersistMapImpl<Self> as PersistObject>::default_full_enc(buf, map);
            }
            DictEntryGeneric::Data(dc) => {
                buf.push(
                    PersistDictEntryDscr::translate_from_class(dc.tag().tag_class()).value_u8()
                        * (!dc.is_null() as u8),
                );
                buf.extend(key.as_bytes());
                fn encode_element(buf: &mut VecU8, dc: &Datacell) {
                    unsafe {
                        use TagClass::*;
                        match dc.tag().tag_class() {
                            Bool if dc.is_init() => buf.push(dc.read_bool() as u8),
                            Bool => {}
                            UnsignedInt | SignedInt | Float => {
                                buf.extend(dc.read_uint().to_le_bytes())
                            }
                            Str | Bin => {
                                let slc = dc.read_bin();
                                buf.extend(slc.len().u64_bytes_le());
                                buf.extend(slc);
                            }
                            List => {
                                let lst = dc.read_list().read();
                                buf.extend(lst.len().u64_bytes_le());
                                for item in lst.iter() {
                                    encode_element(buf, item);
                                }
                            }
                        }
                    }
                }
                encode_element(buf, dc);
            }
        }
    }
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Key> {
        inf::dec::utils::decode_string(scanner, md.klen as usize)
            .map(|s| s.into_boxed_str())
            .ok()
    }
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::EntryMD) -> Option<Self::Value> {
        unsafe fn decode_element(
            scanner: &mut BufferedScanner,
            dscr: PersistDictEntryDscr,
            dg_top_element: bool,
        ) -> Option<DictEntryGeneric> {
            let r = match dscr {
                PersistDictEntryDscr::Null => DictEntryGeneric::Data(Datacell::null()),
                PersistDictEntryDscr::Bool => {
                    DictEntryGeneric::Data(Datacell::new_bool(scanner.next_byte() == 1))
                }
                PersistDictEntryDscr::UnsignedInt
                | PersistDictEntryDscr::SignedInt
                | PersistDictEntryDscr::Float => DictEntryGeneric::Data(Datacell::new_qw(
                    scanner.next_u64_le(),
                    CUTag::new(
                        dscr.into_class(),
                        [
                            TagUnique::UnsignedInt,
                            TagUnique::SignedInt,
                            TagUnique::Illegal,
                            TagUnique::Illegal, // pad
                        ][(dscr.value_u8() - 2) as usize],
                    ),
                )),
                PersistDictEntryDscr::Str | PersistDictEntryDscr::Bin => {
                    let slc_len = scanner.next_u64_le() as usize;
                    if !scanner.has_left(slc_len) {
                        return None;
                    }
                    let slc = scanner.next_chunk_variable(slc_len);
                    DictEntryGeneric::Data(if dscr == PersistDictEntryDscr::Str {
                        if core::str::from_utf8(slc).is_err() {
                            return None;
                        }
                        Datacell::new_str(
                            String::from_utf8_unchecked(slc.to_owned()).into_boxed_str(),
                        )
                    } else {
                        Datacell::new_bin(slc.to_owned().into_boxed_slice())
                    })
                }
                PersistDictEntryDscr::List => {
                    let list_len = scanner.next_u64_le() as usize;
                    let mut v = Vec::with_capacity(list_len);
                    while (!scanner.eof()) & (v.len() < list_len) {
                        let dscr = scanner.next_byte();
                        if dscr > PersistDictEntryDscr::Dict.value_u8() {
                            return None;
                        }
                        v.push(
                            match decode_element(
                                scanner,
                                PersistDictEntryDscr::from_raw(dscr),
                                false,
                            ) {
                                Some(DictEntryGeneric::Data(l)) => l,
                                None => return None,
                                _ => unreachable!("found top-level dict item in datacell"),
                            },
                        );
                    }
                    if v.len() == list_len {
                        DictEntryGeneric::Data(Datacell::new_list(v))
                    } else {
                        return None;
                    }
                }
                PersistDictEntryDscr::Dict => {
                    if dg_top_element {
                        DictEntryGeneric::Map(
                            <PersistMapImpl<GenericDictSpec> as PersistObject>::default_full_dec(
                                scanner,
                            )
                            .ok()?,
                        )
                    } else {
                        unreachable!("found top-level dict item in datacell")
                    }
                }
            };
            Some(r)
        }
        decode_element(scanner, PersistDictEntryDscr::from_raw(md.dscr), true)
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
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
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
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
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
