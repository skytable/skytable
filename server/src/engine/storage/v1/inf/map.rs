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
        dec_md, PersistDictEntryDscr, PersistMapSpec, PersistObjectHlIO, PersistObjectMD, VecU8,
        VoidMetadata,
    },
    crate::{
        engine::{
            data::{
                cell::Datacell,
                dict::DictEntryGeneric,
                tag::{CUTag, DataTag, TagClass, TagUnique},
            },
            storage::v1::{rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::{copy_slice_to_array as memcpy, EndianQW},
    },
    core::marker::PhantomData,
    std::{cmp, collections::HashMap},
};

/// This is more of a lazy hack than anything sensible. Just implement a spec and then use this wrapper for any enc/dec operations
pub struct PersistMapImpl<M: PersistMapSpec>(PhantomData<M>);

impl<M: PersistMapSpec> PersistObjectHlIO for PersistMapImpl<M> {
    const ALWAYS_VERIFY_PAYLOAD_USING_MD: bool = false;
    type Type = HashMap<M::Key, M::Value>;
    type Metadata = VoidMetadata;
    fn pe_obj_hlio_enc(buf: &mut VecU8, v: &Self::Type) {
        enc_dict_into_buffer::<M>(buf, v)
    }
    unsafe fn pe_obj_hlio_dec(
        scanner: &mut BufferedScanner,
        _: VoidMetadata,
    ) -> SDSSResult<Self::Type> {
        dec_dict::<M>(scanner)
    }
}

/// Encode the dict into the given buffer
pub fn enc_dict_into_buffer<PM: PersistMapSpec>(
    buf: &mut VecU8,
    map: &HashMap<PM::Key, PM::Value>,
) {
    buf.extend(map.len().u64_bytes_le());
    for (key, val) in map {
        PM::entry_md_enc(buf, key, val);
        if PM::ENC_COUPLED {
            PM::enc_entry(buf, key, val);
        } else {
            PM::enc_key(buf, key);
            PM::enc_val(buf, val);
        }
    }
}

/// Decode the dict using the given buffered scanner
pub fn dec_dict<PM: PersistMapSpec>(
    scanner: &mut BufferedScanner,
) -> SDSSResult<HashMap<PM::Key, PM::Value>> {
    if !(PM::meta_dec_collection_pretest(scanner) & scanner.has_left(sizeof!(u64))) {
        return Err(SDSSError::InternalDecodeStructureCorrupted);
    }
    let size = unsafe {
        // UNSAFE(@ohsayan): pretest
        scanner.next_u64_le() as usize
    };
    let mut dict = HashMap::with_capacity(size);
    while PM::meta_dec_entry_pretest(scanner) & (dict.len() != size) {
        let md = unsafe {
            // pretest
            dec_md::<PM::Metadata, true>(scanner)?
        };
        if PM::META_VERIFY_BEFORE_DEC && !md.pretest_src_for_object_dec(scanner) {
            return Err(SDSSError::InternalDecodeStructureCorrupted);
        }
        let key;
        let val;
        unsafe {
            if PM::DEC_COUPLED {
                match PM::dec_entry(scanner, md) {
                    Some((_k, _v)) => {
                        key = _k;
                        val = _v;
                    }
                    None => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
                }
            } else {
                let _k = PM::dec_key(scanner, &md);
                let _v = PM::dec_val(scanner, &md);
                match (_k, _v) {
                    (Some(_k), Some(_v)) => {
                        key = _k;
                        val = _v;
                    }
                    _ => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
                }
            }
        }
        if dict.insert(key, val).is_some() {
            return Err(SDSSError::InternalDecodeStructureIllegalData);
        }
    }
    if dict.len() == size {
        Ok(dict)
    } else {
        Err(SDSSError::InternalDecodeStructureIllegalData)
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

impl PersistObjectMD for GenericDictEntryMD {
    const MD_DEC_INFALLIBLE: bool = true;
    fn pretest_src_for_metadata_dec(scanner: &BufferedScanner) -> bool {
        scanner.has_left((sizeof!(u64) * 2) + 1)
    }
    unsafe fn dec_md_payload(scanner: &mut BufferedScanner) -> Option<Self> {
        Some(Self::decode(scanner.next_chunk()))
    }
    fn pretest_src_for_object_dec(&self, scanner: &BufferedScanner) -> bool {
        static EXPECT_ATLEAST: [u8; 4] = [0, 1, 8, 8]; // PAD to align
        let lbound_rem = self.klen + EXPECT_ATLEAST[cmp::min(self.dscr, 3) as usize] as usize;
        scanner.has_left(lbound_rem) & (self.dscr <= PersistDictEntryDscr::Dict.value_u8())
    }
}

impl PersistMapSpec for GenericDictSpec {
    type Key = Box<str>;
    type Value = DictEntryGeneric;
    type Metadata = GenericDictEntryMD;
    const DEC_COUPLED: bool = false;
    const ENC_COUPLED: bool = true;
    const META_VERIFY_BEFORE_DEC: bool = true;
    fn meta_dec_collection_pretest(_: &BufferedScanner) -> bool {
        true
    }
    fn meta_dec_entry_pretest(scanner: &BufferedScanner) -> bool {
        // we just need to see if we can decode the entry metadata
        Self::Metadata::pretest_src_for_metadata_dec(scanner)
    }
    fn entry_md_enc(buf: &mut VecU8, key: &Self::Key, _: &Self::Value) {
        buf.extend(key.len().u64_bytes_le());
    }
    unsafe fn entry_md_dec(scanner: &mut BufferedScanner) -> Option<Self::Metadata> {
        Some(Self::Metadata::decode(scanner.next_chunk()))
    }
    fn enc_entry(buf: &mut VecU8, key: &Self::Key, val: &Self::Value) {
        match val {
            DictEntryGeneric::Map(map) => {
                buf.push(PersistDictEntryDscr::Dict.value_u8());
                buf.extend(key.as_bytes());
                enc_dict_into_buffer::<Self>(buf, map);
            }
            DictEntryGeneric::Lit(dc) => {
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
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Key> {
        String::from_utf8(scanner.next_chunk_variable(md.klen).to_owned())
            .map(|s| s.into_boxed_str())
            .ok()
    }
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Value> {
        unsafe fn decode_element(
            scanner: &mut BufferedScanner,
            dscr: PersistDictEntryDscr,
            dg_top_element: bool,
        ) -> Option<DictEntryGeneric> {
            let r = match dscr {
                PersistDictEntryDscr::Null => DictEntryGeneric::Lit(Datacell::null()),
                PersistDictEntryDscr::Bool => {
                    DictEntryGeneric::Lit(Datacell::new_bool(scanner.next_byte() == 1))
                }
                PersistDictEntryDscr::UnsignedInt
                | PersistDictEntryDscr::SignedInt
                | PersistDictEntryDscr::Float => DictEntryGeneric::Lit(Datacell::new_qw(
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
                    DictEntryGeneric::Lit(if dscr == PersistDictEntryDscr::Str {
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
                                Some(DictEntryGeneric::Lit(l)) => l,
                                None => return None,
                                _ => unreachable!("found top-level dict item in datacell"),
                            },
                        );
                    }
                    if v.len() == list_len {
                        DictEntryGeneric::Lit(Datacell::new_list(v))
                    } else {
                        return None;
                    }
                }
                PersistDictEntryDscr::Dict => {
                    if dg_top_element {
                        DictEntryGeneric::Map(dec_dict::<GenericDictSpec>(scanner).ok()?)
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
        _: Self::Metadata,
    ) -> Option<(Self::Key, Self::Value)> {
        unimplemented!()
    }
}
