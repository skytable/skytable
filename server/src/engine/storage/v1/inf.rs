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

use {
    crate::{
        engine::{
            data::{
                cell::Datacell,
                dict::{DictEntryGeneric, DictGeneric},
                tag::{CUTag, DataTag, TagClass, TagUnique},
            },
            idx::{AsKey, AsValue},
            storage::v1::{rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::{copy_slice_to_array as memcpy, EndianQW},
    },
    std::{cmp, collections::HashMap, mem},
};

type VecU8 = Vec<u8>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, sky_macros::EnumMethods)]
#[repr(u8)]
/// Disambiguation for data
pub enum PersistDictEntryDscr {
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

impl PersistDictEntryDscr {
    /// translates the tag class definition into the dscr definition
    pub const fn translate_from_class(class: TagClass) -> Self {
        unsafe { Self::from_raw(class.d() + 1) }
    }
    pub const unsafe fn from_raw(v: u8) -> Self {
        core::mem::transmute(v)
    }
    pub fn new_from_dict_gen_entry(e: &DictEntryGeneric) -> Self {
        match e {
            DictEntryGeneric::Map(_) => Self::Dict,
            DictEntryGeneric::Lit(dc) => Self::translate_from_class(dc.tag().tag_class()),
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
    spec
*/

/// metadata spec for a persist dict
pub trait PersistDictEntryMetadata {
    /// Verify the state of scanner to ensure that it complies with the metadata
    fn verify_with_src(&self, scanner: &BufferedScanner) -> bool;
}

/// spec for a persist dict
pub trait PersistDict {
    /// type of key
    type Key: AsKey;
    /// type of value
    type Value: AsValue;
    /// metadata type
    type Metadata: PersistDictEntryMetadata;
    /// enc coupled (packed enc)
    const ENC_COUPLED: bool;
    /// during dec, ignore failure of the metadata parse (IMP: NOT the metadata src verification but the
    /// validity of the metadata itself) because it is handled later
    const DEC_ENTRYMD_INFALLIBLE: bool;
    /// dec coupled (packed dec)
    const DEC_COUPLED: bool;
    /// during dec, verify the md directly with the src instead of handing it over to the dec helpers
    const DEC_VERIFY_MD_WITH_SRC_STANDALONE: bool;
    // meta
    /// pretest for pre-entry stage
    fn metadec_pretest_routine(scanner: &BufferedScanner) -> bool;
    /// pretest for entry stage
    fn metadec_pretest_entry(scanner: &BufferedScanner) -> bool;
    /// enc md for an entry
    fn enc_entry_metadata(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// dec the entry metadata
    /// SAFETY: Must have passed entry pretest
    unsafe fn dec_entry_metadata(scanner: &mut BufferedScanner) -> Option<Self::Metadata>;
    // entry (coupled)
    /// enc a packed entry
    fn enc_entry_coupled(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// dec a packed entry
    /// SAFETY: must have verified metadata with src (unless explicitly skipped with the `DEC_VERIFY_MD_WITH_SRC_STANDALONE`)
    /// flag
    unsafe fn dec_entry_coupled(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> Option<(Self::Key, Self::Value)>;
    // entry (non-packed)
    /// enc key for a normal entry
    fn enc_key(buf: &mut VecU8, key: &Self::Key);
    /// dec normal entry key
    /// SAFETY: must have verified metadata with src (unless explicitly skipped with the `DEC_VERIFY_MD_WITH_SRC_STANDALONE`)
    /// flag
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Key>;
    /// enc val for a normal entry
    fn enc_val(buf: &mut VecU8, val: &Self::Value);
    /// dec normal entry val
    /// SAFETY: must have verified metadata with src (unless explicitly skipped with the `DEC_VERIFY_MD_WITH_SRC_STANDALONE`)
    /// flag
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Value>;
}

/*
    blanket
*/

pub fn encode_dict<Pd: PersistDict>(dict: &HashMap<Pd::Key, Pd::Value>) -> Vec<u8> {
    let mut v = vec![];
    _encode_dict::<Pd>(&mut v, dict);
    v
}

fn _encode_dict<Pd: PersistDict>(buf: &mut VecU8, dict: &HashMap<Pd::Key, Pd::Value>) {
    buf.extend(dict.len().u64_bytes_le());
    for (key, val) in dict {
        Pd::enc_entry_metadata(buf, key, val);
        if Pd::ENC_COUPLED {
            Pd::enc_entry_coupled(buf, key, val);
        } else {
            Pd::enc_key(buf, key);
            Pd::enc_val(buf, val);
        }
    }
}

pub fn decode_dict<Pd: PersistDict>(
    scanner: &mut BufferedScanner,
) -> SDSSResult<HashMap<Pd::Key, Pd::Value>> {
    if !(Pd::metadec_pretest_routine(scanner) & scanner.has_left(sizeof!(u64))) {
        return Err(SDSSError::InternalDecodeStructureCorrupted);
    }
    let dict_len = unsafe {
        // UNSAFE(@ohsayan): pretest
        scanner.next_u64_le() as usize
    };
    let mut dict = HashMap::with_capacity(dict_len);
    while Pd::metadec_pretest_entry(scanner) & (dict.len() < dict_len) {
        let md = unsafe {
            // UNSAFE(@ohsayan): this is compeletely because of the entry pretest
            match Pd::dec_entry_metadata(scanner) {
                Some(dec) => dec,
                None => {
                    if Pd::DEC_ENTRYMD_INFALLIBLE {
                        impossible!()
                    } else {
                        return Err(SDSSError::InternalDecodeStructureCorrupted);
                    }
                }
            }
        };
        if Pd::DEC_VERIFY_MD_WITH_SRC_STANDALONE && !md.verify_with_src(scanner) {
            return Err(SDSSError::InternalDecodeStructureCorrupted);
        }
        let k;
        let v;
        if Pd::DEC_COUPLED {
            match unsafe {
                // UNSAFE(@ohsayan): verified metadata
                Pd::dec_entry_coupled(scanner, md)
            } {
                Some((_k, _v)) => {
                    k = _k;
                    v = _v;
                }
                None => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
            }
        } else {
            match unsafe {
                // UNSAFE(@ohsayan): verified metadata
                (Pd::dec_key(scanner, &md), Pd::dec_val(scanner, &md))
            } {
                (Some(_k), Some(_v)) => {
                    k = _k;
                    v = _v;
                }
                _ => {
                    return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
                }
            }
        }
        if dict.insert(k, v).is_some() {
            return Err(SDSSError::InternalDecodeStructureIllegalData);
        }
    }
    if dict.len() == dict_len {
        Ok(dict)
    } else {
        Err(SDSSError::InternalDecodeStructureIllegalData)
    }
}

/*
    impls
*/

pub struct DGEntryMD {
    klen: usize,
    dscr: u8,
}

impl DGEntryMD {
    fn decode(data: [u8; 9]) -> Self {
        Self {
            klen: u64::from_le_bytes(memcpy(&data[..8])) as usize,
            dscr: data[8],
        }
    }
    fn encode(klen: usize, dscr: u8) -> [u8; 9] {
        let mut ret = [0u8; 9];
        ret[..8].copy_from_slice(&klen.u64_bytes_le());
        ret[8] = dscr;
        ret
    }
}

impl PersistDictEntryMetadata for DGEntryMD {
    fn verify_with_src(&self, scanner: &BufferedScanner) -> bool {
        static EXPECT_ATLEAST: [u8; 4] = [0, 1, 8, 8]; // PAD to align
        let lbound_rem = self.klen + EXPECT_ATLEAST[cmp::min(self.dscr, 3) as usize] as usize;
        scanner.has_left(lbound_rem) & (self.dscr <= PersistDictEntryDscr::Dict.value_u8())
    }
}

impl PersistDict for DictGeneric {
    type Key = Box<str>;
    type Value = DictEntryGeneric;
    type Metadata = DGEntryMD;
    const ENC_COUPLED: bool = true;
    const DEC_ENTRYMD_INFALLIBLE: bool = true;
    const DEC_COUPLED: bool = false;
    const DEC_VERIFY_MD_WITH_SRC_STANDALONE: bool = true;
    fn metadec_pretest_routine(_: &BufferedScanner) -> bool {
        true
    }
    fn metadec_pretest_entry(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64) + 1)
    }
    fn enc_entry_metadata(buf: &mut VecU8, key: &Self::Key, _: &Self::Value) {
        buf.extend(key.len().u64_bytes_le());
    }
    unsafe fn dec_entry_metadata(scanner: &mut BufferedScanner) -> Option<Self::Metadata> {
        Some(Self::Metadata::decode(scanner.next_chunk()))
    }
    fn enc_entry_coupled(buf: &mut VecU8, key: &Self::Key, val: &Self::Value) {
        match val {
            DictEntryGeneric::Map(map) => {
                buf.push(PersistDictEntryDscr::Dict.value_u8());
                buf.extend(key.as_bytes());
                _encode_dict::<Self>(buf, map);
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
    unsafe fn dec_entry_coupled(
        _: &mut BufferedScanner,
        _: Self::Metadata,
    ) -> Option<(Self::Key, Self::Value)> {
        unimplemented!()
    }
    fn enc_key(_: &mut VecU8, _: &Self::Key) {
        unimplemented!()
    }
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Key> {
        String::from_utf8(scanner.next_chunk_variable(md.klen).to_owned())
            .map(|s| s.into_boxed_str())
            .ok()
    }
    fn enc_val(_: &mut VecU8, _: &Self::Value) {
        unimplemented!()
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
                        DictEntryGeneric::Map(decode_dict::<DictGeneric>(scanner).ok()?)
                    } else {
                        unreachable!("found top-level dict item in datacell")
                    }
                }
            };
            Some(r)
        }
        decode_element(scanner, PersistDictEntryDscr::from_raw(md.dscr), true)
    }
}

#[test]
fn t_dict() {
    let dict: DictGeneric = into_dict! {
        "hello" => Datacell::new_str("world".into()),
        "omg a null?" => Datacell::null(),
        "a big fat dict" => DictEntryGeneric::Map(into_dict!(
            "with a value" => Datacell::new_uint(1002),
            "and a null" => Datacell::null(),
        ))
    };
    let encoded = encode_dict::<DictGeneric>(&dict);
    let mut scanner = BufferedScanner::new(&encoded);
    let decoded = decode_dict::<DictGeneric>(&mut scanner).unwrap();
    assert_eq!(dict, decoded);
}
