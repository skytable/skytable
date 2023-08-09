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
                dict::DictEntryGeneric,
                tag::{DataTag, TagClass},
            },
            idx::{AsKey, AsValue},
            storage::v1::{rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::EndianQW,
    },
    std::collections::HashMap,
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
        unsafe { core::mem::transmute(class.d() + 1) }
    }
    pub fn new_from_dict_gen_entry(e: &DictEntryGeneric) -> Self {
        match e {
            DictEntryGeneric::Null => Self::Null,
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
    unsafe fn dec_entry_metadata(scanner: &mut BufferedScanner) -> Option<Self::Metadata>;
    // entry (coupled)
    /// enc a packed entry
    fn enc_entry_coupled(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// dec a packed entry
    fn dec_entry_coupled(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> Option<(Self::Key, Self::Value)>;
    // entry (non-packed)
    /// enc key for a normal entry
    fn enc_key(buf: &mut VecU8, key: &Self::Key);
    /// dec normal entry key
    fn dec_key(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Key>;
    /// enc val for a normal entry
    fn enc_val(buf: &mut VecU8, val: &Self::Value);
    /// dec normal entry val
    fn dec_val(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Value>;
}

/*
    blanket
*/

pub fn encode_dict<Pd: PersistDict>(dict: &HashMap<Pd::Key, Pd::Value>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(dict.len().u64_bytes_le());
    for (key, val) in dict {
        Pd::enc_entry_metadata(&mut buf, key, val);
        if Pd::ENC_COUPLED {
            Pd::enc_entry_coupled(&mut buf, key, val);
        } else {
            Pd::enc_key(&mut buf, key);
            Pd::enc_val(&mut buf, val);
        }
    }
    buf
}

pub fn decode_dict<Pd: PersistDict>(
    scanner: &mut BufferedScanner,
) -> SDSSResult<HashMap<Pd::Key, Pd::Value>> {
    if Pd::metadec_pretest_routine(scanner) & scanner.has_left(sizeof!(u64)) {
        return Err(SDSSError::InternalDecodeStructureCorrupted);
    }
    let dict_len = unsafe {
        // UNSAFE(@ohsayan): pretest
        scanner.next_u64_le() as usize
    };
    let mut dict = HashMap::with_capacity(dict_len);
    while Pd::metadec_pretest_entry(scanner) {
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
            match Pd::dec_entry_coupled(scanner, md) {
                Some((_k, _v)) => {
                    k = _k;
                    v = _v;
                }
                None => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
            }
        } else {
            match (Pd::dec_key(scanner, &md), Pd::dec_val(scanner, &md)) {
                (Some(_k), Some(_v)) => {
                    k = _k;
                    v = _v;
                }
                _ => return Err(SDSSError::InternalDecodeStructureCorruptedPayload),
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
