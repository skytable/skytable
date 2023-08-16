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

#[cfg(test)]
mod tests;

use {
    crate::{
        engine::{
            data::{
                cell::Datacell,
                dict::DictEntryGeneric,
                tag::{CUTag, DataTag, TagClass, TagUnique},
            },
            idx::{AsKey, AsValue},
            storage::v1::{rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::{copy_slice_to_array as memcpy, EndianQW},
    },
    std::{cmp, collections::HashMap, marker::PhantomData, mem},
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

/// Specification for any object that can be persisted
pub trait PersistObjectHlIO {
    /// the actual type (we can have wrappers)
    type Type;
    /// enc routine
    fn pe_obj_hlio_enc(buf: &mut VecU8, v: &Self::Type);
    /// verify the src to see if we can atleast start the routine
    fn pe_obj_hlio_dec_ver(scanner: &BufferedScanner) -> bool;
    /// dec routine
    unsafe fn pe_obj_hlio_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Type>;
}

/// enc the given object into a new buffer
pub fn enc<Obj: PersistObjectHlIO>(obj: &Obj::Type) -> VecU8 {
    let mut buf = vec![];
    Obj::pe_obj_hlio_enc(&mut buf, obj);
    buf
}

/// enc the object into the given buffer
pub fn enc_into_buf<Obj: PersistObjectHlIO>(buf: &mut VecU8, obj: &Obj::Type) {
    Obj::pe_obj_hlio_enc(buf, obj)
}

/// enc the object into the given buffer
pub fn enc_self_into_buf<Obj: PersistObjectHlIO<Type = Obj>>(buf: &mut VecU8, obj: &Obj) {
    Obj::pe_obj_hlio_enc(buf, obj)
}

/// enc the object into a new buffer
pub fn enc_self<Obj: PersistObjectHlIO<Type = Obj>>(obj: &Obj) -> VecU8 {
    enc::<Obj>(obj)
}

/// dec the object
pub fn dec<Obj: PersistObjectHlIO>(scanner: &mut BufferedScanner) -> SDSSResult<Obj::Type> {
    if Obj::pe_obj_hlio_dec_ver(scanner) {
        unsafe { Obj::pe_obj_hlio_dec(scanner) }
    } else {
        Err(SDSSError::InternalDecodeStructureCorrupted)
    }
}

/// dec the object
pub fn dec_self<Obj: PersistObjectHlIO<Type = Obj>>(
    scanner: &mut BufferedScanner,
) -> SDSSResult<Obj> {
    dec::<Obj>(scanner)
}

/// metadata spec for a persist map entry
pub trait PersistMapEntryMD {
    fn verify_with_src(&self, scanner: &BufferedScanner) -> bool;
}

/// specification for a persist map
pub trait PersistMapSpec {
    /// metadata type
    type Metadata: PersistMapEntryMD;
    /// key type (NOTE: set this to the true key type; handle any differences using the spec unless you have an entirely different
    /// wrapper type)
    type Key: AsKey;
    /// value type (NOTE: see [`PersistMapSpec::Key`])
    type Value: AsValue;
    /// coupled enc
    const ENC_COUPLED: bool;
    /// coupled dec
    const DEC_COUPLED: bool;
    /// once pretests pass, the metadata dec is infallible
    const META_INFALLIBLE_MD_PARSE: bool;
    /// verify the src using the given metadata
    const META_VERIFY_BEFORE_DEC: bool;
    // collection meta
    /// pretest before jmp to routine for entire collection
    fn meta_dec_collection_pretest(scanner: &BufferedScanner) -> bool;
    /// pretest before jmp to entry dec routine
    fn meta_dec_entry_pretest(scanner: &BufferedScanner) -> bool;
    // entry meta
    /// enc the entry meta
    fn entry_md_enc(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// dec the entry meta
    /// SAFETY: ensure that all pretests have passed (we expect the caller to not be stupid)
    unsafe fn entry_md_dec(scanner: &mut BufferedScanner) -> Option<Self::Metadata>;
    // independent packing
    /// enc key (non-packed)
    fn enc_key(buf: &mut VecU8, key: &Self::Key);
    /// enc val (non-packed)
    fn enc_val(buf: &mut VecU8, key: &Self::Value);
    /// dec key (non-packed)
    unsafe fn dec_key(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Key>;
    /// dec val (non-packed)
    unsafe fn dec_val(scanner: &mut BufferedScanner, md: &Self::Metadata) -> Option<Self::Value>;
    // coupled packing
    /// entry packed enc
    fn enc_entry(buf: &mut VecU8, key: &Self::Key, val: &Self::Value);
    /// entry packed dec
    unsafe fn dec_entry(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> Option<(Self::Key, Self::Value)>;
}

/*
    blanket
*/

/// This is more of a lazy hack than anything sensible. Just implement a spec and then use this wrapper for any enc/dec operations
pub struct PersistMapImpl<M: PersistMapSpec>(PhantomData<M>);

impl<M: PersistMapSpec> PersistObjectHlIO for PersistMapImpl<M> {
    type Type = HashMap<M::Key, M::Value>;
    fn pe_obj_hlio_enc(buf: &mut VecU8, v: &Self::Type) {
        enc_dict_into_buffer::<M>(buf, v)
    }
    fn pe_obj_hlio_dec_ver(_: &BufferedScanner) -> bool {
        true // handled by the dec impl
    }
    unsafe fn pe_obj_hlio_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Type> {
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
            match PM::entry_md_dec(scanner) {
                Some(v) => v,
                None => {
                    if PM::META_INFALLIBLE_MD_PARSE {
                        impossible!()
                    } else {
                        return Err(SDSSError::InternalDecodeStructureCorrupted);
                    }
                }
            }
        };
        if PM::META_VERIFY_BEFORE_DEC && !md.verify_with_src(scanner) {
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

/*
    impls
*/

/// generic dict spec (simple spec for [DictGeneric](crate::engine::data::dict::DictGeneric))
pub struct GenericDictSpec;
/// generic dict entry metadata
pub struct GenericDictEntryMD {
    dscr: u8,
    klen: usize,
}

impl GenericDictEntryMD {
    /// decode md (no need for any validation since that has to be handled later and can only produce incorrect results
    /// if unsafe code is used to translate an incorrect dscr)
    fn decode(data: [u8; 9]) -> Self {
        Self {
            klen: u64::from_le_bytes(memcpy(&data[..8])) as usize,
            dscr: data[8],
        }
    }
    /// encode md
    fn encode(klen: usize, dscr: u8) -> [u8; 9] {
        let mut ret = [0u8; 9];
        ret[..8].copy_from_slice(&klen.u64_bytes_le());
        ret[8] = dscr;
        ret
    }
}

impl PersistMapEntryMD for GenericDictEntryMD {
    fn verify_with_src(&self, scanner: &BufferedScanner) -> bool {
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
    const META_INFALLIBLE_MD_PARSE: bool = true;
    const META_VERIFY_BEFORE_DEC: bool = true;
    fn meta_dec_collection_pretest(_: &BufferedScanner) -> bool {
        true
    }
    fn meta_dec_entry_pretest(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64) + 1)
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

/*
    persist obj impls
*/

use crate::engine::{
    core::model::{Field, Layer},
    data::tag::{FullTag, TagSelector},
    mem::VInline,
};

struct POByteBlockFullTag(FullTag);

impl PersistObjectHlIO for POByteBlockFullTag {
    type Type = FullTag;
    fn pe_obj_hlio_enc(buf: &mut VecU8, slf: &Self::Type) {
        buf.extend(slf.tag_selector().d().u64_bytes_le())
    }
    fn pe_obj_hlio_dec_ver(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64))
    }
    unsafe fn pe_obj_hlio_dec(scanner: &mut BufferedScanner) -> SDSSResult<FullTag> {
        let dscr = scanner.next_u64_le();
        if dscr > TagSelector::max_dscr() as u64 {
            return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
        }
        Ok(TagSelector::from_raw(dscr as u8).into_full())
    }
}

impl PersistObjectHlIO for Layer {
    type Type = Layer;
    fn pe_obj_hlio_enc(buf: &mut VecU8, slf: &Self::Type) {
        // [8B: type sig][8B: empty property set]
        POByteBlockFullTag::pe_obj_hlio_enc(buf, &slf.tag());
        buf.extend(0u64.to_le_bytes());
    }
    fn pe_obj_hlio_dec_ver(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64) * 2)
    }
    unsafe fn pe_obj_hlio_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Type> {
        let type_sel = scanner.next_u64_le();
        let prop_set_arity = scanner.next_u64_le();
        if (type_sel > TagSelector::List.d() as u64) | (prop_set_arity != 0) {
            return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
        }
        Ok(Layer::new_empty_props(
            TagSelector::from_raw(type_sel as u8).into_full(),
        ))
    }
}

impl PersistObjectHlIO for Field {
    type Type = Self;
    fn pe_obj_hlio_enc(buf: &mut VecU8, slf: &Self::Type) {
        // [null][prop_c][layer_c]
        buf.push(slf.is_nullable() as u8);
        buf.extend(0u64.to_le_bytes());
        buf.extend(slf.layers().len().u64_bytes_le());
        for layer in slf.layers() {
            Layer::pe_obj_hlio_enc(buf, layer);
        }
    }
    fn pe_obj_hlio_dec_ver(scanner: &BufferedScanner) -> bool {
        scanner.has_left((sizeof!(u64) * 2) + 1)
    }
    unsafe fn pe_obj_hlio_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Type> {
        let nullable = scanner.next_byte();
        let prop_c = scanner.next_u64_le();
        let layer_cnt = scanner.next_u64_le();
        let mut layers = VInline::new();
        let mut fin = false;
        while (!scanner.eof())
            & (layers.len() as u64 != layer_cnt)
            & (Layer::pe_obj_hlio_dec_ver(scanner))
            & !fin
        {
            let l = Layer::pe_obj_hlio_dec(scanner)?;
            fin = l.tag().tag_class() != TagClass::List;
            layers.push(l);
        }
        let field = Field::new(layers, nullable == 1);
        if (field.layers().len() as u64 == layer_cnt) & (nullable <= 1) & (prop_c == 0) & fin {
            Ok(field)
        } else {
            Err(SDSSError::InternalDecodeStructureCorrupted)
        }
    }
}
