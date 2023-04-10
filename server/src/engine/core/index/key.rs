/*
 * Created on Sun Apr 09 2023
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

#[cfg(test)]
use crate::{engine::data::spec::Dataspec1D, util::test_utils};
use {
    crate::engine::{
        core::model::cell::Datacell,
        data::{
            lit::LitIR,
            spec::DataspecMeta1D,
            tag::{DataTag, TagUnique},
        },
        idx::meta::Comparable,
        mem::{self, NativeDword, SystemDword},
    },
    core::{
        fmt,
        hash::{Hash, Hasher},
        mem::ManuallyDrop,
        slice, str,
    },
};

pub struct PrimaryIndexKey {
    tag: TagUnique,
    data: NativeDword,
}

impl PrimaryIndexKey {
    pub unsafe fn read_uint(&self) -> u64 {
        self.data.load_qw()
    }
    pub fn uint(&self) -> Option<u64> {
        (self.tag == TagUnique::UnsignedInt).then_some(unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_uint()
        })
    }
    pub unsafe fn read_sint(&self) -> i64 {
        self.data.load_qw() as _
    }
    pub fn sint(&self) -> Option<i64> {
        (self.tag == TagUnique::SignedInt).then_some(unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_sint()
        })
    }
    pub unsafe fn read_bin(&self) -> &[u8] {
        self.vdata()
    }
    pub fn bin(&self) -> Option<&[u8]> {
        (self.tag == TagUnique::Bin).then(|| unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_bin()
        })
    }
    pub unsafe fn read_str(&self) -> &str {
        str::from_utf8_unchecked(self.vdata())
    }
    pub fn str(&self) -> Option<&str> {
        (self.tag == TagUnique::Str).then(|| unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_str()
        })
    }
}

impl PrimaryIndexKey {
    pub fn try_from_dc(dc: Datacell) -> Option<Self> {
        Self::check(&dc).then(|| unsafe { Self::new_from_dc(dc) })
    }
    /// ## Safety
    ///
    /// Make sure that the [`Datacell`] is an eligible candidate key (ensuring uniqueness constraints + allocation correctness).
    ///
    /// If you violate this:
    /// - You might leak memory
    /// - You might segfault
    /// - Even if you escape both, it will produce incorrect results which is something you DO NOT want in an index
    pub unsafe fn new_from_dc(dc: Datacell) -> Self {
        debug_assert!(Self::check(&dc));
        let tag = dc.tag().tag_unique();
        let dc = ManuallyDrop::new(dc);
        let dword = unsafe {
            // UNSAFE(@ohsayan): this doesn't do anything "bad" by itself. needs the construction to be broken for it to do something silly
            dc.as_raw()
        }
        .load_double();
        Self {
            tag,
            data: unsafe {
                // UNSAFE(@ohsayan): Perfectly safe since we're tranforming it and THIS will not by itself crash anything
                core::mem::transmute(dword)
            },
        }
    }
    pub fn check(dc: &Datacell) -> bool {
        dc.tag().tag_unique().is_unique()
    }
    pub fn check_opt(dc: &Option<Datacell>) -> bool {
        dc.as_ref().map(Self::check).unwrap_or(false)
    }
    /// ## Safety
    /// If you mess up construction, everything will fall apart
    pub unsafe fn new(tag: TagUnique, data: NativeDword) -> Self {
        Self { tag, data }
    }
    fn __compute_vdata_offset(&self) -> [usize; 2] {
        let [len, data] = self.data.load_double();
        let actual_len = len * (self.tag >= TagUnique::Bin) as usize;
        [data, actual_len]
    }
    fn vdata(&self) -> &[u8] {
        let [data, actual_len] = self.__compute_vdata_offset();
        unsafe {
            // UNSAFE(@ohsayan): Safe, due to construction
            slice::from_raw_parts(data as *const u8, actual_len)
        }
    }
    fn vdata_mut(&mut self) -> &mut [u8] {
        let [data, actual_len] = self.__compute_vdata_offset();
        unsafe {
            // UNSAFE(@ohsayan): safe due to construction
            slice::from_raw_parts_mut(data as *mut u8, actual_len)
        }
    }
}

impl Drop for PrimaryIndexKey {
    fn drop(&mut self) {
        if let TagUnique::Bin | TagUnique::Str = self.tag {
            unsafe {
                // UNSAFE(@ohsayan): Aliasing, sole owner and correct initialization
                let vdata = self.vdata_mut();
                mem::dealloc_array(vdata.as_mut_ptr(), vdata.len());
            }
        }
    }
}

impl PartialEq for PrimaryIndexKey {
    fn eq(&self, other: &Self) -> bool {
        let [data_1, data_2] = [self.data.load_double()[0], other.data.load_double()[0]];
        ((self.tag == other.tag) & (data_1 == data_2)) && self.vdata() == other.vdata()
    }
}

impl Eq for PrimaryIndexKey {}

impl Hash for PrimaryIndexKey {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.tag.hash(hasher);
        self.vdata().hash(hasher);
    }
}

impl<'a> PartialEq<LitIR<'a>> for PrimaryIndexKey {
    fn eq(&self, key: &LitIR<'a>) -> bool {
        debug_assert!(key.kind().tag_unique().is_unique());
        self.tag == key.kind().tag_unique() && self.vdata() == key.__vdata()
    }
}

impl<'a> Comparable<LitIR<'a>> for PrimaryIndexKey {
    fn cmp_eq(&self, key: &LitIR<'a>) -> bool {
        self == key
    }
}

impl fmt::Debug for PrimaryIndexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg_struct = f.debug_struct("PrimaryIndexKey");
        dbg_struct.field("tag", &self.tag);
        macro_rules! fmt {
            ($($mtch:ident => $expr:expr),* $(,)?) => {
                match self.tag {
                    $(TagUnique::$mtch => dbg_struct.field("data", &($expr.unwrap())),)*
                    TagUnique::Illegal => panic!("found illegal value. check ctor."),
                }
            };
        }
        fmt!(
            UnsignedInt => self.uint(),
            SignedInt => self.sint(),
            Bin => self.bin(),
            Str => self.str(),
        );
        dbg_struct.finish()
    }
}

#[test]
fn check_pk_wrong_type() {
    let data = [
        Datacell::from(false),
        Datacell::from(100),
        Datacell::from(-100),
        Datacell::from(10.11),
        Datacell::from("hello"),
        Datacell::from("hello".as_bytes()),
        Datacell::from([]),
    ];
    for datum in data {
        let tag = datum.tag();
        let candidate = PrimaryIndexKey::try_from_dc(datum);
        if tag.tag_unique() == TagUnique::Illegal {
            assert!(candidate.is_none(), "{:?}", &candidate);
        } else {
            assert!(candidate.is_some(), "{:?}", &candidate);
        }
    }
}

#[test]
fn check_pk_eq_hash() {
    let state = test_utils::randomstate();
    let data = [
        Datacell::from(100),
        Datacell::from(-100),
        Datacell::from("binary".as_bytes()),
        Datacell::from("string"),
    ];

    for datum in data {
        let pk1 = PrimaryIndexKey::try_from_dc(datum.clone()).unwrap();
        let pk2 = PrimaryIndexKey::try_from_dc(datum).unwrap();
        assert_eq!(pk1, pk2);
        assert_eq!(
            test_utils::hash_rs(&state, &pk1),
            test_utils::hash_rs(&state, &pk2)
        );
    }
}

#[test]
fn check_pk_lit_eq_hash() {
    let state = test_utils::randomstate();
    let data = [
        LitIR::UnsignedInt(100),
        LitIR::SignedInt(-100),
        LitIR::Bin(b"binary bro"),
        LitIR::Str("string bro"),
    ];
    for litir in data {
        let pk = PrimaryIndexKey::try_from_dc(Datacell::from(litir.clone())).unwrap();
        assert_eq!(pk, litir);
        assert_eq!(
            test_utils::hash_rs(&state, &litir),
            test_utils::hash_rs(&state, &pk)
        );
    }
}
