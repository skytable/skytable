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
use crate::util::test_utils;
use {
    crate::engine::{
        data::{
            cell::Datacell,
            lit::Lit,
            tag::{DataTag, TagUnique},
        },
        idx::meta::Comparable,
        mem::{self, DwordNN, DwordQN, SpecialPaddedWord, WordIO, ZERO_BLOCK},
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
    data: SpecialPaddedWord,
}

impl PrimaryIndexKey {
    pub fn tag(&self) -> TagUnique {
        self.tag
    }
}

impl PrimaryIndexKey {
    pub unsafe fn data(&self) -> SpecialPaddedWord {
        core::mem::transmute_copy(&self.data)
    }
    pub unsafe fn read_uint(&self) -> u64 {
        self.data.load()
    }
    pub fn uint(&self) -> Option<u64> {
        (self.tag == TagUnique::UnsignedInt).then_some(unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_uint()
        })
    }
    pub unsafe fn read_sint(&self) -> i64 {
        self.data.load()
    }
    pub fn sint(&self) -> Option<i64> {
        (self.tag == TagUnique::SignedInt).then_some(unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_sint()
        })
    }
    pub unsafe fn read_bin(&self) -> &[u8] {
        self.virtual_block()
    }
    pub fn bin(&self) -> Option<&[u8]> {
        (self.tag == TagUnique::Bin).then(|| unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_bin()
        })
    }
    pub unsafe fn read_str(&self) -> &str {
        str::from_utf8_unchecked(self.virtual_block())
    }
    pub fn str(&self) -> Option<&str> {
        (self.tag == TagUnique::Str).then(|| unsafe {
            // UNSAFE(@ohsayan): verified tag
            self.read_str()
        })
    }
}

impl PrimaryIndexKey {
    #[cfg(test)]
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
        let (a, b) = unsafe {
            // UNSAFE(@ohsayan): this doesn't do anything "bad" by itself. needs the construction to be broken for it to do something silly
            dc.as_raw()
        }
        .dwordqn_load_qw_nw();
        if cfg!(debug_assertions) && tag < TagUnique::Bin {
            assert_eq!(b, mem::ZERO_BLOCK.as_ptr() as usize);
        }
        let me = Self {
            tag,
            data: unsafe {
                // UNSAFE(@ohsayan): loaded above, writing here
                SpecialPaddedWord::new(a, b)
            },
        };
        if cfg!(debug_assertions) {
            let vblk = me.virtual_block();
            assert_eq!(vblk.as_ptr() as usize, b);
        }
        me
    }
    /// Create a new quadword based primary key
    pub unsafe fn new_from_qw(tag: TagUnique, qw: u64) -> Self {
        debug_assert!(tag == TagUnique::SignedInt || tag == TagUnique::UnsignedInt);
        Self {
            tag,
            data: unsafe {
                // UNSAFE(@ohsayan): manually choosing block
                SpecialPaddedWord::new(qw, ZERO_BLOCK.as_ptr() as usize)
            },
        }
    }
    pub unsafe fn new_from_dual(tag: TagUnique, qw: u64, ptr: usize) -> Self {
        debug_assert!(tag == TagUnique::Str || tag == TagUnique::Bin);
        Self {
            tag,
            data: unsafe {
                // UNSAFE(@ohsayan): manually choosing qw and nw
                SpecialPaddedWord::new(qw, ptr)
            },
        }
    }
    pub unsafe fn raw_clone(&self) -> Self {
        Self::new(self.tag, {
            let (qw, nw) = self.data.dwordqn_load_qw_nw();
            SpecialPaddedWord::new(qw, nw)
        })
    }
    pub fn check(dc: &Datacell) -> bool {
        dc.tag().tag_unique().is_unique()
    }
    /// ## Safety
    /// If you mess up construction, everything will fall apart
    pub unsafe fn new(tag: TagUnique, data: SpecialPaddedWord) -> Self {
        Self { tag, data }
    }
    fn __compute_vdata_offset(&self) -> [usize; 2] {
        let (len, data) = self.data.dwordqn_load_qw_nw();
        if cfg!(debug_assertions) && self.tag < TagUnique::Bin {
            assert_eq!(data, mem::ZERO_BLOCK.as_ptr() as usize);
        }
        let actual_len = (len as usize) * (self.tag >= TagUnique::Bin) as usize;
        [data, actual_len]
    }
    fn virtual_block(&self) -> &[u8] {
        let [data, actual_len] = self.__compute_vdata_offset();
        unsafe {
            // UNSAFE(@ohsayan): Safe, due to construction
            slice::from_raw_parts(data as *const u8, actual_len)
        }
    }
    fn virtual_block_mut(&mut self) -> &mut [u8] {
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
                let vdata = self.virtual_block_mut();
                mem::unsafe_apis::dealloc_array(vdata.as_mut_ptr(), vdata.len());
            }
        }
    }
}

impl PartialEq for PrimaryIndexKey {
    fn eq(&self, other: &Self) -> bool {
        let [data_1, data_2]: [u64; 2] = [self.data.load(), other.data.load()];
        ((self.tag == other.tag) & (data_1 == data_2))
            && self.virtual_block() == other.virtual_block()
    }
}

impl Eq for PrimaryIndexKey {}

impl Hash for PrimaryIndexKey {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.tag.hash(hasher);
        self.virtual_block().hash(hasher);
    }
}

impl<'a> PartialEq<Lit<'a>> for PrimaryIndexKey {
    fn eq(&self, key: &Lit<'a>) -> bool {
        debug_assert!(key.kind().tag_unique().is_unique());
        let pk_data = self.data.dwordnn_load_qw();
        let lit_data = unsafe { key.data() }.dwordnn_load_qw();
        ((self.tag == key.kind().tag_unique()) & (pk_data == lit_data))
            && self.virtual_block() == key.__vdata()
    }
}

impl<'a> Comparable<Lit<'a>> for PrimaryIndexKey {
    fn cmp_eq(&self, key: &Lit<'a>) -> bool {
        <PrimaryIndexKey as PartialEq<Lit>>::eq(self, key)
    }
}

impl<'a> Comparable<PrimaryIndexKey> for Lit<'a> {
    fn cmp_eq(&self, key: &PrimaryIndexKey) -> bool {
        <PrimaryIndexKey as PartialEq<Lit>>::eq(key, self)
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

#[sky_macros::test]
fn gh_issue_test_325_same_type_collapse() {
    assert_ne!(
        PrimaryIndexKey::try_from_dc(Datacell::new_uint_default(1)).unwrap(),
        PrimaryIndexKey::try_from_dc(Datacell::new_uint_default(11)).unwrap()
    );
    assert_ne!(
        PrimaryIndexKey::try_from_dc(Datacell::new_uint_default(1)).unwrap(),
        Lit::new_uint(11)
    );
    assert_ne!(
        PrimaryIndexKey::try_from_dc(Datacell::new_uint_default(11)).unwrap(),
        Lit::new_uint(1)
    );
}

#[sky_macros::test]
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

#[sky_macros::test]
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

#[sky_macros::test]
fn check_pk_lit_eq_hash() {
    let state = test_utils::randomstate();
    let data = [
        Lit::new_uint(100),
        Lit::new_sint(-100),
        Lit::new_bin(b"binary bro"),
        Lit::new_str("string bro"),
    ];
    for lit in data {
        let pk = PrimaryIndexKey::try_from_dc(Datacell::from(lit.clone())).unwrap();
        assert_eq!(pk, lit);
        assert_eq!(
            test_utils::hash_rs(&state, &lit),
            test_utils::hash_rs(&state, &pk)
        );
    }
}

#[sky_macros::test]
fn check_pk_extremes() {
    let state = test_utils::randomstate();
    let d1 = PrimaryIndexKey::try_from_dc(Datacell::new_uint_default(u64::MAX)).unwrap();
    let d2 = PrimaryIndexKey::try_from_dc(Datacell::from(Lit::new_uint(u64::MAX))).unwrap();
    assert_eq!(d1, d2);
    assert_eq!(d1.uint().unwrap(), u64::MAX);
    assert_eq!(d2.uint().unwrap(), u64::MAX);
    assert_eq!(
        test_utils::hash_rs(&state, &d1),
        test_utils::hash_rs(&state, &d2)
    );
    assert_eq!(d1, Lit::new_uint(u64::MAX));
    assert_eq!(d2, Lit::new_uint(u64::MAX));
    assert_eq!(d1.uint().unwrap(), u64::MAX);
}

#[sky_macros::test]
fn empty_slice() {
    // bin
    let pk1 = PrimaryIndexKey::try_from_dc(Datacell::from(Lit::new_bin(b""))).unwrap();
    let pk1_ = PrimaryIndexKey::try_from_dc(Datacell::from(Lit::new_bin(b""))).unwrap();
    assert_eq!(pk1, Lit::new_bin(b""));
    assert_eq!(pk1, pk1_);
    drop((pk1, pk1_));
    // str
    let pk2 = PrimaryIndexKey::try_from_dc(Datacell::from(Lit::new_str(""))).unwrap();
    let pk2_ = PrimaryIndexKey::try_from_dc(Datacell::from(Lit::new_str(""))).unwrap();
    assert_eq!(pk2, Lit::new_str(""));
    assert_eq!(pk2, pk2_);
    drop((pk2, pk2_));
}

#[sky_macros::test]
fn ensure_ptr_offsets() {
    let data = String::from("hello").into_boxed_str();
    let __orig = (data.as_ptr(), data.len());
    // dc
    let dc = Datacell::new_str(data);
    assert_eq!(__orig, (dc.str().as_ptr(), dc.str().len()));
    // pk
    let pk = PrimaryIndexKey::try_from_dc(dc).unwrap();
    assert_eq!(
        __orig,
        (pk.str().unwrap().as_ptr(), pk.str().unwrap().len())
    );
}

#[sky_macros::test]
fn queue_ensure_offsets() {
    use crate::engine::sync::queue::Queue;
    let data: Vec<_> = (0..100)
        .map(|_| "hello".to_owned().into_boxed_str())
        .collect();
    let __orig: Vec<_> = data.iter().map(|s| (s.as_ptr(), s.len())).collect();
    let q = Queue::new();
    for datum in data {
        q.blocking_enqueue(
            PrimaryIndexKey::try_from_dc(Datacell::new_str(datum)).unwrap(),
            &crossbeam_epoch::pin(),
        );
    }
    let mut __reloaded = vec![];
    while let Some(key) = q.blocking_try_dequeue(&crossbeam_epoch::pin()) {
        let pk_str = key.str().unwrap();
        __reloaded.push((pk_str.as_ptr(), pk_str.len()));
    }
    assert_eq!(__orig, __reloaded);
}
