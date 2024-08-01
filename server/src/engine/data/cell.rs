/*
 * Created on Tue Feb 28 2023
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

use crate::engine::core::index::PrimaryIndexKey;

use super::tag::TagUnique;

use {
    crate::engine::{
        self,
        data::{
            lit::Lit,
            tag::{DataTag, FloatSpec, FullTag, SIntSpec, TagClass, UIntSpec},
        },
        mem::{DwordNN, DwordQN, NativeQword, SpecialPaddedWord, WordIO},
    },
    core::{
        fmt,
        marker::PhantomData,
        mem::{self, ManuallyDrop},
        ops::Deref,
        slice, str,
    },
    parking_lot::RwLock,
};

pub struct Datacell {
    init: bool,
    tag: FullTag,
    data: DataRaw,
}

impl Datacell {
    // bool
    pub fn new_bool(b: bool) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(
                FullTag::BOOL,
                DataRaw::word(SpecialPaddedWord::store(b).dwordqn_promote()),
            )
        }
    }
    pub unsafe fn read_bool(&self) -> bool {
        self.load_word()
    }
    pub fn try_bool(&self) -> Option<bool> {
        self.checked_tag(TagClass::Bool, || unsafe {
            // UNSAFE(@ohsayan): correct because we just verified the tag
            self.read_bool()
        })
    }
    pub fn bool(&self) -> bool {
        self.try_bool().unwrap()
    }
    // uint
    pub fn new_uint(u: u64, kind: UIntSpec) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(
                kind.into(),
                DataRaw::word(SpecialPaddedWord::store(u).dwordqn_promote()),
            )
        }
    }
    pub fn new_uint_default(u: u64) -> Self {
        Self::new_uint(u, UIntSpec::DEFAULT)
    }
    pub unsafe fn read_uint(&self) -> u64 {
        self.load_word()
    }
    pub fn try_uint(&self) -> Option<u64> {
        self.checked_tag(TagClass::UnsignedInt, || unsafe {
            // UNSAFE(@ohsayan): correct because we just verified the tag
            self.read_uint()
        })
    }
    pub fn uint(&self) -> u64 {
        self.try_uint().unwrap()
    }
    pub fn into_uint(self) -> Option<u64> {
        if self.kind() != TagClass::UnsignedInt {
            return None;
        }
        unsafe {
            // UNSAFE(@ohsayan): +tagck
            let md = ManuallyDrop::new(self);
            Some(md.data.word.dwordnn_load_qw())
        }
    }
    // sint
    pub fn new_sint(i: i64, kind: SIntSpec) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(
                kind.into(),
                DataRaw::word(SpecialPaddedWord::store(i).dwordqn_promote()),
            )
        }
    }
    pub fn new_sint_default(s: i64) -> Self {
        Self::new_sint(s, SIntSpec::DEFAULT)
    }
    pub unsafe fn read_sint(&self) -> i64 {
        self.load_word()
    }
    pub fn try_sint(&self) -> Option<i64> {
        self.checked_tag(TagClass::SignedInt, || unsafe {
            // UNSAFE(@ohsayan): Correct because we just verified the tag
            self.read_sint()
        })
    }
    pub fn sint(&self) -> i64 {
        self.try_sint().unwrap()
    }
    // float
    pub fn new_float(f: f64, spec: FloatSpec) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(
                spec.into(),
                DataRaw::word(SpecialPaddedWord::store(f).dwordqn_promote()),
            )
        }
    }
    pub fn new_float_default(f: f64) -> Self {
        Self::new_float(f, FloatSpec::DEFAULT)
    }
    pub unsafe fn read_float(&self) -> f64 {
        self.load_word()
    }
    pub fn try_float(&self) -> Option<f64> {
        self.checked_tag(TagClass::Float, || unsafe {
            // UNSAFE(@ohsayan): Correcrt because we just verified the tag
            self.read_float()
        })
    }
    pub fn float(&self) -> f64 {
        self.try_float().unwrap()
    }
    // bin
    pub fn new_bin(s: Box<[u8]>) -> Self {
        let mut md = ManuallyDrop::new(s);
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(
                FullTag::BIN,
                DataRaw::word(WordIO::store((md.len(), md.as_mut_ptr()))),
            )
        }
    }
    pub unsafe fn read_bin(&self) -> &[u8] {
        let (l, p) = self.load_word();
        slice::from_raw_parts::<u8>(p, l)
    }
    pub fn try_bin(&self) -> Option<&[u8]> {
        self.checked_tag(TagClass::Bin, || unsafe {
            // UNSAFE(@ohsayan): Correct because we just verified the tag
            self.read_bin()
        })
    }
    pub fn bin(&self) -> &[u8] {
        self.try_bin().unwrap()
    }
    pub fn into_bin(self) -> Option<Vec<u8>> {
        if self.kind() != TagClass::Bin {
            return None;
        }
        unsafe {
            // UNSAFE(@ohsayan): no double free + tagck
            let md = ManuallyDrop::new(self);
            let (a, b) = md.data.word.dwordqn_load_qw_nw();
            Some(Vec::from_raw_parts(
                b as *const u8 as *mut u8,
                a as usize,
                a as usize,
            ))
        }
    }
    // str
    pub fn new_str(s: Box<str>) -> Self {
        let mut md = ManuallyDrop::new(s.into_boxed_bytes());
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(
                FullTag::STR,
                DataRaw::word(WordIO::store((md.len(), md.as_mut_ptr()))),
            )
        }
    }
    pub unsafe fn read_str(&self) -> &str {
        let (l, p) = self.load_word();
        str::from_utf8_unchecked(slice::from_raw_parts(p, l))
    }
    pub fn try_str(&self) -> Option<&str> {
        self.checked_tag(TagClass::Str, || unsafe {
            // UNSAFE(@ohsayan): Correct because we just verified the tag
            self.read_str()
        })
    }
    pub fn str(&self) -> &str {
        self.try_str().unwrap()
    }
    pub fn into_str(self) -> Option<String> {
        if self.kind() != TagClass::Str {
            return None;
        }
        unsafe {
            // UNSAFE(@ohsayan): no double free + tagck
            let md = ManuallyDrop::new(self);
            let (a, b) = md.data.word.dwordqn_load_qw_nw();
            Some(String::from_raw_parts(
                b as *const u8 as *mut u8,
                a as usize,
                a as usize,
            ))
        }
    }
    // list
    pub fn new_list(l: Vec<Self>) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): Correct because we are initializing Self with the correct tag
            Self::new(FullTag::LIST, DataRaw::rwl(RwLock::new(l)))
        }
    }
    pub unsafe fn read_list(&self) -> &RwLock<Vec<Self>> {
        &self.data.rwl
    }
    pub fn try_list(&self) -> Option<&RwLock<Vec<Self>>> {
        self.checked_tag(TagClass::List, || unsafe {
            // UNSAFE(@ohsayan): Correct because we just verified the tag
            self.read_list()
        })
    }
    pub fn list(&self) -> &RwLock<Vec<Self>> {
        self.try_list().unwrap()
    }
    pub fn into_list(self) -> Option<Vec<Datacell>> {
        if self.kind() != TagClass::List {
            return None;
        }
        unsafe {
            // UNSAFE(@ohsayan): +tagck +avoid double free
            let md = ManuallyDrop::new(self);
            let rwl = core::ptr::read(&md.data.rwl);
            Some(ManuallyDrop::into_inner(rwl).into_inner())
        }
    }
    pub unsafe fn new_qw(qw: u64, tag: FullTag) -> Datacell {
        Self::new(
            tag,
            DataRaw::word(SpecialPaddedWord::store(qw).dwordqn_promote()),
        )
    }
    pub unsafe fn set_tag(&mut self, tag: FullTag) {
        self.tag = tag;
    }
}

direct_from! {
    Datacell => {
        bool as new_bool,
        u64 as new_uint_default,
        i64 as new_sint_default,
        f64 as new_float_default,
        Vec<u8> as new_bin,
        Box<[u8]> as new_bin,
        &'static [u8] as new_bin,
        String as new_str,
        Box<str> as new_str,
        &'static str as new_str,
        Vec<Self> as new_list,
        Box<[Self]> as new_list,
    }
}

impl<'a> From<Lit<'a>> for Datacell {
    fn from(l: Lit<'a>) -> Self {
        match l.kind().tag_class() {
            tag if tag < TagClass::Bin => unsafe {
                // UNSAFE(@ohsayan): Correct because we are using the same tag, and in this case the type doesn't need any advanced construction
                Datacell::new(
                    l.kind(),
                    // DO NOT RELY ON the payload's bit pattern; it's padded
                    DataRaw::word(l.data().dwordqn_promote()),
                )
            },
            TagClass::Bin | TagClass::Str => unsafe {
                // UNSAFE(@ohsayan): Correct because we are using the same tag, and in this case the type requires a new heap for construction
                let mut bin = ManuallyDrop::new(l.bin().to_owned().into_boxed_slice());
                Datacell::new(
                    l.kind(),
                    DataRaw::word(DwordQN::dwordqn_store_qw_nw(
                        bin.len() as u64,
                        bin.as_mut_ptr() as usize,
                    )),
                )
            },
            _ => unsafe {
                // UNSAFE(@ohsayan): a Lit will never be higher than a string
                impossible!()
            },
        }
    }
}

#[cfg(test)]
impl From<i32> for Datacell {
    fn from(i: i32) -> Self {
        if i.is_negative() {
            Self::new_sint_default(i as _)
        } else {
            Self::new_uint_default(i as _)
        }
    }
}

impl<const N: usize> From<[Datacell; N]> for Datacell {
    fn from(l: [Datacell; N]) -> Self {
        Self::new_list(l.into())
    }
}

impl Datacell {
    pub const fn tag(&self) -> FullTag {
        self.tag
    }
    pub fn kind(&self) -> TagClass {
        self.tag.tag_class()
    }
    pub fn null() -> Self {
        unsafe {
            // UNSAFE(@ohsayan): This is a hack. It's safe because we set init to false
            Self::_new(
                FullTag::BOOL,
                DataRaw::word(NativeQword::dwordnn_store_qw(0)),
                false,
            )
        }
    }
    pub fn is_null(&self) -> bool {
        !self.init
    }
    pub fn is_init(&self) -> bool {
        self.init
    }
    unsafe fn load_word<'a, T>(&'a self) -> T
    where
        NativeQword: WordIO<T>,
    {
        self.data.word.load()
    }
    unsafe fn _new(tag: FullTag, data: DataRaw, init: bool) -> Self {
        Self { init, tag, data }
    }
    unsafe fn new(tag: FullTag, data: DataRaw) -> Self {
        Self::_new(tag, data, true)
    }
    fn checked_tag<T>(&self, tag: TagClass, f: impl FnOnce() -> T) -> Option<T> {
        ((self.kind() == tag) & (self.is_init())).then(f)
    }
    pub unsafe fn as_raw(&self) -> NativeQword {
        mem::transmute_copy(&self.data.word)
    }
}

impl fmt::Debug for Datacell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("Datacell");
        f.field("tag", &self.tag);
        macro_rules! fmtdbg {
            ($($match:ident => $ret:expr),* $(,)?) => {
                match self.kind() {
                    $(TagClass::$match if self.is_init() => { f.field("data", &Some($ret));},)*
                    TagClass::Bool if self.is_null() => {f.field("data", &Option::<u8>::None);},
                    _ => unreachable!("incorrect state"),
                }
            }
        }
        fmtdbg!(
            Bool => self.bool(),
            UnsignedInt => self.uint(),
            SignedInt => self.sint(),
            Float => self.float(),
            Bin => self.bin(),
            Str => self.str(),
            List => self.list(),
        );
        f.finish()
    }
}

impl PartialEq for Datacell {
    fn eq(&self, other: &Datacell) -> bool {
        if self.is_null() {
            return other.is_null();
        }
        match (self.kind(), other.kind()) {
            (TagClass::Bool, TagClass::Bool) => self.bool() == other.bool(),
            (TagClass::UnsignedInt, TagClass::UnsignedInt) => self.uint() == other.uint(),
            (TagClass::SignedInt, TagClass::SignedInt) => self.sint() == other.sint(),
            (TagClass::Float, TagClass::Float) => self.float() == other.float(),
            (TagClass::Bin, TagClass::Bin) => self.bin() == other.bin(),
            (TagClass::Str, TagClass::Str) => self.str() == other.str(),
            (TagClass::List, TagClass::List) => {
                let l1_l = self.list().read();
                let l2_l = other.list().read();
                let l1: &[Self] = l1_l.as_ref();
                let l2: &[Self] = l2_l.as_ref();
                l1 == l2
            }
            _ => false,
        }
    }
}

impl Eq for Datacell {}

union! {
    union DataRaw {
        !word: NativeQword,
        !rwl: RwLock<Vec<Datacell>>,
    }
}

impl DataRaw {
    fn word(word: NativeQword) -> Self {
        Self {
            word: ManuallyDrop::new(word),
        }
    }
    fn rwl(rwl: RwLock<Vec<Datacell>>) -> Self {
        Self {
            rwl: ManuallyDrop::new(rwl),
        }
    }
}

impl Drop for Datacell {
    fn drop(&mut self) {
        match self.kind() {
            TagClass::Str | TagClass::Bin => unsafe {
                // UNSAFE(@ohsayan): we have checked that the cell is initialized (uninit will not satisfy this class), and we have checked its class
                let (l, p) = self.load_word();
                engine::mem::unsafe_apis::dealloc_array::<u8>(p, l)
            },
            TagClass::List => unsafe {
                // UNSAFE(@ohsayan): we have checked that the cell is initialized (uninit will not satisfy this class), and we have checked its class
                ManuallyDrop::drop(&mut self.data.rwl)
            },
            _ => {}
        }
    }
}

#[cfg(test)]
impl Clone for Datacell {
    fn clone(&self) -> Self {
        let data = match self.kind() {
            TagClass::Str | TagClass::Bin => unsafe {
                // UNSAFE(@ohsayan): we have checked that the cell is initialized (uninit will not satisfy this class), and we have checked its class
                let mut block = ManuallyDrop::new(self.read_bin().to_owned().into_boxed_slice());
                DataRaw::word(DwordQN::dwordqn_store_qw_nw(
                    block.len() as u64,
                    block.as_mut_ptr() as usize,
                ))
            },
            TagClass::List => unsafe {
                // UNSAFE(@ohsayan): we have checked that the cell is initialized (uninit will not satisfy this class), and we have checked its class
                let data = self.read_list().read().iter().cloned().collect();
                DataRaw::rwl(RwLock::new(data))
            },
            _ => unsafe {
                // UNSAFE(@ohsayan): we have checked that the cell is a stack class
                DataRaw::word(mem::transmute_copy(&self.data.word))
            },
        };
        unsafe {
            // UNSAFE(@ohsayan): same tag, we correctly init data and also return the same init state
            Self::_new(self.tag, data, self.init)
        }
    }
}

#[derive(Debug)]
pub struct VirtualDatacell<'a> {
    dc: ManuallyDrop<Datacell>,
    _lt: PhantomData<Lit<'a>>,
}

impl<'a> VirtualDatacell<'a> {
    pub fn new(lit: Lit<'a>, tag: TagUnique) -> Self {
        debug_assert_eq!(lit.kind().tag_unique(), tag);
        Self {
            dc: ManuallyDrop::new(unsafe {
                // UNSAFE(@ohsayan): this is a "reference" to a "virtual" aka fake DC. this just works because of memory layouts
                Datacell::new(lit.kind(), DataRaw::word(lit.data().dwordqn_promote()))
            }),
            _lt: PhantomData,
        }
    }
    pub fn new_pk(pk: &'a PrimaryIndexKey, tag: FullTag) -> Self {
        debug_assert_eq!(pk.tag(), tag.tag_unique());
        Self {
            dc: ManuallyDrop::new(unsafe {
                Datacell::new(tag, DataRaw::word(pk.data().dwordqn_promote()))
            }),
            _lt: PhantomData,
        }
    }
}

impl<'a> Deref for VirtualDatacell<'a> {
    type Target = Datacell;
    fn deref(&self) -> &Self::Target {
        &self.dc
    }
}

impl<'a> PartialEq<Datacell> for VirtualDatacell<'a> {
    fn eq(&self, other: &Datacell) -> bool {
        self.dc.deref() == other
    }
}

impl<'a> Clone for VirtualDatacell<'a> {
    fn clone(&self) -> Self {
        unsafe { core::mem::transmute_copy(self) }
    }
}

#[sky_macros::test]
fn vdc_check() {
    let dc = Lit::new_str("hello, world");
    assert_eq!(
        VirtualDatacell::new(dc, TagUnique::Str),
        Datacell::from("hello, world")
    );
}

#[sky_macros::test]
fn empty_slice() {
    let dc1 = Datacell::from(Lit::new_bin(b""));
    assert_eq!(dc1, Datacell::new_bin(b"".to_vec().into_boxed_slice()));
    drop(dc1);
    let dc2 = Datacell::from(Lit::new_str(""));
    assert_eq!(dc2, Datacell::new_str("".into()));
    drop(dc2);
}
