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

#[cfg(test)]
use core::mem;
use {
    crate::engine::{
        self,
        data::{
            lit::{Lit, LitIR},
            spec::{Dataspec1D, DataspecMeta1D},
            tag::{DataTag, TagClass},
        },
        mem::{NativeQword, SystemDword, WordRW},
    },
    core::{fmt, mem::ManuallyDrop, slice, str},
    parking_lot::RwLock,
};

pub struct Datacell {
    tag: TagClass,
    data: DataRaw,
}

impl Datacell {
    // bool
    pub fn new_bool(b: bool) -> Self {
        unsafe { Self::new(TagClass::Bool, DataRaw::word(SystemDword::store(b))) }
    }
    pub unsafe fn read_bool(&self) -> bool {
        self.load_word()
    }
    pub fn try_bool(&self) -> Option<bool> {
        self.checked_tag(TagClass::Bool, || unsafe { self.read_bool() })
    }
    pub fn bool(&self) -> bool {
        self.try_bool().unwrap()
    }
    // uint
    pub fn new_uint(u: u64) -> Self {
        unsafe { Self::new(TagClass::UnsignedInt, DataRaw::word(SystemDword::store(u))) }
    }
    pub unsafe fn read_uint(&self) -> u64 {
        self.load_word()
    }
    pub fn try_uint(&self) -> Option<u64> {
        self.checked_tag(TagClass::UnsignedInt, || unsafe { self.read_uint() })
    }
    pub fn uint(&self) -> u64 {
        self.try_uint().unwrap()
    }
    // sint
    pub fn new_sint(u: i64) -> Self {
        unsafe { Self::new(TagClass::SignedInt, DataRaw::word(SystemDword::store(u))) }
    }
    pub unsafe fn read_sint(&self) -> i64 {
        self.load_word()
    }
    pub fn try_sint(&self) -> Option<i64> {
        self.checked_tag(TagClass::SignedInt, || unsafe { self.read_sint() })
    }
    pub fn sint(&self) -> i64 {
        self.try_sint().unwrap()
    }
    // float
    pub fn new_float(f: f64) -> Self {
        unsafe { Self::new(TagClass::Float, DataRaw::word(SystemDword::store(f))) }
    }
    pub unsafe fn read_float(&self) -> f64 {
        self.load_word()
    }
    pub fn try_float(&self) -> Option<f64> {
        self.checked_tag(TagClass::Float, || unsafe { self.read_float() })
    }
    pub fn float(&self) -> f64 {
        self.try_float().unwrap()
    }
    // bin
    pub fn new_bin(s: Box<[u8]>) -> Self {
        let mut md = ManuallyDrop::new(s);
        unsafe {
            Self::new(
                TagClass::Bin,
                DataRaw::word(SystemDword::store((md.as_mut_ptr(), md.len()))),
            )
        }
    }
    pub unsafe fn read_bin(&self) -> &[u8] {
        let (p, l) = self.load_word();
        slice::from_raw_parts::<u8>(p, l)
    }
    pub fn try_bin(&self) -> Option<&[u8]> {
        self.checked_tag(TagClass::Bin, || unsafe { self.read_bin() })
    }
    pub fn bin(&self) -> &[u8] {
        self.try_bin().unwrap()
    }
    // str
    pub fn new_str(s: Box<str>) -> Self {
        let mut md = ManuallyDrop::new(s.into_boxed_bytes());
        unsafe {
            Self::new(
                TagClass::Str,
                DataRaw::word(SystemDword::store((md.as_mut_ptr(), md.len()))),
            )
        }
    }
    pub unsafe fn read_str(&self) -> &str {
        let (p, l) = self.load_word();
        str::from_utf8_unchecked(slice::from_raw_parts(p, l))
    }
    pub fn try_str(&self) -> Option<&str> {
        self.checked_tag(TagClass::Str, || unsafe { self.read_str() })
    }
    pub fn str(&self) -> &str {
        self.try_str().unwrap()
    }
    // list
    pub fn new_list(l: Vec<Self>) -> Self {
        unsafe { Self::new(TagClass::List, DataRaw::rwl(RwLock::new(l))) }
    }
    pub unsafe fn read_list(&self) -> &RwLock<Vec<Self>> {
        &self.data.rwl
    }
    pub fn try_list(&self) -> Option<&RwLock<Vec<Self>>> {
        self.checked_tag(TagClass::List, || unsafe { self.read_list() })
    }
    pub fn list(&self) -> &RwLock<Vec<Self>> {
        self.try_list().unwrap()
    }
}

direct_from! {
    Datacell => {
        bool as new_bool,
        u64 as new_uint,
        i64 as new_sint,
        f64 as new_float,
        f32 as new_float,
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

impl<'a> From<LitIR<'a>> for Datacell {
    fn from(l: LitIR<'a>) -> Self {
        match l.kind().tag_class() {
            tag if tag < TagClass::Bin => unsafe {
                let [a, b] = l.data().load_fat();
                Datacell::new(
                    l.kind().tag_class(),
                    DataRaw::word(SystemDword::store_fat(a, b)),
                )
            },
            tag @ (TagClass::Bin | TagClass::Str) => unsafe {
                let mut bin = ManuallyDrop::new(l.read_bin_uck().to_owned().into_boxed_slice());
                Datacell::new(
                    tag,
                    DataRaw::word(SystemDword::store((bin.as_mut_ptr(), bin.len()))),
                )
            },
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
impl From<i32> for Datacell {
    fn from(i: i32) -> Self {
        if i.is_negative() {
            Self::new_sint(i as _)
        } else {
            Self::new_uint(i as _)
        }
    }
}

impl<'a> From<Lit<'a>> for Datacell {
    fn from(l: Lit<'a>) -> Self {
        Self::from(l.as_ir())
    }
}

impl<const N: usize> From<[Datacell; N]> for Datacell {
    fn from(l: [Datacell; N]) -> Self {
        Self::new_list(l.into())
    }
}

impl Datacell {
    unsafe fn new(tag: TagClass, data: DataRaw) -> Self {
        Self { tag, data }
    }
    fn checked_tag<T>(&self, tag: TagClass, f: impl FnOnce() -> T) -> Option<T> {
        (self.tag == tag).then_some(f())
    }
    pub fn kind(&self) -> TagClass {
        self.tag
    }
    unsafe fn load_word<'a, T: WordRW<NativeQword, Target<'a> = T>>(&'a self) -> T {
        self.data.word.ld()
    }
}

impl fmt::Debug for Datacell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("Datacell");
        f.field("tag", &self.tag);
        macro_rules! fmtdbg {
            ($($match:ident => $ret:expr),* $(,)?) => {
                match self.tag {
                    $(TagClass::$match => f.field("data", &$ret),)*
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
        match (self.tag, other.tag) {
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
        match self.tag {
            TagClass::Str | TagClass::Bin => unsafe {
                let (p, l) = self.load_word();
                engine::mem::dealloc_array::<u8>(p, l)
            },
            TagClass::List => unsafe { ManuallyDrop::drop(&mut self.data.rwl) },
            _ => {}
        }
    }
}

#[cfg(test)]
impl Clone for Datacell {
    fn clone(&self) -> Self {
        let data = match self.tag {
            TagClass::Str | TagClass::Bin => unsafe {
                let block = ManuallyDrop::new(self.read_bin().to_owned().into_boxed_slice());
                DataRaw {
                    word: ManuallyDrop::new(SystemDword::store((block.as_ptr(), block.len()))),
                }
            },
            TagClass::List => unsafe {
                let data = self.read_list().read().iter().cloned().collect();
                DataRaw {
                    rwl: ManuallyDrop::new(RwLock::new(data)),
                }
            },
            _ => unsafe {
                DataRaw {
                    word: ManuallyDrop::new(mem::transmute_copy(&self.data.word)),
                }
            },
        };
        unsafe { Self::new(self.tag, data) }
    }
}
