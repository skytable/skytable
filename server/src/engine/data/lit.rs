/*
 * Created on Sun Feb 26 2023
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
        spec::{Dataspec1D, DataspecMeta1D, DataspecMethods1D, DataspecRaw1D},
        tag::{DataTag, FullTag},
    },
    crate::engine::mem::{SpecialPaddedWord, SystemDword},
    core::{
        fmt,
        marker::PhantomData,
        mem::{self, ManuallyDrop},
    },
};

/*
    Lit
*/

pub struct Lit<'a> {
    data: SpecialPaddedWord,
    tag: FullTag,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Lit<'a> {
    pub fn as_ir(&'a self) -> LitIR<'a> {
        unsafe {
            // UNSAFE(@ohsayan): 'tis the lifetime. 'tis the savior
            mem::transmute_copy(self)
        }
    }
}

impl<'a> DataspecMeta1D for Lit<'a> {
    type Tag = FullTag;
    type Target = SpecialPaddedWord;
    type StringItem = Box<str>;
    fn new(flag: Self::Tag, data: Self::Target) -> Self {
        Self {
            data,
            tag: flag,
            _lt: PhantomData,
        }
    }
    fn kind(&self) -> Self::Tag {
        self.tag
    }
    fn data(&self) -> Self::Target {
        unsafe {
            // UNSAFE(@ohsayan): This function doesn't create any clones, so we're good
            mem::transmute_copy(self)
        }
    }
}

/*
    UNSAFE(@ohsayan): Safety checks:
    - Heap str: yes
    - Heap bin: no
    - Drop str: yes, dealloc
    - Drop bin: not needed
    - Clone str: yes, alloc
    - Clone bin: not needed
*/
unsafe impl<'a> DataspecRaw1D for Lit<'a> {
    const HEAP_STR: bool = true;
    const HEAP_BIN: bool = false;
    unsafe fn drop_str(&mut self) {
        let [ptr, len] = self.data().load_fat();
        drop(String::from_raw_parts(ptr as *mut u8, len, len));
    }
    unsafe fn drop_bin(&mut self) {}
    unsafe fn clone_str(s: &str) -> Self::Target {
        let new_string = ManuallyDrop::new(s.to_owned().into_boxed_str());
        SystemDword::store((new_string.as_ptr(), new_string.len()))
    }
    unsafe fn clone_bin(b: &[u8]) -> Self::Target {
        SystemDword::store((b.as_ptr(), b.len()))
    }
}

/*
    UNSAFE(@ohsayan): Safety checks:
    - We LEAK memory because, duh
    - We don't touch our own targets, ever (well, I'm a bad boy so I do touch it in fmt::Debug)
*/
unsafe impl<'a> Dataspec1D for Lit<'a> {
    fn Str(s: Box<str>) -> Self {
        let md = ManuallyDrop::new(s);
        Self::new(FullTag::STR, SystemDword::store((md.as_ptr(), md.len())))
    }
}

/*
    UNSAFE(@ohsayan):
    - No target touch
*/
unsafe impl<'a> DataspecMethods1D for Lit<'a> {}

impl<'a, T: DataspecMethods1D> PartialEq<T> for Lit<'a> {
    fn eq(&self, other: &T) -> bool {
        <Self as DataspecMethods1D>::self_eq(self, other)
    }
}
impl<'a> fmt::Debug for Lit<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("Lit");
        f.field("tag", &self.tag);
        self.self_fmt_debug_data("data", &mut f);
        f.field("_lt", &self._lt);
        f.finish()
    }
}

impl<'a> Drop for Lit<'a> {
    fn drop(&mut self) {
        self.self_drop();
    }
}

impl<'a> Clone for Lit<'a> {
    fn clone(&self) -> Self {
        self.self_clone()
    }
}

impl<'a> ToString for Lit<'a> {
    fn to_string(&self) -> String {
        <Self as DataspecMethods1D>::to_string_debug(self)
    }
}

direct_from! {
    Lit<'a> => {
        bool as Bool,
        u64 as UnsignedInt,
        i64 as SignedInt,
        f64 as Float,
        &'a str as Str,
        String as Str,
        Box<str> as Str,
        &'a [u8] as Bin,
    }
}

/*
    LitIR
*/

pub struct LitIR<'a> {
    tag: FullTag,
    data: SpecialPaddedWord,
    _lt: PhantomData<&'a str>,
}

impl<'a> DataspecMeta1D for LitIR<'a> {
    type Target = SpecialPaddedWord;
    type StringItem = &'a str;
    type Tag = FullTag;
    fn new(flag: Self::Tag, data: Self::Target) -> Self {
        Self {
            tag: flag,
            data,
            _lt: PhantomData,
        }
    }
    fn kind(&self) -> Self::Tag {
        self.tag
    }
    fn data(&self) -> Self::Target {
        unsafe {
            // UNSAFE(@ohsayan): We can freely copy our stack because everything is already allocated
            mem::transmute_copy(self)
        }
    }
}

/*
    UNSAFE(@ohsayan): Safety:
    - Heap str: no
    - Heap bin: no
    - Drop str: no
    - Drop bin: no
    - Clone str: stack
    - Clone bin: stack
*/
unsafe impl<'a> DataspecRaw1D for LitIR<'a> {
    const HEAP_STR: bool = false;
    const HEAP_BIN: bool = false;
    unsafe fn drop_str(&mut self) {}
    unsafe fn drop_bin(&mut self) {}
    unsafe fn clone_str(s: &str) -> Self::Target {
        SystemDword::store((s.as_ptr(), s.len()))
    }
    unsafe fn clone_bin(b: &[u8]) -> Self::Target {
        SystemDword::store((b.as_ptr(), b.len()))
    }
}

/*
    UNSAFE(@ohsayan): Safety:
    - No touches :)
*/
unsafe impl<'a> Dataspec1D for LitIR<'a> {
    fn Str(s: Self::StringItem) -> Self {
        Self::new(FullTag::STR, SystemDword::store((s.as_ptr(), s.len())))
    }
}

impl<'a> ToString for LitIR<'a> {
    fn to_string(&self) -> String {
        <Self as DataspecMethods1D>::to_string_debug(self)
    }
}

/*
    UNSAFE(@ohsayan): Safety:
    - No touches
*/
unsafe impl<'a> DataspecMethods1D for LitIR<'a> {}

impl<'a, T: DataspecMethods1D> PartialEq<T> for LitIR<'a> {
    fn eq(&self, other: &T) -> bool {
        <Self as DataspecMethods1D>::self_eq(self, other)
    }
}
impl<'a> fmt::Debug for LitIR<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("LitIR");
        f.field("tag", &self.tag);
        self.self_fmt_debug_data("data", &mut f);
        f.field("_lt", &self._lt);
        f.finish()
    }
}

impl<'a> Drop for LitIR<'a> {
    fn drop(&mut self) {
        self.self_drop();
    }
}

impl<'a> Clone for LitIR<'a> {
    fn clone(&self) -> Self {
        self.self_clone()
    }
}

direct_from! {
    LitIR<'a> => {
        bool as Bool,
        u64 as UnsignedInt,
        i64 as SignedInt,
        f64 as Float,
        &'a str as Str,
        &'a [u8] as Bin,
    }
}

#[test]
fn tlit() {
    let str1 = Lit::Str("hello".into());
    let str2 = str1.clone();
    assert_eq!(str1, str2);
    assert_eq!(str1.str(), "hello");
    assert_eq!(str2.str(), "hello");
    drop(str1);
    assert_eq!(str2.str(), "hello");
}

#[test]
fn tlitir() {
    let str1 = LitIR::Str("hello");
    let str2 = str1.clone();
    assert_eq!(str1, str2);
    assert_eq!(str1.str(), "hello");
    assert_eq!(str2.str(), "hello");
    drop(str1);
    assert_eq!(str2.str(), "hello");
}
