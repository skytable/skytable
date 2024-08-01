/*
 * Created on Wed Sep 20 2023
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
    crate::engine::{
        data::tag::{DataTag, FullTag, TagClass, TagUnique},
        mem::{DwordQN, SpecialPaddedWord},
    },
    core::{
        fmt,
        hash::{Hash, Hasher},
        marker::PhantomData,
        mem::ManuallyDrop,
        slice, str,
    },
};

/*
    NOTE(@ohsayan): Heinous hackery that should not ever be repeated. Just don't touch anything here.
*/

/// A literal representation
pub struct Lit<'a> {
    tag: FullTag,
    dtc: u8,
    word: SpecialPaddedWord,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Lit<'a> {
    /// Create a new bool literal
    pub fn new_bool(b: bool) -> Self {
        Self::_quad(b as _, FullTag::BOOL)
    }
    /// Create a new unsigned integer
    pub fn new_uint(u: u64) -> Self {
        Self::_quad(u, FullTag::UINT)
    }
    /// Create a new signed integer
    pub fn new_sint(s: i64) -> Self {
        Self::_quad(s as _, FullTag::SINT)
    }
    /// Create a new float64
    pub fn new_float(f: f64) -> Self {
        Self::_quad(f.to_bits(), FullTag::FLOAT)
    }
    /// Returns a "shallow clone"
    ///
    /// This function will fall apart if lifetimes aren't handled correctly (aka will segfault)
    pub fn as_ir(&'a self) -> Lit<'a> {
        unsafe {
            // UNSAFE(@ohsayan): this is a dirty, uncanny and wild hack that everyone should be forbidden from doing
            let mut slf: Lit<'a> = core::mem::transmute_copy(self);
            slf.dtc = Self::DTC_NONE;
            slf
        }
    }
}

#[allow(unused)]
impl<'a> Lit<'a> {
    /// Attempt to read a bool
    pub fn try_bool(&self) -> Option<bool> {
        (self.tag.tag_class() == TagClass::Bool).then_some(unsafe {
            // UNSAFE(@ohsayan): +tagck
            self.bool()
        })
    }
    /// Attempt to read an unsigned integer
    pub fn try_uint(&self) -> Option<u64> {
        (self.tag.tag_class() == TagClass::UnsignedInt).then_some(unsafe {
            // UNSAFE(@ohsayan): +tagck
            self.uint()
        })
    }
    /// Attempt to read a signed integer
    pub fn try_sint(&self) -> Option<i64> {
        (self.tag.tag_class() == TagClass::SignedInt).then_some(unsafe {
            // UNSAFE(@ohsayan): +tagck
            self.sint()
        })
    }
    /// Attempt to read a float
    pub fn try_float(&self) -> Option<f64> {
        (self.tag.tag_class() == TagClass::Float).then_some(unsafe {
            // UNSAFE(@ohsayan): +tagck
            self.float()
        })
    }
    /// Read a bool directly. This function isn't exactly unsafe, but we want to provide a type preserving API
    pub unsafe fn bool(&self) -> bool {
        self.uint() == 1
    }
    /// Read an unsigned integer directly. This function isn't exactly unsafe, but we want to provide a type
    /// preserving API
    pub unsafe fn uint(&self) -> u64 {
        self.word.dwordqn_load_qw_nw().0
    }
    /// Read a signed integer directly. This function isn't exactly unsafe, but we want to provide a type
    /// preserving API
    pub unsafe fn sint(&self) -> i64 {
        self.uint() as _
    }
    /// Read a floating point number directly. This function isn't exactly unsafe, but we want to provide a type
    /// preserving API
    pub unsafe fn float(&self) -> f64 {
        f64::from_bits(self.uint())
    }
}

impl<'a> Lit<'a> {
    #[allow(unused)]
    /// Attempt to read a binary value
    pub fn try_bin(&self) -> Option<&'a [u8]> {
        (self.tag.tag_class() == TagClass::Bin).then(|| unsafe {
            // UNSAFE(@ohsayan): +tagck
            self.bin()
        })
    }
    #[allow(unused)]
    /// Attempt to read a string value
    pub fn try_str(&self) -> Option<&'a str> {
        (self.tag.tag_class() == TagClass::Str).then(|| unsafe {
            // UNSAFE(@ohsayan): +tagck
            self.str()
        })
    }
    /// Read a string value directly
    ///
    /// ## Safety
    /// The underlying repr MUST be a string. Otherwise you'll segfault or cause other library functions to misbehave
    pub unsafe fn str(&self) -> &'a str {
        str::from_utf8_unchecked(self.bin())
    }
    /// Read a binary value directly
    ///
    /// ## Safety
    /// The underlying repr MUST be a string. Otherwise you'll segfault
    pub unsafe fn bin(&self) -> &'a [u8] {
        let (q, n) = self.word.dwordqn_load_qw_nw();
        slice::from_raw_parts(n as *const u8 as *mut u8, q as _)
    }
}

impl<'a> Lit<'a> {
    /// Create a new string (referenced)
    pub fn new_str(s: &'a str) -> Self {
        unsafe {
            /*
                UNSAFE(@ohsayan): the mut cast is just for typesake so it doesn't matter while we also set DTC
                to none so it shouldn't matter anyway
            */
            Self::_str(s.as_ptr() as *mut u8, s.len(), Self::DTC_NONE)
        }
    }
    /// Create a new boxed string
    pub fn new_boxed_str(s: Box<str>) -> Self {
        let mut md = ManuallyDrop::new(s); // mut -> aliasing!
        unsafe {
            // UNSAFE(@ohsayan): correct aliasing, and DTC to destroy heap
            Self::_str(md.as_mut_ptr(), md.len(), Self::DTC_HSTR)
        }
    }
    /// Create a new string
    pub fn new_string(s: String) -> Self {
        Self::new_boxed_str(s.into_boxed_str())
    }
    /// Create a new binary (referenced)
    pub fn new_bin(b: &'a [u8]) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): mut cast is once again just a typesake change
            Self::_wide_word(b.as_ptr() as *mut _, b.len(), Self::DTC_NONE, FullTag::BIN)
        }
    }
}

impl<'a> Lit<'a> {
    /// Returns the type of this literal
    pub fn kind(&self) -> FullTag {
        self.tag
    }
    /// Returns the internal representation of this type
    pub unsafe fn data(&self) -> &SpecialPaddedWord {
        &self.word
    }
    pub fn __vdata(&self) -> &'a [u8] {
        let (vlen, data) = self.word.dwordqn_load_qw_nw();
        let len = vlen as usize * (self.kind().tag_unique() >= TagUnique::Bin) as usize;
        unsafe {
            // UNSAFE(@ohsayan): either because of static or lt
            slice::from_raw_parts(data as *const u8, len)
        }
    }
}

impl<'a> Lit<'a> {
    const DTC_NONE: u8 = 0;
    const DTC_HSTR: u8 = 1;
    unsafe fn _new(tag: FullTag, dtc: u8, word: SpecialPaddedWord) -> Self {
        Self {
            tag,
            dtc,
            word,
            _lt: PhantomData,
        }
    }
    fn _quad(quad: u64, tag: FullTag) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): we initialize the correct bit pattern
            Self::_new(tag, Self::DTC_NONE, SpecialPaddedWord::new_quad(quad))
        }
    }
    unsafe fn _wide_word(ptr: *mut u8, len: usize, dtc: u8, tag: FullTag) -> Self {
        Self::_new(tag, dtc, SpecialPaddedWord::new(len as _, ptr as _))
    }
    unsafe fn _str(ptr: *mut u8, len: usize, dtc: u8) -> Self {
        Self::_wide_word(ptr, len, dtc, FullTag::STR)
    }
    unsafe fn _drop_zero(_: SpecialPaddedWord) {}
    unsafe fn _drop_hstr(word: SpecialPaddedWord) {
        let (a, b) = word.dwordqn_load_qw_nw();
        drop(Vec::from_raw_parts(
            b as *const u8 as *mut u8,
            a as _,
            a as _,
        ));
    }
}

impl<'a> Drop for Lit<'a> {
    fn drop(&mut self) {
        static DFN: [unsafe fn(SpecialPaddedWord); 2] = [Lit::_drop_zero, Lit::_drop_hstr];
        unsafe { DFN[self.dtc as usize](core::mem::transmute_copy(&self.word)) }
    }
}

impl<'a> Clone for Lit<'a> {
    fn clone(&self) -> Lit<'a> {
        static CFN: [unsafe fn(SpecialPaddedWord) -> SpecialPaddedWord; 2] = unsafe {
            [
                |stack| core::mem::transmute(stack),
                |hstr| {
                    let (q, n) = hstr.dwordqn_load_qw_nw();
                    let mut md = ManuallyDrop::new(
                        slice::from_raw_parts(n as *const u8, q as usize).to_owned(),
                    );
                    md.shrink_to_fit();
                    SpecialPaddedWord::new(q, md.as_mut_ptr() as _)
                },
            ]
        };
        unsafe {
            Self::_new(
                self.tag,
                self.dtc,
                CFN[self.dtc as usize](core::mem::transmute_copy(&self.word)),
            )
        }
    }
}

impl<'a> fmt::Debug for Lit<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut field = f.debug_struct("Lit");
        field.field("tag", &self.tag);
        unsafe {
            macro_rules! d {
                ($expr:expr) => {{
                    field.field("data", &$expr);
                }};
            }
            match self.tag.tag_class() {
                TagClass::Bool => d!(self.bool()),
                TagClass::UnsignedInt => d!(self.uint()),
                TagClass::SignedInt => d!(self.sint()),
                TagClass::Float => d!(self.float()),
                TagClass::Bin => d!(self.bin()),
                TagClass::Str => d!(self.str()),
                TagClass::List => panic!("found 2D in 1D"),
            }
        }
        field.finish()
    }
}

impl<'a> Hash for Lit<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag.tag_unique().hash(state);
        self.__vdata().hash(state);
    }
}

impl<'a> PartialEq for Lit<'a> {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): +tagck
            match (self.tag.tag_class(), other.tag.tag_class()) {
                (TagClass::Bool, TagClass::Bool) => self.bool() == other.bool(),
                (TagClass::UnsignedInt, TagClass::UnsignedInt) => self.uint() == other.uint(),
                (TagClass::SignedInt, TagClass::SignedInt) => self.sint() == other.sint(),
                (TagClass::Float, TagClass::Float) => self.float() == other.float(),
                (TagClass::Bin, TagClass::Bin) => self.bin() == other.bin(),
                (TagClass::Str, TagClass::Str) => self.str() == other.str(),
                _ => false,
            }
        }
    }
}

direct_from! {
    Lit<'a> => {
        bool as new_bool,
        u64 as new_uint,
        i64 as new_sint,
        f64 as new_float,
        &'a str as new_str,
        String as new_string,
        Box<str> as new_boxed_str,
        &'a [u8] as new_bin,
    }
}

impl<'a> ToString for Lit<'a> {
    fn to_string(&self) -> String {
        unsafe {
            match self.kind().tag_class() {
                TagClass::Bool => self.bool().to_string(),
                TagClass::UnsignedInt => self.uint().to_string(),
                TagClass::SignedInt => self.sint().to_string(),
                TagClass::Float => self.float().to_string(),
                TagClass::Bin => format!("{:?}", self.bin()),
                TagClass::Str => format!("{:?}", self.str()),
                TagClass::List => panic!("found 2D in 1D"),
            }
        }
    }
}

#[sky_macros::test]
fn stk_variants() {
    let stk1 = [
        Lit::new_bool(true),
        Lit::new_uint(u64::MAX),
        Lit::new_sint(i64::MIN),
        Lit::new_float(f64::MIN),
        Lit::new_str("hello"),
        Lit::new_bin(b"world"),
    ];
    let stk2 = stk1.clone();
    assert_eq!(stk1, stk2);
}

#[sky_macros::test]
fn hp_variants() {
    let hp1 = [
        Lit::new_string("hello".into()),
        Lit::new_string("world".into()),
    ];
    let hp2 = hp1.clone();
    assert_eq!(hp1, hp2);
}

#[sky_macros::test]
fn lt_link() {
    let l = Lit::new_string("hello".into());
    let l_ir = l.as_ir();
    assert_eq!(l, l_ir);
}

#[sky_macros::test]
fn token_array_lt_test() {
    let tokens = vec![Lit::new_string("hello".to_string()), Lit::new_str("hi")];
    #[derive(Debug)]
    pub struct SelectStatement<'a> {
        primary_key: Lit<'a>,
        shorthand: Lit<'a>,
    }
    let select_stmt = SelectStatement {
        primary_key: tokens[0].as_ir(),
        shorthand: tokens[1].as_ir(),
    };
    {
        {
            let SelectStatement {
                primary_key,
                shorthand,
            } = &select_stmt;
            let _ = primary_key.as_ir();
            let _ = shorthand.as_ir();
        }
    }
    drop(select_stmt);
    drop(tokens);
}
