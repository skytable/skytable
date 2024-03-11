/*
 * Created on Mon Feb 27 2023
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

macro_rules! strid {
    ($(#[$attr:meta])*$vis:vis enum $enum:ident {$($(#[$var_attr:meta])* $variant:ident $(= $dscr:expr)?),* $(,)?}) => {
        $(#[$attr])* $vis enum $enum { $($(#[$var_attr])* $variant $(= $dscr)?),*}
        impl $enum {
            pub const fn name_str(&self) -> &'static str { match self { $(Self::$variant => stringify!($variant),)* } }
        }
    }
}

#[repr(u8)]
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    sky_macros::EnumMethods,
    sky_macros::TaggedEnum,
)]
pub enum TagClass {
    Bool = 0,
    UnsignedInt = 1,
    SignedInt = 2,
    Float = 3,
    Bin = 4,
    Str = 5,
    List = 6,
}

strid! {
    #[repr(u8)]
    #[derive(
        Debug,
        PartialEq,
        Eq,
        Clone,
        Copy,
        Hash,
        PartialOrd,
        Ord,
        sky_macros::EnumMethods,
        sky_macros::TaggedEnum,
    )]
    pub enum TagSelector {
        Bool = 0,
        UInt8 = 1,
        UInt16 = 2,
        UInt32 = 3,
        UInt64 = 4,
        SInt8 = 5,
        SInt16 = 6,
        SInt32 = 7,
        SInt64 = 8,
        Float32 = 9,
        Float64 = 10,
        Binary = 11,
        String = 12,
        List = 13,
    }
}

impl TagSelector {
    pub const fn into_full(self) -> FullTag {
        FullTag::new(self.tag_class(), self, self.tag_unique())
    }
    pub const fn tag_unique(&self) -> TagUnique {
        [
            TagUnique::Illegal,     // bool
            TagUnique::UnsignedInt, // uint8
            TagUnique::UnsignedInt, // uint16
            TagUnique::UnsignedInt, // uint32
            TagUnique::UnsignedInt, // uint64
            TagUnique::SignedInt,   // sint8
            TagUnique::SignedInt,   // sint16
            TagUnique::SignedInt,   // sint32
            TagUnique::SignedInt,   // sint64
            TagUnique::Illegal,     // f32
            TagUnique::Illegal,     // f64
            TagUnique::Bin,         // bin
            TagUnique::Str,         // str
            TagUnique::Illegal,     // list
        ][self.value_word()]
    }
    pub const fn tag_class(&self) -> TagClass {
        [
            TagClass::Bool,        // bool
            TagClass::UnsignedInt, // uint8
            TagClass::UnsignedInt, // uint16
            TagClass::UnsignedInt, // uint32
            TagClass::UnsignedInt, // uint64
            TagClass::SignedInt,   // sint8
            TagClass::SignedInt,   // sint16
            TagClass::SignedInt,   // sint32
            TagClass::SignedInt,   // sint64
            TagClass::Float,       // f32
            TagClass::Float,       // f64
            TagClass::Bin,         // bin
            TagClass::Str,         // str
            TagClass::List,        // recursive list
        ][self.value_word()]
    }
}

#[repr(u8)]
#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    sky_macros::EnumMethods,
    sky_macros::TaggedEnum,
)]
pub enum TagUnique {
    UnsignedInt = 0,
    SignedInt = 1,
    Bin = 2,
    Str = 3,
    Illegal = 0xFF,
}

impl TagUnique {
    pub const fn is_unique(&self) -> bool {
        self.value_u8() != Self::Illegal.value_u8()
    }
}

pub trait DataTag {
    const BOOL: Self;
    const UINT: Self;
    const SINT: Self;
    const FLOAT: Self;
    const BIN: Self;
    const STR: Self;
    const LIST: Self;
    fn tag_class(&self) -> TagClass;
    fn tag_selector(&self) -> TagSelector;
    fn tag_unique(&self) -> TagUnique;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct FullTag {
    class: TagClass,
    selector: TagSelector,
    unique: TagUnique,
}

impl FullTag {
    const fn new(class: TagClass, selector: TagSelector, unique: TagUnique) -> Self {
        Self {
            class,
            selector,
            unique,
        }
    }
    pub const fn new_uint(selector: TagSelector) -> Self {
        Self::new(TagClass::UnsignedInt, selector, TagUnique::UnsignedInt)
    }
    pub const fn new_sint(selector: TagSelector) -> Self {
        Self::new(TagClass::SignedInt, selector, TagUnique::SignedInt)
    }
    pub const fn new_float(selector: TagSelector) -> Self {
        Self::new(TagClass::Float, selector, TagUnique::Illegal)
    }
}

macro_rules! fulltag {
    ($class:ident, $selector:ident, $unique:ident) => {
        FullTag::new(TagClass::$class, TagSelector::$selector, TagUnique::$unique)
    };
    ($class:ident, $selector:ident) => {
        fulltag!($class, $selector, Illegal)
    };
}

impl DataTag for FullTag {
    const BOOL: Self = fulltag!(Bool, Bool);
    const UINT: Self = fulltag!(UnsignedInt, UInt64, UnsignedInt);
    const SINT: Self = fulltag!(SignedInt, SInt64, SignedInt);
    const FLOAT: Self = fulltag!(Float, Float64);
    const BIN: Self = fulltag!(Bin, Binary, Bin);
    const STR: Self = fulltag!(Str, String, Str);
    const LIST: Self = fulltag!(List, List);
    fn tag_class(&self) -> TagClass {
        self.class
    }
    fn tag_selector(&self) -> TagSelector {
        self.selector
    }
    fn tag_unique(&self) -> TagUnique {
        self.unique
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
#[repr(transparent)]
pub struct UIntSpec(FullTag);
impl UIntSpec {
    pub const LIM_MAX: [u64; 4] = as_array![u8::MAX, u16::MAX, u32::MAX, u64::MAX];
    pub const DEFAULT: Self = Self::UINT64;
    const UINT64: Self = Self(FullTag::new_uint(TagSelector::UInt64));
    pub const unsafe fn from_full(f: FullTag) -> Self {
        Self(f)
    }
    pub fn check(&self, v: u64) -> bool {
        v <= Self::LIM_MAX[self.0.tag_selector().value_word() - 1]
    }
}

impl From<UIntSpec> for FullTag {
    fn from(value: UIntSpec) -> Self {
        value.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
#[repr(transparent)]
pub struct SIntSpec(FullTag);
impl SIntSpec {
    pub const LIM_MIN: [i64; 4] = as_array![i8::MIN, i16::MIN, i32::MIN, i64::MIN];
    pub const LIM_MAX: [i64; 4] = as_array![i8::MAX, i16::MAX, i32::MAX, i64::MAX];
    pub const DEFAULT: Self = Self::SINT64;
    const SINT64: Self = Self(FullTag::new_sint(TagSelector::SInt64));
    pub const unsafe fn from_full(f: FullTag) -> Self {
        Self(f)
    }
    pub fn check(&self, i: i64) -> bool {
        let tag = self.0.tag_selector().value_word() - 5;
        (i >= Self::LIM_MIN[tag]) & (i <= Self::LIM_MAX[tag])
    }
}

impl From<SIntSpec> for FullTag {
    fn from(value: SIntSpec) -> Self {
        value.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
#[repr(transparent)]
pub struct FloatSpec(FullTag);
impl FloatSpec {
    pub const LIM_MIN: [f64; 2] = as_array![f32::MIN, f64::MIN];
    pub const LIM_MAX: [f64; 2] = as_array![f32::MAX, f64::MAX];
    pub const DEFAULT: Self = Self::F64;
    const F64: Self = Self(FullTag::new_float(TagSelector::Float64));
    pub const unsafe fn from_full(f: FullTag) -> Self {
        Self(f)
    }
    pub fn check(&self, f: f64) -> bool {
        let tag = self.0.tag_selector().value_word() - 9;
        (f >= Self::LIM_MIN[tag]) & (f <= Self::LIM_MAX[tag])
    }
}

impl From<FloatSpec> for FullTag {
    fn from(value: FloatSpec) -> Self {
        value.0
    }
}
