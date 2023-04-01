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

/*
    So, I woke up and chose violence. God bless me and the stack memory. What I've done here is a sin. Do not follow my footsteps here if you want to write safe and maintainable code.
    -- @ohsayan
*/

use {
    super::tag::{DataTag, TagClass},
    crate::engine::mem::SystemDword,
    core::{fmt, mem, slice},
};

#[inline(always)]
fn when_then<T>(cond: bool, then: T) -> Option<T> {
    cond.then_some(then)
}

/// Information about the type that implements the dataspec traits
pub trait DataspecMeta1D: Sized {
    // assoc
    type Tag: DataTag;
    /// The target must be able to store (atleast) a native dword
    type Target: SystemDword;
    /// The string item. This helps us remain correct with the dtors
    type StringItem;
    // fn
    /// Create a new instance. Usually allocates zero memory *directly*
    fn new(tag: Self::Tag, data: Self::Target) -> Self;
    /// Returns the reduced dataflag
    fn kind(&self) -> Self::Tag;
    /// Returns the data stack
    fn data(&self) -> Self::Target;
}

/// Unsafe dtor/ctor impls for dataspec items. We have no clue about these things, the implementor must take care of them
///
/// ## Safety
///
/// - Your dtors MUST BE correct
pub unsafe trait DataspecRaw1D: DataspecMeta1D {
    /// Is the string heap allocated...anywhere down the line?
    const HEAP_STR: bool;
    /// Is the binary heap allocated...anywhere down the line?
    const HEAP_BIN: bool;
    /// Drop the string, if you need a dtor
    unsafe fn drop_str(&mut self);
    /// Drop the binary, if you need a dtor
    unsafe fn drop_bin(&mut self);
    /// Clone the string object. Note, we literally HAVE NO IDEA about what you're doing here
    unsafe fn clone_str(s: &str) -> Self::Target;
    /// Clone the binary object. Again, NOT A DAMN CLUE about whay you're doing down there
    unsafe fn clone_bin(b: &[u8]) -> Self::Target;
}

/// Functions that can be used to read/write to/from dataspec objects
///
/// ## Safety
/// - You must touch your targets by yourself
pub unsafe trait Dataspec1D: DataspecMeta1D + DataspecRaw1D {
    // store
    /// Store a new bool. This function is always safe to call
    #[allow(non_snake_case)]
    fn Bool(b: bool) -> Self {
        Self::new(Self::Tag::BOOL, SystemDword::store(b))
    }
    /// Store a new uint. This function is always safe to call
    #[allow(non_snake_case)]
    fn UnsignedInt(u: u64) -> Self {
        Self::new(Self::Tag::UINT, SystemDword::store(u))
    }
    /// Store a new sint. This function is always safe to call
    #[allow(non_snake_case)]
    fn SignedInt(s: i64) -> Self {
        Self::new(Self::Tag::SINT, SystemDword::store(s))
    }
    /// Store a new float. This function is always safe to call
    #[allow(non_snake_case)]
    fn Float(f: f64) -> Self {
        Self::new(Self::Tag::FLOAT, SystemDword::store(f.to_bits()))
    }
    /// Store a new binary. This function is always safe to call
    #[allow(non_snake_case)]
    fn Bin(b: &[u8]) -> Self {
        Self::new(Self::Tag::BIN, SystemDword::store((b.as_ptr(), b.len())))
    }

    /// Store a new string. Now, I won't talk about this one's safety because it depends on the implementor
    #[allow(non_snake_case)]
    fn Str(s: Self::StringItem) -> Self;

    // load
    // bool
    /// Load a bool (this is unsafe for logical verity)
    unsafe fn read_bool_uck(&self) -> bool {
        self.data().ld()
    }
    /// Load a bool
    fn read_bool_try(&self) -> Option<bool> {
        when_then(self.kind().tag_class() == TagClass::Bool, unsafe {
            // UNSAFE(@ohsayan): we've verified the flag. but lol because this isn't actually unsafe
            self.read_bool_uck()
        })
    }
    /// Load a bool
    /// ## Panics
    /// If you're not a bool, you panic
    fn bool(&self) -> bool {
        self.read_bool_try().unwrap()
    }
    // uint
    /// Load a uint (this is unsafe for logical verity)
    unsafe fn read_uint_uck(&self) -> u64 {
        self.data().ld()
    }
    /// Load a uint
    fn read_uint_try(&self) -> Option<u64> {
        when_then(self.kind().tag_class() == TagClass::UnsignedInt, unsafe {
            // UNSAFE(@ohsayan): we've verified the flag. but lol because this isn't actually unsafe
            self.read_uint_uck()
        })
    }
    /// Load a uint
    /// ## Panics
    /// If you're not a uint, you panic
    fn uint(&self) -> u64 {
        self.read_uint_try().unwrap()
    }
    // sint
    /// Load a sint (unsafe for logical verity)
    unsafe fn read_sint_uck(&self) -> i64 {
        self.data().ld()
    }
    /// Load a sint
    fn read_sint_try(&self) -> Option<i64> {
        when_then(self.kind().tag_class() == TagClass::SignedInt, unsafe {
            // UNSAFE(@ohsayan): we've verified the flag. but lol because this isn't actually unsafe
            self.read_sint_uck()
        })
    }
    /// Load a sint and panic if we're not a sint
    fn sint(&self) -> i64 {
        self.read_sint_try().unwrap()
    }
    // float
    /// Load a float (unsafe for logical verity)
    unsafe fn read_float_uck(&self) -> f64 {
        self.data().ld()
    }
    /// Load a float
    fn read_float_try(&self) -> Option<f64> {
        when_then(self.kind().tag_class() == TagClass::Float, unsafe {
            self.read_float_uck()
        })
    }
    /// Load a float and panic if we aren't one
    fn float(&self) -> f64 {
        self.read_float_try().unwrap()
    }
    // bin
    /// Load a binary
    ///
    /// ## Safety
    /// Are you a binary? Did you store it correctly? Are you a victim of segfaults?
    unsafe fn read_bin_uck(&self) -> &[u8] {
        let (p, l) = self.data().ld();
        slice::from_raw_parts(p, l)
    }
    /// Load a bin
    fn read_bin_try(&self) -> Option<&[u8]> {
        when_then(self.kind().tag_class() == TagClass::Bin, unsafe {
            self.read_bin_uck()
        })
    }
    /// Load a bin or panic if we aren't one
    fn bin(&self) -> &[u8] {
        self.read_bin_try().unwrap()
    }
    // str
    /// Load a str
    ///
    /// ## Safety
    /// Are you a str? Did you store it correctly? Are you a victim of segfaults?
    unsafe fn read_str_uck(&self) -> &str {
        mem::transmute(self.read_bin_uck())
    }
    /// Load a str
    fn read_str_try(&self) -> Option<&str> {
        when_then(self.kind().tag_class() == TagClass::Str, unsafe {
            self.read_str_uck()
        })
    }
    /// Load a str and panic if we aren't one
    fn str(&self) -> &str {
        self.read_str_try().unwrap()
    }
}

/// Common impls
///
/// ## Safety
/// - You are not touching your target
pub unsafe trait DataspecMethods1D: Dataspec1D {
    fn self_drop(&mut self) {
        match self.kind().tag_class() {
            TagClass::Str if <Self as DataspecRaw1D>::HEAP_STR => unsafe {
                // UNSAFE(@ohsayan): we are heap allocated, and we're calling the implementor's definition
                <Self as DataspecRaw1D>::drop_str(self)
            },
            TagClass::Bin if <Self as DataspecRaw1D>::HEAP_BIN => unsafe {
                // UNSAFE(@ohsayan): we are heap allocated, and we're calling the implementor's definition
                <Self as DataspecRaw1D>::drop_bin(self)
            },
            _ => {}
        }
    }
    fn self_clone(&self) -> Self {
        let data = match self.kind().tag_class() {
            TagClass::Str if <Self as DataspecRaw1D>::HEAP_STR => unsafe {
                // UNSAFE(@ohsayan): we are heap allocated, and we're calling the implementor's definition
                <Self as DataspecRaw1D>::clone_str(Dataspec1D::read_str_uck(self))
            },
            TagClass::Bin if <Self as DataspecRaw1D>::HEAP_BIN => unsafe {
                // UNSAFE(@ohsayan): we are heap allocated, and we're calling the implementor's definition
                <Self as DataspecRaw1D>::clone_bin(Dataspec1D::read_bin_uck(self))
            },
            _ => self.data(),
        };
        Self::new(self.kind(), data)
    }
    fn self_eq(&self, other: &impl DataspecMethods1D) -> bool {
        unsafe {
            // UNSAFE(@ohsayan): we are checking our flags
            match (self.kind().tag_class(), other.kind().tag_class()) {
                (TagClass::Bool, TagClass::Bool) => self.read_bool_uck() == other.read_bool_uck(),
                (TagClass::UnsignedInt, TagClass::UnsignedInt) => {
                    self.read_uint_uck() == other.read_uint_uck()
                }
                (TagClass::SignedInt, TagClass::SignedInt) => {
                    self.read_sint_uck() == other.read_sint_uck()
                }
                (TagClass::Float, TagClass::Float) => {
                    self.read_float_uck() == other.read_float_uck()
                }
                (TagClass::Bin, TagClass::Bin) => self.read_bin_uck() == other.read_bin_uck(),
                (TagClass::Str, TagClass::Str) => self.read_str_uck() == other.read_str_uck(),
                _ => false,
            }
        }
    }
    fn self_fmt_debug_data(&self, data_field: &str, f: &mut fmt::DebugStruct) {
        macro_rules! fmtdebug {
            ($($(#[$attr:meta])* $match:pat => $ret:expr),* $(,)?) => {
                match self.kind().tag_class() {$($(#[$attr])* $match => { let _x = $ret; f.field(data_field, &_x) },)*}
            }
        }
        unsafe {
            // UNSAFE(@ohsayan): we are checking our flags
            fmtdebug!(
                TagClass::Bool => self.read_bool_uck(),
                TagClass::UnsignedInt => self.read_uint_uck(),
                TagClass::SignedInt => self.read_sint_uck(),
                TagClass::Float => self.read_float_uck(),
                TagClass::Bin => self.read_bin_uck(),
                TagClass::Str => self.read_str_uck(),
                #[allow(unreachable_code)]
                TagClass::List => unreachable!("found 2D data in 1D"),
            )
        };
    }
    #[rustfmt::skip]
    fn to_string_debug(&self) -> String {
        match_data!(match ref self {
            Self::Bool(b) => b.to_string(),
            Self::UnsignedInt(u) => u.to_string(),
            Self::SignedInt(s) => s.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Bin(b) => format!("{:?}", b),
            Self::Str(s) => format!("{:?}", s),
            Self::List(_) => unreachable!("found 2D data in 1D"),
        })
    }
}
