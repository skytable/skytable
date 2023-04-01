/*
 * Created on Wed Mar 01 2023
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

use super::{NativeDword, NativeQword, NativeTword, SpecialPaddedWord};

static ZERO_BLOCK: [u8; 0] = [];

/// Native quad pointer stack (must also be usable as a double and triple pointer stack. see [`SystemTword`] and [`SystemDword`])
pub trait SystemQword: SystemTword {
    fn store_full(a: usize, b: usize, c: usize, d: usize) -> Self;
    fn load_full(&self) -> [usize; 4];
    fn store<'a, T>(v: T) -> Self
    where
        T: WordRW<Self>,
    {
        WordRW::store(v)
    }
    fn ld<'a, T>(&'a self) -> T
    where
        T: WordRW<Self, Target<'a> = T>,
    {
        <T>::load(self)
    }
}

/// Native tripe pointer stack (must also be usable as a double pointer stack, see [`SystemDword`])
pub trait SystemTword: SystemDword {
    fn store_full(a: usize, b: usize, c: usize) -> Self;
    fn load_full(&self) -> [usize; 3];
    fn store<'a, T>(v: T) -> Self
    where
        T: WordRW<Self>,
    {
        WordRW::store(v)
    }
    fn ld<'a, T>(&'a self) -> T
    where
        T: WordRW<Self, Target<'a> = T>,
    {
        <T>::load(self)
    }
}

/// Native double pointer stack
pub trait SystemDword: Sized {
    fn store_qw(u: u64) -> Self;
    fn store_fat(a: usize, b: usize) -> Self;
    fn load_qw(&self) -> u64;
    fn load_fat(&self) -> [usize; 2];
    fn store<'a, T>(v: T) -> Self
    where
        T: WordRW<Self>,
    {
        WordRW::store(v)
    }
    fn ld<'a, T>(&'a self) -> T
    where
        T: WordRW<Self, Target<'a> = T>,
    {
        <T>::load(self)
    }
}

impl SystemDword for SpecialPaddedWord {
    fn store_qw(u: u64) -> Self {
        Self::new(u, ZERO_BLOCK.as_ptr() as usize)
    }
    fn store_fat(a: usize, b: usize) -> Self {
        Self::new(a as u64, b)
    }
    fn load_qw(&self) -> u64 {
        self.a
    }
    fn load_fat(&self) -> [usize; 2] {
        [self.a as usize, self.b]
    }
}

impl SystemDword for NativeDword {
    #[inline(always)]
    fn store_qw(u: u64) -> Self {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            x = unsafe {
                // UNSAFE(@ohsayan): same layout and this is a stupidly simple cast and it's wild that the rust std doesn't have a simpler way to do it
                core::mem::transmute(u)
            };
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = [u as usize, 0]
        }
        Self(x)
    }
    #[inline(always)]
    fn store_fat(a: usize, b: usize) -> Self {
        Self([a, b])
    }
    #[inline(always)]
    fn load_qw(&self) -> u64 {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            x = unsafe {
                // UNSAFE(@ohsayan): same layout and this is a stupidly simple cast and it's wild that the rust std doesn't have a simpler way to do it
                core::mem::transmute_copy(self)
            }
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = self.0[0] as _;
        }
        x
    }
    #[inline(always)]
    fn load_fat(&self) -> [usize; 2] {
        self.0
    }
}

impl SystemTword for NativeTword {
    #[inline(always)]
    fn store_full(a: usize, b: usize, c: usize) -> Self {
        Self([a, b, c])
    }
    #[inline(always)]
    fn load_full(&self) -> [usize; 3] {
        self.0
    }
}

impl SystemDword for NativeTword {
    #[inline(always)]
    fn store_qw(u: u64) -> Self {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            let [a, b]: [usize; 2] = unsafe {
                // UNSAFE(@ohsayan): same layout and this is a stupidly simple cast and it's wild that the rust std doesn't have a simpler way to do it
                core::mem::transmute(u)
            };
            x = [a, b, 0];
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = [u as _, 0, 0];
        }
        Self(x)
    }
    #[inline(always)]
    fn store_fat(a: usize, b: usize) -> Self {
        Self([a, b, 0])
    }
    #[inline(always)]
    fn load_qw(&self) -> u64 {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            let ab = [self.0[0], self.0[1]];
            x = unsafe {
                // UNSAFE(@ohsayan): same layout and this is a stupidly simple cast and it's wild that the rust std doesn't have a simpler way to do it
                core::mem::transmute(ab)
            };
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = self.0[0] as _;
        }
        x
    }
    #[inline(always)]
    fn load_fat(&self) -> [usize; 2] {
        [self.0[0], self.0[1]]
    }
}

impl SystemQword for NativeQword {
    fn store_full(a: usize, b: usize, c: usize, d: usize) -> Self {
        Self([a, b, c, d])
    }
    fn load_full(&self) -> [usize; 4] {
        self.0
    }
}

impl SystemTword for NativeQword {
    fn store_full(a: usize, b: usize, c: usize) -> Self {
        Self([a, b, c, 0])
    }
    fn load_full(&self) -> [usize; 3] {
        [self.0[0], self.0[1], self.0[2]]
    }
}

impl SystemDword for NativeQword {
    fn store_qw(u: u64) -> Self {
        let ret;
        #[cfg(target_pointer_width = "32")]
        {
            let [a, b]: [usize; 2] = unsafe {
                // UNSAFE(@ohsayan): same layout and this is a stupidly simple cast and it's wild that the rust std doesn't have a simpler way to do it
                core::mem::transmute(u)
            };
            ret = <Self as SystemQword>::store_full(a, b, 0, 0);
        }
        #[cfg(target_pointer_width = "64")]
        {
            ret = <Self as SystemQword>::store_full(u as _, 0, 0, 0);
        }
        ret
    }
    fn store_fat(a: usize, b: usize) -> Self {
        <Self as SystemQword>::store_full(a, b, 0, 0)
    }
    fn load_qw(&self) -> u64 {
        let ret;
        #[cfg(target_pointer_width = "32")]
        {
            ret = unsafe {
                // UNSAFE(@ohsayan): same layout and this is a stupidly simple cast and it's wild that the rust std doesn't have a simpler way to do it
                core::mem::transmute([self.0[0], self.0[1]])
            };
        }
        #[cfg(target_pointer_width = "64")]
        {
            ret = self.0[0] as _;
        }
        ret
    }
    fn load_fat(&self) -> [usize; 2] {
        [self.0[0], self.0[1]]
    }
}

pub trait WordRW<W> {
    type Target<'a>
    where
        W: 'a;
    fn store(self) -> W;
    fn load<'a>(word: &'a W) -> Self::Target<'a>;
}

macro_rules! impl_wordrw {
	($($ty:ty as $minword:ident => { type Target<'a> = $target:ty; |$selfname:ident| $store:expr; |$wordarg:ident| $load:expr;})*) => {
		$(impl<W: $minword> WordRW<W> for $ty { type Target<'a> = $target where W: 'a; fn store($selfname: Self) -> W { $store } fn load<'a>($wordarg: &'a W) -> Self::Target<'a> { $load } })*
	};
	($($ty:ty as $minword:ident => { |$selfname:ident| $store:expr; |$wordarg:ident| $load:expr;})*) => { impl_wordrw!($($ty as $minword => { type Target<'a> = $ty; |$selfname| $store; |$wordarg| $load;})*); };
}

impl_wordrw! {
    bool as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) == 1;
    }
    u8 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as u8;
    }
    u16 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as u16;
    }
    u32 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as u32;
    }
    u64 as SystemDword => {
        |self| SystemDword::store_qw(self);
        |word| SystemDword::load_qw(word);
    }
    i8 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as i8;
    }
    i16 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as i16;
    }
    i32 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as i32;
    }
    i64 as SystemDword => {
        |self| SystemDword::store_qw(self as _);
        |word| SystemDword::load_qw(word) as i64;
    }
    f32 as SystemDword => {
        |self| SystemDword::store_qw(self.to_bits() as u64);
        |word| f32::from_bits(SystemDword::load_qw(word) as u32);
    }
    f64 as SystemDword => {
        |self| SystemDword::store_qw(self.to_bits());
        |word| f64::from_bits(SystemDword::load_qw(word));
    }
    [usize; 2] as SystemDword => {
        |self| SystemDword::store_fat(self[0], self[1]);
        |word| SystemDword::load_fat(word);
    }
    (*mut u8, usize) as SystemDword => {
        |self| SystemDword::store_fat(self.0 as usize, self.1);
        |word| {
            let [a, b] = word.load_fat();
            (a as *mut u8, b)
        };
    }
    (*const u8, usize) as SystemDword => {
        |self| SystemDword::store_fat(self.0 as usize, self.1);
        |word| {
            let [a, b] = word.load_fat();
            (a as *const u8, b)
        };
    }
}
