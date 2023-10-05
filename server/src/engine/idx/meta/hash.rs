/*
 * Created on Sat Apr 29 2023
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

use std::hash::{BuildHasher, Hasher};

pub type HasherNativeFx = HasherRawFx<usize>;

const ROTATE: u32 = 5;
const PRIME32: u32 = 0x9E3779B9; // golden
const PRIME64: u64 = 0x517CC1B727220A95; // archimedes (obtained from rustc)

pub trait WriteNumeric {
    fn self_u32(self) -> u32;
}

macro_rules! impl_numeric_writes {
    ($($ty:ty),*) => {
        $(impl WriteNumeric for $ty { fn self_u32(self) -> u32 { self as u32 } })*
    };
}

impl_numeric_writes!(u8, i8, u16, i16, u32, i32);

pub trait HashWord: Sized {
    const STATE: Self;
    fn fin(&self) -> u64;
    fn h_bytes(&mut self, bytes: &[u8]);
    fn h_quad(&mut self, quad: u64);
    fn h_word(&mut self, v: impl WriteNumeric);
}

impl HashWord for u32 {
    const STATE: Self = 0;
    fn fin(&self) -> u64 {
        (*self) as _
    }
    fn h_bytes(&mut self, mut bytes: &[u8]) {
        let mut state = *self;
        while bytes.len() >= 4 {
            // no need for ptr am; let opt with loop invariant
            state = self::hash32(
                state,
                u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            );
            bytes = &bytes[4..];
        }

        if bytes.len() >= 2 {
            state = self::hash32(state, u16::from_ne_bytes([bytes[0], bytes[1]]) as u32);
            bytes = &bytes[2..];
        }

        if !bytes.is_empty() {
            state = self::hash32(state, bytes[0] as u32);
        }

        *self = state;
    }
    fn h_quad(&mut self, quad: u64) {
        let mut state = *self;
        let [x, y]: [u32; 2] = unsafe { core::mem::transmute(quad.to_ne_bytes()) };
        state = self::hash32(state, x);
        state = self::hash32(state, y);
        *self = state;
    }
    fn h_word(&mut self, v: impl WriteNumeric) {
        *self = self::hash32(*self, v.self_u32());
    }
}

impl HashWord for u64 {
    const STATE: Self = 0;
    fn fin(&self) -> u64 {
        (*self) as _
    }
    fn h_bytes(&mut self, mut bytes: &[u8]) {
        let mut state = *self;
        while bytes.len() >= 8 {
            state = self::hash64(
                state,
                u64::from_ne_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]),
            );
            bytes = &bytes[8..];
        }

        if bytes.len() >= 4 {
            state = self::hash64(
                state,
                u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64,
            );
            bytes = &bytes[4..];
        }

        if bytes.len() >= 2 {
            state = self::hash64(state, u16::from_ne_bytes([bytes[0], bytes[1]]) as u64);
            bytes = &bytes[2..];
        }

        if !bytes.is_empty() {
            state = self::hash64(state, bytes[0] as u64);
        }

        *self = state;
    }
    fn h_quad(&mut self, quad: u64) {
        *self = self::hash64(*self, quad);
    }
    fn h_word(&mut self, v: impl WriteNumeric) {
        *self = self::hash64(*self, v.self_u32() as _)
    }
}

impl HashWord for usize {
    const STATE: Self = 0;
    fn fin(&self) -> u64 {
        (*self) as _
    }
    fn h_bytes(&mut self, bytes: &[u8]) {
        if cfg!(target_pointer_width = "32") {
            let mut slf = *self as u32;
            <u32 as HashWord>::h_bytes(&mut slf, bytes);
            *self = slf as usize;
        } else {
            let mut slf = *self as u64;
            <u64 as HashWord>::h_bytes(&mut slf, bytes);
            *self = slf as usize;
        }
    }
    fn h_quad(&mut self, quad: u64) {
        if cfg!(target_pointer_width = "32") {
            let mut slf = *self as u32;
            <u32 as HashWord>::h_quad(&mut slf, quad);
            *self = slf as usize;
        } else {
            let mut slf = *self as u64;
            <u64 as HashWord>::h_quad(&mut slf, quad);
            *self = slf as usize;
        }
    }
    fn h_word(&mut self, v: impl WriteNumeric) {
        if cfg!(target_pointer_width = "32") {
            let mut slf = *self as u32;
            <u32 as HashWord>::h_word(&mut slf, v);
            *self = slf as usize;
        } else {
            let mut slf = *self as u64;
            <u64 as HashWord>::h_word(&mut slf, v);
            *self = slf as usize;
        }
    }
}

fn hash32(state: u32, word: u32) -> u32 {
    (state.rotate_left(ROTATE) ^ word).wrapping_mul(PRIME32)
}
fn hash64(state: u64, word: u64) -> u64 {
    (state.rotate_left(ROTATE) ^ word).wrapping_mul(PRIME64)
}

#[derive(Debug)]
pub struct HasherRawFx<T>(T);

impl<T: HashWord> HasherRawFx<T> {
    pub const fn new() -> Self {
        Self(T::STATE)
    }
}

impl<T: HashWord> Hasher for HasherRawFx<T> {
    fn finish(&self) -> u64 {
        self.0.fin()
    }
    fn write(&mut self, bytes: &[u8]) {
        T::h_bytes(&mut self.0, bytes)
    }
    fn write_u8(&mut self, i: u8) {
        T::h_word(&mut self.0, i)
    }
    fn write_u16(&mut self, i: u16) {
        T::h_word(&mut self.0, i)
    }
    fn write_u32(&mut self, i: u32) {
        T::h_word(&mut self.0, i)
    }
    fn write_u64(&mut self, i: u64) {
        T::h_quad(&mut self.0, i)
    }
    fn write_u128(&mut self, i: u128) {
        let [a, b]: [u64; 2] = unsafe { core::mem::transmute(i) };
        T::h_quad(&mut self.0, a);
        T::h_quad(&mut self.0, b);
    }
    fn write_usize(&mut self, i: usize) {
        if cfg!(target_pointer_width = "32") {
            T::h_word(&mut self.0, i as u32);
        } else {
            T::h_quad(&mut self.0, i as u64);
        }
    }
}

impl<T: HashWord> BuildHasher for HasherRawFx<T> {
    type Hasher = Self;

    fn build_hasher(&self) -> Self::Hasher {
        Self::new()
    }
}

impl<T: HashWord> Default for HasherRawFx<T> {
    fn default() -> Self {
        Self::new()
    }
}
