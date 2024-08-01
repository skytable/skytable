/*
 * Created on Thu Nov 23 2023
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
    derived from the implementation in libcore
*/

use core::{mem, ptr, slice};

pub trait Int {
    type Buffer: Default;
    fn init_buf() -> Self::Buffer {
        Self::Buffer::default()
    }
    fn init(self, buf: &mut Self::Buffer) -> &[u8];
}

pub struct IntegerRepr<I: Int> {
    b: I::Buffer,
}

impl<I: Int> IntegerRepr<I> {
    pub fn new() -> Self {
        Self { b: I::init_buf() }
    }
    pub fn as_bytes(&mut self, i: I) -> &[u8] {
        i.init(&mut self.b)
    }
    pub fn scoped<T>(i: I, mut f: impl FnMut(&[u8]) -> T) -> T {
        let mut slf = Self::new();
        f(slf.as_bytes(i))
    }
    #[cfg(test)]
    pub fn as_str(&mut self, i: I) -> &str {
        unsafe { core::mem::transmute(self.as_bytes(i)) }
    }
}

const DEC_DIGITS_LUT: &[u8] = b"\
      0001020304050607080910111213141516171819\
      2021222324252627282930313233343536373839\
      4041424344454647484950515253545556575859\
      6061626364656667686970717273747576777879\
      8081828384858687888990919293949596979899";

macro_rules! impl_int {
    ($($($int:ty => $max:literal),* as $cast:ty),*) => {
        $($(impl Int for $int {
            type Buffer = [u8; $max];
            fn init(self, buf: &mut Self::Buffer) -> &[u8] {
                #[allow(unused_comparisons)]
                let negative = self < 0;
                let mut n = if negative {
                    // two's complement (invert, add 1)
                    ((!(self as $cast)).wrapping_add(1))
                } else {
                    self as $cast
                };
                let mut curr_idx = buf.len() as isize;
                let buf_ptr = buf.as_mut_ptr();
                let lut_ptr = DEC_DIGITS_LUT.as_ptr();
                unsafe {
                    if mem::size_of::<Self>() >= 2 {
                        while n >= 10_000 {
                            let rem = (n % 10_000) as isize;
                            n /= 10_000;
                            let d1 = (rem / 100) << 1;
                            let d2 = (rem % 100) << 1;
                            curr_idx -= 4;
                            ptr::copy_nonoverlapping(lut_ptr.offset(d1), buf_ptr.offset(curr_idx), 2);
                            ptr::copy_nonoverlapping(lut_ptr.offset(d2), buf_ptr.offset(curr_idx + 2), 2);
                        }
                    }
                    // 4 chars left
                    let mut n = n as isize;
                    // 2 chars
                    if n >= 100 {
                        let d1 = (n % 100) << 1;
                        n /= 100;
                        curr_idx -= 2;
                        ptr::copy_nonoverlapping(lut_ptr.offset(d1), buf_ptr.offset(curr_idx), 2);
                    }
                    // 1 or 2 left
                    if n < 10 {
                        curr_idx -= 1;
                        *buf_ptr.offset(curr_idx) = (n as u8) + b'0';
                    } else {
                        let d1 = n << 1;
                        curr_idx -= 2;
                        ptr::copy_nonoverlapping(lut_ptr.offset(d1), buf_ptr.offset(curr_idx), 2);
                    }
                    if negative {
                        curr_idx -= 1;
                        *buf_ptr.offset(curr_idx) = b'-';
                    }
                    slice::from_raw_parts(buf_ptr.offset(curr_idx), buf.len() - curr_idx as usize)
                }
            }
        })*)*
    };
}

impl_int!(u8 => 3, i8 => 4, u16 => 5, i16 => 6, u32 => 10, i32 => 11 as u32, u64 => 20, i64 => 20 as u64);

#[cfg(test)]
mod tests {
    fn ibufeq<I: super::Int + ToString + Copy>(v: I) {
        let mut buf = super::IntegerRepr::new();
        assert_eq!(buf.as_str(v), v.to_string());
    }
    #[sky_macros::test]
    fn u8() {
        ibufeq(u8::MIN);
        ibufeq(u8::MAX);
    }
    #[sky_macros::test]
    fn i8() {
        ibufeq(i8::MIN);
        ibufeq(i8::MAX);
    }
    #[sky_macros::test]
    fn u16() {
        ibufeq(u16::MIN);
        ibufeq(u16::MAX);
    }
    #[sky_macros::test]
    fn i16() {
        ibufeq(i16::MIN);
        ibufeq(i16::MAX);
    }
    #[sky_macros::test]
    fn u32() {
        ibufeq(u32::MIN);
        ibufeq(u32::MAX);
    }
    #[sky_macros::test]
    fn i32() {
        ibufeq(i32::MIN);
        ibufeq(i32::MAX);
    }
    #[sky_macros::test]
    fn u64() {
        ibufeq(u64::MIN);
        ibufeq(u64::MAX);
    }
    #[sky_macros::test]
    fn i64() {
        ibufeq(i64::MIN);
        ibufeq(i64::MAX);
    }
}
