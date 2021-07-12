/*
 * Created on Mon Jul 12 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use super::array::Array;
use core::ops::Deref;
use core::str;

const PAIR_MAP_LUT: [u8; 200] = [
    0x30, 0x30, 0x30, 0x31, 0x30, 0x32, 0x30, 0x33, 0x30, 0x34, 0x30, 0x35, 0x30, 0x36, 0x30, 0x37,
    0x30, 0x38, 0x30, 0x39, // 0x30
    0x31, 0x30, 0x31, 0x31, 0x31, 0x32, 0x31, 0x33, 0x31, 0x34, 0x31, 0x35, 0x31, 0x36, 0x31, 0x37,
    0x31, 0x38, 0x31, 0x39, // 0x31
    0x32, 0x30, 0x32, 0x31, 0x32, 0x32, 0x32, 0x33, 0x32, 0x34, 0x32, 0x35, 0x32, 0x36, 0x32, 0x37,
    0x32, 0x38, 0x32, 0x39, // 0x32
    0x33, 0x30, 0x33, 0x31, 0x33, 0x32, 0x33, 0x33, 0x33, 0x34, 0x33, 0x35, 0x33, 0x36, 0x33, 0x37,
    0x33, 0x38, 0x33, 0x39, // 0x33
    0x34, 0x30, 0x34, 0x31, 0x34, 0x32, 0x34, 0x33, 0x34, 0x34, 0x34, 0x35, 0x34, 0x36, 0x34, 0x37,
    0x34, 0x38, 0x34, 0x39, // 0x34
    0x35, 0x30, 0x35, 0x31, 0x35, 0x32, 0x35, 0x33, 0x35, 0x34, 0x35, 0x35, 0x35, 0x36, 0x35, 0x37,
    0x35, 0x38, 0x35, 0x39, // 0x35
    0x36, 0x30, 0x36, 0x31, 0x36, 0x32, 0x36, 0x33, 0x36, 0x34, 0x36, 0x35, 0x36, 0x36, 0x36, 0x37,
    0x36, 0x38, 0x36, 0x39, // 0x36
    0x37, 0x30, 0x37, 0x31, 0x37, 0x32, 0x37, 0x33, 0x37, 0x34, 0x37, 0x35, 0x37, 0x36, 0x37, 0x37,
    0x37, 0x38, 0x37, 0x39, // 0x37
    0x38, 0x30, 0x38, 0x31, 0x38, 0x32, 0x38, 0x33, 0x38, 0x34, 0x38, 0x35, 0x38, 0x36, 0x38, 0x37,
    0x38, 0x38, 0x38, 0x39, // 0x38
    0x39, 0x30, 0x39, 0x31, 0x39, 0x32, 0x39, 0x33, 0x39, 0x34, 0x39, 0x35, 0x39, 0x36, 0x39, 0x37,
    0x39, 0x38, 0x39, 0x39, // 0x39
];

#[derive(Debug)]
/// A buffer for unsigned 32-bit integers with one _extra byte_ of memory reserved for
/// adding characters. On initialization (through [`Self::init`]), your integer will be
/// encoded and stored into the _unsafe array_
pub struct Integer32Buffer {
    inner_stack: Array<u8, 11>,
}

impl Integer32Buffer {
    /// Initialize a buffer
    pub fn init(integer: u32) -> Self {
        let mut slf = Self {
            inner_stack: Array::new(),
        };
        unsafe {
            slf._init_integer(integer);
        }
        slf
    }
    /// Initialize an integer. This is unsafe to be called outside because you'll be
    /// pushing in another integer and might end up corrupting your own stack as all
    /// pushes are unchecked!
    unsafe fn _init_integer(&mut self, mut val: u32) {
        if val < 10_000 {
            let d1 = (val / 100) << 1;
            let d2 = (val % 100) << 1;
            if val >= 1000 {
                self.inner_stack.push_unchecked(PAIR_MAP_LUT[d1 as usize]);
            }
            if val >= 100 {
                self.inner_stack
                    .push_unchecked(PAIR_MAP_LUT[(d1 + 1) as usize]);
            }
            if val >= 10 {
                self.inner_stack.push_unchecked(PAIR_MAP_LUT[d2 as usize]);
            }
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d2 + 1) as usize]);
        } else if val < 100_000_000 {
            let b = val / 10000;
            let c = val % 10000;
            let d1 = (b / 100) << 1;
            let d2 = (b % 100) << 1;
            let d3 = (c / 100) << 1;
            let d4 = (c % 100) << 1;

            if val > 10_000_000 {
                self.inner_stack.push_unchecked(PAIR_MAP_LUT[d1 as usize]);
            }
            if val > 1_000_000 {
                self.inner_stack
                    .push_unchecked(PAIR_MAP_LUT[(d1 + 1) as usize]);
            }
            if val > 100_000 {
                self.inner_stack.push_unchecked(PAIR_MAP_LUT[d2 as usize]);
            }
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d2 + 1) as usize]);
            self.inner_stack.push_unchecked(PAIR_MAP_LUT[d3 as usize]);
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d3 + 1) as usize]);
            self.inner_stack.push_unchecked(PAIR_MAP_LUT[d4 as usize]);
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d4 + 1) as usize]);
        } else {
            // worst, 1B or more
            let a = val / 100000000;
            val %= 100000000;

            if a >= 10 {
                let i = a << 1;
                self.inner_stack.push_unchecked(PAIR_MAP_LUT[i as usize]);
                self.inner_stack
                    .push_unchecked(PAIR_MAP_LUT[(i + 1) as usize]);
            } else {
                self.inner_stack.push_unchecked(0x30);
            }
            let b = val / 10000;
            let c = val % 10000;
            let d1 = (b / 100) << 1;
            let d2 = (b % 100) << 1;
            let d3 = (c / 100) << 1;
            let d4 = (c % 100) << 1;
            // write back
            self.inner_stack.push_unchecked(PAIR_MAP_LUT[d1 as usize]);
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d1 + 1) as usize]);
            self.inner_stack.push_unchecked(PAIR_MAP_LUT[d2 as usize]);
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d2 + 1) as usize]);
            self.inner_stack.push_unchecked(PAIR_MAP_LUT[d3 as usize]);
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d3 + 1) as usize]);
            self.inner_stack.push_unchecked(PAIR_MAP_LUT[d4 as usize]);
            self.inner_stack
                .push_unchecked(PAIR_MAP_LUT[(d4 + 1) as usize]);
        }
    }
    /// **This is very unsafe** Only push something when you know that the capacity won't overflow
    /// your allowance of 11 bytes. Oh no, there's no panic for you because you'll silently
    /// corrupt your own memory (or others' :/)
    pub unsafe fn push(&mut self, val: u8) {
        self.inner_stack.push_unchecked(val)
    }
}

impl Deref for Integer32Buffer {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        unsafe { str::from_utf8_unchecked(&self.inner_stack) }
    }
}

impl AsRef<str> for Integer32Buffer {
    fn as_ref(&self) -> &str {
        &self
    }
}

impl<T> PartialEq<T> for Integer32Buffer
where
    T: AsRef<str>,
{
    fn eq(&self, other_str: &T) -> bool {
        self.as_ref() == other_str.as_ref()
    }
}

#[test]
fn test_int32_buffer() {
    let buffer = Integer32Buffer::init(256);
    assert_eq!(buffer, 256.to_string());
}

#[test]
fn test_push() {
    let mut buffer = Integer32Buffer::init(278);
    unsafe {
        buffer.push(b'?');
    }
    assert_eq!(buffer, "278?");
}
