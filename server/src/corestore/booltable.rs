/*
 * Created on Fri Sep 10 2021
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

/*
    ⚠⚠⚠⚠ A WORD OF WARNING ⚠⚠⚠⚠
    This module contains some dark stuff (and asumptions) about type layouts and/or representations,
    things which can change from time to time. Do not rely on any of this!
*/

use core::ops::Index;

pub type BytesBoolTable = BoolTable<&'static [u8]>;
pub type BytesNicheLUT = NicheLUT<&'static [u8]>;

/// A two-value boolean LUT
pub struct BoolTable<T> {
    base: [T; 2],
}

impl<T> BoolTable<T> {
    /// Supply values in the order: `if_true` and `if_false`
    pub const fn new(if_true: T, if_false: T) -> Self {
        Self {
            base: [if_false, if_true],
        }
    }
}

impl<T> Index<bool> for BoolTable<T> {
    type Output = T;
    fn index(&self, index: bool) -> &Self::Output {
        unsafe { &*self.base.as_ptr().add(index as usize) }
    }
}

/// A LUT based on niche values, especially built to support the `Option<bool>` optimized
/// structure
///
/// **Warning:** This is a terrible opt and only works on the Rust ABI
pub struct NicheLUT<T> {
    base: [T; 3],
}

impl<T> NicheLUT<T> {
    /// Supply values in the following order: [`if_none`, `if_true`, `if_false`]
    pub const fn new(if_none: T, if_true: T, if_false: T) -> Self {
        Self {
            // 0 == S(F); 1 == S(T); 2 == NULL
            base: [if_false, if_true, if_none],
        }
    }
}

impl<T> Index<Option<bool>> for NicheLUT<T> {
    type Output = T;
    fn index(&self, idx: Option<bool>) -> &Self::Output {
        unsafe {
            &*self
                .base
                .as_ptr()
                .add(*(&idx as *const _ as *const u8) as usize)
        }
    }
}

#[test]
fn niche_optim_sanity_test() {
    let none: Option<bool> = None;
    let some_t: Option<bool> = Some(true);
    let some_f: Option<bool> = Some(false);
    unsafe {
        let r_some_f = &some_f as *const _ as *const u8;
        let r_some_t = &some_t as *const _ as *const u8;
        let r_none = &none as *const _ as *const u8;
        assert_eq!(*r_some_f, 0);
        assert_eq!(*r_some_t, 1);
        assert_eq!(*r_none, 2);
    }
}

/// A 2-bit indexed boolean LUT
pub struct TwoBitLUT<T> {
    base: [T; 4],
}

type Bit = bool;
type TwoBitIndex = (Bit, Bit);

impl<T> TwoBitLUT<T> {
    /// Supply values in the following order:
    /// - 1st unset, 2nd unset
    /// - 1st unset, 2nd set
    /// - 1st set, 2nd unset
    /// - 1st set, 2nd set
    pub const fn new(ff: T, ft: T, tf: T, tt: T) -> Self {
        Self {
            base: [ff, ft, tf, tt],
        }
    }
}

impl<T> Index<TwoBitIndex> for TwoBitLUT<T> {
    type Output = T;
    fn index(&self, (bit_a, bit_b): TwoBitIndex) -> &Self::Output {
        unsafe {
            &*self
                .base
                .as_ptr()
                .add((((bit_a as u8) << 1) + (bit_b as u8)) as usize)
        }
    }
}

#[test]
fn test_two_bit_indexed_lut() {
    let (bit_a, bit_b) = unsafe { tmut_bool!(0, 0) };
    let twobitlut = TwoBitLUT::new('a', 'b', 'c', 'd');
    // the operators, are just for sanity
    assert_eq!('d', twobitlut[(!bit_a, !bit_b)]);
    assert_eq!('c', twobitlut[(!bit_a, bit_b)]);
    assert_eq!('b', twobitlut[(bit_a, !bit_b)]);
    assert_eq!('a', twobitlut[(bit_a, bit_b)]);
}
