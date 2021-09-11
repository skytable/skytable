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

use core::ops::Index;

/// A two-value boolean LUT
pub struct BoolTable {
    base: [&'static [u8]; 2],
}

impl BoolTable {
    /// Supply values in the order: `if_true` and `if_false`
    pub const fn new(if_true: &'static [u8], if_false: &'static [u8]) -> Self {
        Self {
            base: [if_false, if_true],
        }
    }
}

impl Index<bool> for BoolTable {
    type Output = &'static [u8];
    fn index(&self, index: bool) -> &Self::Output {
        unsafe { self.base.get_unchecked(index as usize) }
    }
}

/// A LUT based on niche values, especially built to support the `Option<bool>` optimized
/// structure
///
/// **Warning:** This is a terrible opt and only works on the Rust ABI
pub struct NicheLUT {
    base: [&'static [u8]; 3],
}

impl NicheLUT {
    /// Supply values in the following order: [`if_none`, `if_true`, `if_false`]
    pub const fn new(
        if_none: &'static [u8],
        if_true: &'static [u8],
        if_false: &'static [u8],
    ) -> Self {
        Self {
            // 0 == S(F); 1 == S(T); 2 == NULL
            base: [if_false, if_true, if_none],
        }
    }
}

impl Index<Option<bool>> for NicheLUT {
    type Output = &'static [u8];
    fn index(&self, idx: Option<bool>) -> &Self::Output {
        unsafe {
            self.base
                .get_unchecked(*(&idx as *const _ as *const u8) as usize)
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
