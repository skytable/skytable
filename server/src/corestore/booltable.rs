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
