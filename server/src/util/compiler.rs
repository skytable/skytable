/*
 * Created on Sat Jan 29 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

//! Dark compiler arts and hackery to defy the normal. Use at your own
//! risk

use core::mem;

#[cold]
#[inline(never)]
pub const fn cold() {}

pub const fn likely(b: bool) -> bool {
    if !b {
        cold()
    }
    b
}

pub const fn unlikely(b: bool) -> bool {
    if b {
        cold()
    }
    b
}

#[cold]
#[inline(never)]
pub const fn cold_err<T>(v: T) -> T {
    v
}
#[inline(always)]
#[allow(unused)]
pub const fn hot<T>(v: T) -> T {
    if false {
        cold()
    }
    v
}

pub const unsafe fn extend_lifetime<'a, 'b, T>(inp: &'a T) -> &'b T {
    mem::transmute(inp)
}
pub unsafe fn extend_lifetime_mut<'a, 'b, T>(inp: &'a mut T) -> &'b mut T {
    mem::transmute(inp)
}
