/*
 * Created on Thu Jan 18 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use std::alloc::{self, Layout};

pub unsafe fn alloc_layout<T>(layout: Layout) -> *mut T {
    let ptr = alloc::alloc(layout);
    assert!(!ptr.is_null(), "malloc failed");
    ptr as _
}

pub unsafe fn alloc_array<T>(l: usize) -> *mut T {
    self::alloc_layout(Layout::array::<T>(l).unwrap_unchecked())
}

pub unsafe fn dealloc_layout(ptr: *mut u8, layout: Layout) {
    alloc::dealloc(ptr, layout)
}

pub unsafe fn dealloc_array<T>(ptr: *mut T, l: usize) {
    if l != 0 {
        self::dealloc_layout(ptr as *mut u8, Layout::array::<T>(l).unwrap_unchecked())
    }
}

pub unsafe fn memcpy<const N: usize>(src: &[u8]) -> [u8; N] {
    let mut dst = [0u8; N];
    src.as_ptr().copy_to_nonoverlapping(dst.as_mut_ptr(), N);
    dst
}
