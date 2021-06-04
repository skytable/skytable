/*
 * Created on Fri Jun 04 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! SSE2 Vectorized implementations of group lookups for hosts that support them
use super::bitmask::Bitmask;
use super::control_bytes;

#[cfg(target_arch = "x86")]
use core::arch::x86;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64 as x86;
use core::mem;

pub type BitmaskWord = u16;
pub const BITMASK_STRIDE: usize = 1;
pub const BITMASK_MASK: BitmaskWord = 0xffff;

#[derive(Clone, Copy)]
pub struct Group(x86::__m128i);

impl Group {
    /// This will return the size of Self, which is a 128-bit wide integer vector (128-bit SIMD register)
    /// (intel platforms only)
    pub const WIDTH: usize = mem::size_of::<Self>();
    /// Returns a full group
    pub const fn empty_static() -> &'static [u8; Group::WIDTH] {
        #[repr(C)]
        struct AlignedBytes {
            // some explicit padding for alignment to ensure alignment to the group size
            _align: [Group; 0],
            bytes: [u8; Group::WIDTH],
        }
        const ALIGNED_BYTES: AlignedBytes = AlignedBytes {
            _align: [],
            bytes: [control_bytes::EMPTY; Group::WIDTH],
        };
        &ALIGNED_BYTES.bytes
    }

    /// Load a group of bytes starting at the given address (unaligned)
    pub unsafe fn load_unaligned(ptr: *const u8) -> Self {
        Group(x86::_mm_loadu_si128(ptr.cast()))
    }

    /// Load a group of bytes starting at the given address. This is an aligned read,
    /// and guranteed to be aligned to the alignment of the [`Group`]
    pub unsafe fn load_aligned(ptr: *const u8) -> Self {
        Group(x86::_mm_load_si128(ptr.cast()))
    }

    /// Store this group of bytes (self's) at the given address. This must be aligned
    /// to the alignment of the [`Group`]
    pub unsafe fn store_aligned(self, ptr: *mut u8) {
        x86::_mm_store_si128(ptr.cast(), self.0)
    }

    /// Returns a bitmask that gives us which bytes in the group have the
    /// given byte
    pub fn match_byte(self, byte: u8) -> Bitmask {
        unsafe {
            /*
             _mm_cmpeq_epi8 will two compare 8-bit packed integers in the Group: a __m128i in this case
             with _mm_set1_epi8 of the byte (basically setting all bits to the provided byte). To be more
             specific, we're doing this:  `pcmpeqb xmm, xmm` on the two SIMD registers. This result
             is stored in `cmp`. _mm_movemask_epi8 will return the most significant bit of each 8-bit
             element in `cmp` (one one the highest end). We then cast this to an unsigned 16-bit integer
             as _mm_movemask_epi8 returns an i32, with the high order bits zeroed. Finally, we return this
             as the Bitmask.
            */
            let cmp = x86::_mm_cmpeq_epi8(self.0, x86::_mm_set1_epi8(byte as i8));
            Bitmask(x86::_mm_movemask_epi8(cmp) as u16)
        }
    }

    /// Returns a bitmask with all the bytes in the group which are empty
    pub fn match_empty(self) -> Bitmask {
        self.match_byte(control_bytes::EMPTY)
    }

    /// Returns a bitmask indicating which all bytes in the group were empty or deleted
    pub fn match_empty_or_deleted(self) -> Bitmask {
        unsafe {
            // _mm_movemask_epi8 will again give us the most significant bits
            Bitmask(x86::_mm_movemask_epi8(self.0) as u16)
        }
    }

    /// Returns a bitmask indicating which all bytes in the group are full
    pub fn match_full(&self) -> Bitmask {
        self.match_empty_or_deleted().invert()
    }

    pub fn transform_full_to_deleted_and_special_to_empty(self) -> Self {
        /*
         for high bit = 1 (EMPTY/DELETED) => 1111_1111
         for high bit = 0 (FULL) => 1000_0000

         So we first compute if the byte is special. If high order bit is 1 => special,
         else it isn't special
         Now just apply a bitwise OR on every byte:
         So, 1111_1111 | 1000_0000 => 1111_1111
         And, 0000_0000 | 1000_0000 => 1000_0000
        */

        unsafe {
            // give us our zeroed vector
            let zero = x86::_mm_setzero_si128();
            // compute greater than for the given vectors
            let special = x86::_mm_cmpgt_epi8(zero, self.0);
            // do a lovely bitwise or on each byte (logical explanation is above)
            Group(x86::_mm_or_si128(
                special,
                x86::_mm_set1_epi8(0b10000000_u8 as i8),
            ))
        }
    }
}
