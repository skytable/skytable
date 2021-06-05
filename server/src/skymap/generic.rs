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

//! Implementations for CPU architectures that do not support SSE instructions
/*
    TODO(@ohsayan): Evaluate the need for NEON/AVX. Also note, SSE3/ SSE4 can
    prove to have much faster vector operations, but older CPUs may not support it (even worse,
    all those intrinsics are unstable on Rust, so that makes using them further problematic).
    Our job is to first build for SSE2 since that has the best support (all the way from Pentium
    chips). NEON has multi-cycle latencies, so that needs more evaluation.

    Note about the `GroupWord`s: we choose the target's pointer word width than just blindly
    using 64-bit pointer sizes because using 64-bit on 32-bit systems would only add to slowness
*/

use super::bitmask::Bitmask;
use super::control_bytes;
use core::mem;
use core::ptr;

cfg_if::cfg_if! {
    if #[cfg(any(
        target_pointer_width = "64",
        target_arch = "x86_64",
        target_arch = "aarch64"
    ))] {
        type GroupWord = u64;
    } else if #[cfg(target_pointer_width = "32")] {
        // make sure that we evaluate this as the host can be cross-compiling from 32 to 64 (cfg_if will do the nots for us)
        type GroupWord = u32;
    }
}
/// Just use the expected pointer width publicly for sanity
pub type BitmaskWord = GroupWord;

fn repeat(byte: u8) -> GroupWord {
    GroupWord::from_ne_bytes([byte; Group::WIDTH])
}

pub const BITMASK_STRIDE: usize = 8;
#[allow(clippy::unnecessary_cast)] // clippy doesn't know anything about target_arch
pub const BITMASK_MASK: BitmaskWord = 0x8080_8080_8080_8080_u64 as BitmaskWord;

/// A group of control-bytes that can be scanned in parallel
#[derive(Clone, Copy)]
pub struct Group(GroupWord);

impl Group {
    /// This will return either 32/64 depending on the target's pointer width
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

    /// Load a group of bytes starting at the provided address (unaligned read)
    pub unsafe fn load_unaligned(ptr: *const u8) -> Self {
        Group(ptr::read_unaligned(ptr.cast()))
    }

    /// Load a group of bytes starting at the provided address (aligned read)
    pub unsafe fn load_aligned(ptr: *const u8) -> Self {
        Group(ptr::read(ptr.cast()))
    }

    /// Store the [`Group`] in the given address. This is guaranteed to be aligned
    pub unsafe fn store_aligned(self, ptr: *mut u8) {
        ptr::write(ptr.cast(), self.0)
    }

    /// Returns a bitmask indicating which all bytes in the group _may_ have this value.
    /// This trick is derived from (the original site is inaccesible at times, for me at least):
    /// https://web.archive.org/web/20210523160500/http://graphics.stanford.edu/~seander/bithacks.html##ValueInWord.
    ///
    /// This however can return a false positive, but since after checking metadata for an entry,
    /// we _do_ check the value for equality, so this wouldn't cause anything to go wrong, fortunately.
    /// The drawback: a little loss on performance for the equality check in the case of a false positive,
    /// but this is extremely insignificant. This is something like C++'s
    /// [strchr](https://en.cppreference.com/w/c/string/byte/strchr)
    pub fn match_byte(self, byte: u8) -> Bitmask {
        let cmp = self.0 ^ repeat(byte);
        // change to little endian
        Bitmask((cmp.wrapping_sub(repeat(0x01)) & !cmp & repeat(0x80)).to_le())
    }

    /// Returns a bitmask indicating which all bytes were empty
    pub fn match_empty(self) -> Bitmask {
        // always change to little endian
        Bitmask((self.0 & (self.0 << 1)) & repeat(0x80).to_le())
    }

    /// Returns a bitmask indicating which all bytes were empty or deleted
    pub fn match_empty_or_deleted(self) -> Bitmask {
        // A byte is EMPTY or DELETED iff the high bit is set
        Bitmask((self.0 & repeat(0x80)).to_le())
    }

    /// Returns a bitmask indicating which all bytes were full
    pub fn match_full(self) -> Bitmask {
        self.match_empty_or_deleted().invert()
    }

    /// Transform DELETED => EMPTY, EMPTY => EMPTY (specials) and FULL => DELETED
    pub fn transform_full_to_deleted_and_special_to_empty(self) -> Self {
        /*
         If high order bit is 1 => EMPTY or DELETED => is special
         If high order bit is 0 => FULL => not special

         So we do this manually unlike SSE2 that does it for us:
         1. !0b1000_0000 => 1111111101111111
         2. 1111111101111111 + 1 => (11111111)10000000 (after shl 7)
         Similarly,
         1. !0b0000_0000 => 1111111111111111
         2. 1111111111111111 + 0 => (11111111)11111111 (after shl 7)
        */
        let full = !self.0 & repeat(0x80);
        Group(!full + (full >> 7))
    }
}
