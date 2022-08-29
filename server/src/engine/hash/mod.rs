/*
 * Created on Mon Aug 29 2022
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

/*
    This module implements the Fowler-Noll-Vo hash function
*/

use std::{hash::Hasher, marker::PhantomData, mem, ops::BitXor, ptr};

/*
    FIXME(@ohsayan): honestly, adjust these for 16B output. I've left it at 8B for now,
    but I think we can get better distribution with higher hashcode lengths (since
    we can also hold more levels in the tree). Could we shove in 562.9 trillion values in a
    hyper 7lev? Maybe.
*/

/*
    How can you 'not memory'?
*/

#[inline(always)]
/// Literally a memcpy except that you set the dst type
///
/// ## Safety
/// You must ensure that the source buffer has **atleast** sizeof(dst type) bytes
unsafe fn copy_block_exact<T>(src: &[u8], dst: &mut T) {
    debug_assert!(
        src.len() >= mem::size_of::<T>(),
        "SRC must have atleast sizeof(DST_TY) bytes"
    );
    ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T as *mut u8, mem::size_of::<T>());
}

#[inline(always)]
/// Ensure source buffer has atleast 8B
unsafe fn read_hashrc64(buf: &[u8]) -> u64 {
    debug_assert!(buf.len() >= 8);
    let mut data: u64 = 0;
    copy_block_exact(buf, &mut data);
    data
}

#[inline(always)]
/// Ensure source buffer has alteast 4B
unsafe fn read_hashrc32(buf: &[u8]) -> u32 {
    debug_assert!(buf.len() >= 4);
    let mut data: u32 = 0;
    copy_block_exact(buf, &mut data);
    data
}

/*
    Hashing policies:
    1. 32-bit
    2. 64-bit
*/

/// 32-bit FNV hash (1A)
pub type Fnv1A32 = Fnv1A<Fnv1APolicy32A>;
/// 64-bit FNV hash (1A)
pub type Fnv1A64 = Fnv1A<Fnv1APolicy64A>;

/// A hashing policy for the FNV algorithm
///
/// Usually providing the prime and offset values is enough, but you can choose to specialize the
/// [`calculate_hash`] function.
pub trait FnvHashPolicy: Sized {
    type SeedType: Sized;
    const PRIME: u64;
    const OFFSET: u64;
    const WORDSIZE: usize = mem::size_of::<Self::SeedType>();
    #[inline(always)]
    fn calculate_hash(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(Self::PRIME as u64);
        }
        hash
    }
}

pub struct Fnv1APolicy32A;
impl FnvHashPolicy for Fnv1APolicy32A {
    type SeedType = u32;
    const OFFSET: u64 = 0x811C9DC5;
    const PRIME: u64 = 0x1000193;
    // specialize 32-bit scan
    fn calculate_hash(mut hash: u64, mut bytes: &[u8]) -> u64 {
        while bytes.len() >= Self::WORDSIZE {
            let word = unsafe { read_hashrc32(bytes) };
            hash = hash.bitxor(word as u64).wrapping_mul(Self::PRIME);
            bytes = &bytes[Self::WORDSIZE..];
        }
        for byte in bytes {
            hash = hash.bitxor(*byte as u64).wrapping_mul(Self::PRIME);
        }
        hash
    }
}

pub struct Fnv1APolicy64A;
impl FnvHashPolicy for Fnv1APolicy64A {
    type SeedType = u64;
    const OFFSET: u64 = 0xCBF29CE484222325;
    const PRIME: u64 = 0x100000001B3;
    // specialize 64-bit scan
    fn calculate_hash(mut hash: u64, mut bytes: &[u8]) -> u64 {
        while bytes.len() >= Self::WORDSIZE {
            let word = unsafe { read_hashrc64(bytes) };
            hash = hash.bitxor(word as u64).wrapping_mul(Self::PRIME);
            bytes = &bytes[Self::WORDSIZE..];
        }
        for byte in bytes {
            hash = hash.bitxor(*byte as u64).wrapping_mul(Self::PRIME);
        }
        hash
    }
}

/*
    Impl
*/

pub struct Fnv1A<P: FnvHashPolicy> {
    hash: u64,
    _m: PhantomData<P>,
}

impl<P: FnvHashPolicy> Fnv1A<P> {
    pub const fn new() -> Self {
        Self {
            hash: P::OFFSET as _,
            _m: PhantomData,
        }
    }
    pub const fn hash(src: &[u8]) -> u64 {
        let mut hash = P::OFFSET;
        let mut i = 0;
        while i < src.len() {
            hash ^= src[i] as u64;
            hash = hash.wrapping_mul(P::PRIME as u64);
            i += 1;
        }
        hash
    }
    pub fn hash_faster(src: &[u8]) -> u64 {
        P::calculate_hash(P::OFFSET, src)
    }
}

impl<P: FnvHashPolicy> Hasher for Fnv1A<P> {
    fn finish(&self) -> u64 {
        // IMPORTANT: Do not reset hasher state
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        // allow specialization to take over
        self.hash = P::calculate_hash(self.hash, bytes);
    }
}

#[test]
fn hash() {
    let hash_hello = Fnv1A64::hash_faster(b"hello");
    let hash_world = Fnv1A64::hash_faster(b"world");
    assert!(hash_hello != hash_world); // know unequal hashes
}
