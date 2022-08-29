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

use core::hash::Hasher;
use std::marker::PhantomData;

/*
    FIXME(@ohsayan): honestly, adjust these for 16B output. I've left it at 8B for now,
    but I think we can get better distribution with higher hashcode lengths (since
    we can also hold more levels in the tree). Could we shove in 562.9 trillion values in a
    hyper 7lev? Maybe.
*/

/*
    Hashing policies:
    1. 32-bit
    2. 64-bit
*/

pub type Fnv1A32 = Fnv1A<Fnv1APolicy32A>;
pub type Fnv1A64 = Fnv1A<Fnv1APolicy64A>;

pub struct Fnv1APolicy32A;
impl HashPolicy for Fnv1APolicy32A {
    const OFFSET: u64 = 0x811C9DC5;
    const PRIME: u64 = 0x1000193;
}

pub struct Fnv1APolicy64A;
impl HashPolicy for Fnv1APolicy64A {
    const OFFSET: u64 = 0xCBF29CE484222325;
    const PRIME: u64 = 0x100000001B3;
}

pub trait HashPolicy {
    const PRIME: u64;
    const OFFSET: u64;
}

/*
    Impl
*/

pub struct Fnv1A<P: HashPolicy> {
    hash: u64,
    _m: PhantomData<P>,
}

impl<P: HashPolicy> Fnv1A<P> {
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
}

impl<P: HashPolicy> Hasher for Fnv1A<P> {
    fn finish(&self) -> u64 {
        // IMPORTANT: Do not reset hasher state
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        // FIXME(@ohsayan): We can actually have this padded and run together or will
        // the optimizer give us a holy blessing?
        for byte in bytes {
            self.hash ^= *byte as u64;
            self.hash = self.hash.wrapping_mul(P::PRIME as u64);
        }
    }
}
