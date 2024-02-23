/*
 * Created on Sun Jan 22 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

mod astr;
pub mod fixed_vec;
mod ll;
mod numbuf;
mod rawslice;
pub mod scanner;
mod stackop;
mod uarray;
pub mod unsafe_apis;
mod vinline;
mod word;
// test
#[cfg(test)]
mod tests;
// re-exports
pub use {
    astr::AStr,
    ll::CachePadded,
    numbuf::IntegerRepr,
    rawslice::RawStr,
    scanner::BufferedScanner,
    uarray::UArray,
    vinline::VInline,
    word::{DwordNN, DwordQN, WordIO, ZERO_BLOCK},
};

/// Native double pointer width (note, native != arch native, but host native)
pub struct NativeDword([usize; 2]);
/// Native triple pointer width (note, native != arch native, but host native)
pub struct NativeTword([usize; 3]);
/// Native quad pointer width (note, native != arch native, but host native)
pub struct NativeQword([usize; 4]);
/// A special word with a special bit pattern padded (with a quad)
///
/// **WARNING**: DO NOT EXPECT this to have the same bit pattern as that of native word sizes. It's called "special" FOR A REASON
pub struct SpecialPaddedWord {
    a: u64,
    b: usize,
}

impl SpecialPaddedWord {
    pub const unsafe fn new(a: u64, b: usize) -> Self {
        Self { a, b }
    }
    pub fn new_quad(a: u64) -> Self {
        Self {
            a,
            b: ZERO_BLOCK.as_ptr() as usize,
        }
    }
}

pub trait StatelessLen {
    fn stateless_len(&self) -> usize;
    fn stateless_empty(&self) -> bool {
        self.stateless_len() == 0
    }
}

impl<T> StatelessLen for Vec<T> {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl<T> StatelessLen for Box<[T]> {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl StatelessLen for String {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl StatelessLen for str {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl<T> StatelessLen for [T] {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl<const N: usize, T> StatelessLen for VInline<N, T> {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl<const N: usize> StatelessLen for AStr<N> {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}

impl<const N: usize, T> StatelessLen for UArray<N, T> {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}
