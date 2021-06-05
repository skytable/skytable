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

use super::imp::{BitmaskWord, BITMASK_MASK, BITMASK_STRIDE};
#[derive(Clone, Copy)]
pub struct Bitmask(pub BitmaskWord);

impl Bitmask {
    /// Returns a bitmask with all the bits inverted
    ///
    /// For example (please, _just an example_), if your input is 11 base 10, or
    /// 1011 base 2 -- then your output is 0100 base 2 or 4 base 10. So it's basically
    /// a bitwise NOT on each bit in the integer
    pub fn invert(self) -> Self {
        Self(self.0 ^ BITMASK_MASK)
    }

    /// Flips the bits of the Bitmask at the given index
    /// 
    /// This will return true if the bit at the provided index was actually set
    pub unsafe fn flip(&mut self, index: usize) -> bool {
        let mask = 1 << (index * BITMASK_STRIDE + BITMASK_STRIDE - 1);
        self.0 ^= mask;
        self.0 & mask == 0
    }

    /// Returns the lowest bit set in he bitmask
    pub fn lowest_set_bit(self) -> Option<usize> {
        if self.0 == 0 {
            // no bits have been set!
            None
        } else {
            Some(unsafe { self.lowest_set_bit_nonzero() })
        }
    }

    /// Returns the bitmask with the lowest bit removed. Pretty simple to understand:
    /// `011010` yields `011000`, i.e the lowest order bit is removed. We don't need
    /// to know the index; if we did, we could have done something like `bits ^= (1 << index)`
    pub fn remove_lowest_bit(self) -> Self {
        Bitmask(self.0 & (self.0 - 1))
    }

    /// Returns the first bit set in the bitmask, if such a bit exists
    ///
    /// So if self (let's go into the beautiful world where there's a single byte everywhere)
    /// is (00000000) this will return 8! So, hazardous if we call it when the bitmask
    /// is uninitialized. But if self is initialized, and our byte looks like: 0001_0000
    /// we'll get back 4, because the 4th bit has been set. I'm sure you've got it now!
    ///
    /// Please check that atleast a single bit has been set in the bitmask before attempting
    /// to use this!
    pub unsafe fn lowest_set_bit_nonzero(self) -> usize {
        // we can use the cttz (count trailing zeros/ unset bits) intrinsic when it is stabilized
        self.trailing_zeros()
    }

    /// Checks if any bit has been set in the bitmask
    pub fn any_bit_set(self) -> bool {
        self.0 != 0
    }

    /// Returns the number of trailing zeros in this bitmask
    ///
    /// We just use or emulate the trailing zeros instructions, that is, by either reversing
    /// the byte order of the word directly, or by emulating it by swapping bytes.
    /// So if our bitmask is something like: 0b1111_1000, then we'd get 0000_0011 (base 10)
    pub fn trailing_zeros(self) -> usize {
        if cfg!(target_arch = "arm") && BITMASK_STRIDE % 8 == 0 {
            /*
             ARM doesn't have a trailing zeros instruction and instead
             we need to use a combination of RBIT (reverse bits) and then CLZ (count
             leading zeros). However, even worse, pre-arm-v7 doesn't have RBIT (for more
             information, see this:
             https://developer.arm.com/documentation/dui0489/h/arm-and-thumb-instructions/rev--rev16--revsh--and-rbit)
             That is why we'll swap bytes (basically 0x12345678 becomes 0x87654321)
             and then get the leading zeros which effectively does the same thing
            */
            self.0.swap_bytes().leading_zeros() as usize / BITMASK_STRIDE
        } else {
            self.0.trailing_zeros() as usize / BITMASK_STRIDE
        }
    }

    /// Returns the number of leading zeros in the bitmask
    ///
    pub fn leading_zeros(self) -> usize {
        // Fortunately architectures do have the leading_zeros instruction :)
        // so we don't have to do some cfg mess
        self.0.leading_zeros() as usize / BITMASK_STRIDE
    }
}

/// An iterator over the contents of a bitmask, returning the indices
/// of the set bits
pub struct BitmaskIterator(Bitmask);

impl Iterator for BitmaskIterator {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let bit = self.0.lowest_set_bit()?;
        self.0 = self.0.remove_lowest_bit();
        Some(bit)
    }
}

impl IntoIterator for Bitmask {
    type IntoIter = BitmaskIterator;
    type Item = usize;
    fn into_iter(self) -> Self::IntoIter {
        BitmaskIterator(self)
    }
}
