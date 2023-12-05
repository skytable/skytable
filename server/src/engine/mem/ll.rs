/*
 * Created on Fri Sep 01 2023
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

use core::ops::{Deref, DerefMut};

#[derive(Debug, Clone, Copy, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    any(
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "x86_64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    any(
        target_arch = "arm",
        target_arch = "hexagon",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "sparc"
    ),
    repr(align(32))
)]
#[cfg_attr(
    not(any(
        target_arch = "aarch64",
        target_arch = "arm",
        target_arch = "hexagon",
        target_arch = "m68k",
        target_arch = "mips",
        target_arch = "mips64",
        target_arch = "powerpc64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "s390x",
        target_arch = "sparc",
        target_arch = "x86_64",
    )),
    repr(align(64))
)]
#[cfg_attr(target_arch = "m68k", repr(align(16)))]
/**
    cache line padding (to avoid unintended cache line invalidation)
    - 256-bit (on a side note, good lord):
        -> s390x: https://community.ibm.com/community/user/ibmz-and-linuxone/viewdocument/microprocessor-optimization-primer
    - 128-bit:
        -> aarch64: ARM64's big.LITTLE (it's a funny situation because there's a silly situation where one set of cores have one cache line
        size while the other ones have a different size; see this excellent article: https://www.mono-project.com/news/2016/09/12/arm64-icache/)
        -> powerpc64: https://reviews.llvm.org/D33656
        -> x86_64: Intel's Sandy Bridge+ (https://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf)
    - 64-bit: default for all non-specific targets
    - 32-bit: arm, hexagon, mips, mips64, riscv64, and sparc have 32-byte cache line size
    - 16-bit: m68k (not very useful for us, but yeah)
*/
pub struct CachePadded<T> {
    data: T,
}

impl<T> CachePadded<T> {
    pub const fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
