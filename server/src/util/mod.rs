/*
 * Created on Fri Jun 25 2021
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

#[macro_use]
mod macros;
pub mod compiler;
pub mod os;
#[cfg(test)]
pub mod test_utils;
use {
    core::{
        fmt::{self, Debug},
        mem::{self, MaybeUninit},
    },
    std::process,
};

pub const IS_ON_CI: bool = option_env!("CI").is_some();

const EXITCODE_ONE: i32 = 0x01;

pub fn exit_error() -> ! {
    process::exit(EXITCODE_ONE)
}

/// [`MaybeInit`] is a structure that is like an [`Option`] in debug mode and like
/// [`MaybeUninit`] in release mode. This means that provided there are good enough test cases, most
/// incorrect `assume_init` calls should be detected in the test phase.
#[cfg_attr(not(test), repr(transparent))]
pub struct MaybeInit<T> {
    #[cfg(test)]
    is_init: bool,
    #[cfg(not(test))]
    is_init: (),
    base: MaybeUninit<T>,
}

impl<T> MaybeInit<T> {
    /// Initialize a new uninitialized variant
    #[inline(always)]
    pub const fn uninit() -> Self {
        Self {
            #[cfg(test)]
            is_init: false,
            #[cfg(not(test))]
            is_init: (),
            base: MaybeUninit::uninit(),
        }
    }
    /// Initialize with a value
    #[inline(always)]
    pub const fn new(val: T) -> Self {
        Self {
            #[cfg(test)]
            is_init: true,
            #[cfg(not(test))]
            is_init: (),
            base: MaybeUninit::new(val),
        }
    }
    const fn ensure_init(#[cfg(test)] is_init: bool, #[cfg(not(test))] is_init: ()) {
        #[cfg(test)]
        {
            if !is_init {
                panic!("Tried to `assume_init` on uninitialized data");
            }
        }
        let _ = is_init;
    }
    /// Assume that `self` is initialized and return the inner value
    ///
    /// ## Safety
    ///
    /// Caller needs to ensure that the data is actually initialized
    #[inline(always)]
    pub const unsafe fn assume_init(self) -> T {
        Self::ensure_init(self.is_init);
        self.base.assume_init()
    }
    /// Assume that `self` is initialized and return a reference
    ///
    /// ## Safety
    ///
    /// Caller needs to ensure that the data is actually initialized
    #[inline(always)]
    pub const unsafe fn assume_init_ref(&self) -> &T {
        Self::ensure_init(self.is_init);
        self.base.assume_init_ref()
    }
    /// Assumes `self` is initialized, replaces `self` with an uninit state, returning
    /// the older value
    ///
    /// ## Safety
    pub unsafe fn take(&mut self) -> T {
        Self::ensure_init(self.is_init);
        let mut r = MaybeUninit::uninit();
        mem::swap(&mut r, &mut self.base);
        #[cfg(test)]
        {
            self.is_init = false;
        }
        r.assume_init()
    }
}

#[cfg(test)]
impl<T: fmt::Debug> fmt::Debug for MaybeInit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dat_fmt = if self.is_init {
            unsafe { format!("{:?}", self.base.assume_init_ref()) }
        } else {
            "MaybeUninit {..}".to_string()
        };
        f.debug_struct("MaybeInit")
            .field("is_init", &self.is_init)
            .field("base", &dat_fmt)
            .finish()
    }
}

#[cfg(not(test))]
impl<T: fmt::Debug> fmt::Debug for MaybeInit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaybeInit")
            .field("base", &self.base)
            .finish()
    }
}

pub const fn copy_slice_to_array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    assert!(bytes.len() <= N);
    let mut data = [0u8; N];
    let mut i = 0;
    while i < bytes.len() {
        data[i] = bytes[i];
        i += 1;
    }
    data
}

/// Copy the elements of a into b, beginning the copy at `pos`
pub const fn copy_a_into_b<const M: usize, const N: usize>(
    from: [u8; M],
    mut to: [u8; N],
    mut pos: usize,
) -> [u8; N] {
    assert!(M <= N);
    assert!(pos < N);
    let mut i = 0;
    while i < M {
        to[pos] = from[i];
        i += 1;
        pos += 1;
    }
    to
}

pub trait EndianQW {
    fn u64_bytes_le(&self) -> [u8; 8];
    fn u64_bytes_be(&self) -> [u8; 8];
    fn u64_bytes_ne(&self) -> [u8; 8] {
        if cfg!(target_endian = "big") {
            self.u64_bytes_be()
        } else {
            self.u64_bytes_le()
        }
    }
}

pub trait EndianDW {
    fn u32_bytes_le(&self) -> [u8; 8];
    fn u32_bytes_be(&self) -> [u8; 8];
    fn u32_bytes_ne(&self) -> [u8; 8] {
        if cfg!(target_endian = "big") {
            self.u32_bytes_be()
        } else {
            self.u32_bytes_le()
        }
    }
}

macro_rules! impl_endian {
    ($($ty:ty),*) => {
        $(impl EndianQW for $ty {
            fn u64_bytes_le(&self) -> [u8; 8] { (*self as u64).to_le_bytes() }
            fn u64_bytes_be(&self) -> [u8; 8] { (*self as u64).to_le_bytes() }
        })*
    }
}

impl_endian!(u8, i8, u16, i16, u32, i32, u64, i64, usize, isize);

#[derive(Debug, PartialEq)]
pub struct ModifyGuard<T> {
    val: T,
    modified: bool,
}

impl<T> ModifyGuard<T> {
    pub const fn new(val: T) -> Self {
        Self {
            val,
            modified: false,
        }
    }
    pub fn into_val(self) -> T {
        self.val
    }
}

impl<T> core::ops::Deref for ModifyGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl<T> core::ops::DerefMut for ModifyGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.modified = true;
        &mut self.val
    }
}
