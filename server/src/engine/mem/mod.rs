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
#[cfg(test)]
mod tests;
mod uarray;
mod vinline;

pub use astr::AStr;
pub use uarray::UArray;
pub use vinline::VInline;

/// Native double pointer width (note, native != arch native, but host native)
pub struct NativeDword([usize; 2]);
/// Native triple pointer width (note, native != arch native, but host native)
pub struct NativeTword([usize; 3]);
/// Native quad pointer width (note, native != arch native, but host native)
pub struct NativeQword([usize; 4]);

/// Native quad pointer stack (must also be usable as a double and triple pointer stack. see [`SystemTword`] and [`SystemDword`])
pub trait SystemQword: SystemTword {
    fn store_full(a: usize, b: usize, c: usize, d: usize) -> Self;
    fn load_full(&self) -> [usize; 4];
}

/// Native tripe pointer stack (must also be usable as a double pointer stack, see [`SystemDword`])
pub trait SystemTword: SystemDword {
    fn store_full(a: usize, b: usize, c: usize) -> Self;
    fn load_full(&self) -> [usize; 3];
}

/// Native double pointer stack
pub trait SystemDword {
    fn store_qw(u: u64) -> Self;
    fn store_fat(a: usize, b: usize) -> Self;
    fn load_qw(&self) -> u64;
    fn load_fat(&self) -> [usize; 2];
}

impl SystemDword for NativeDword {
    #[inline(always)]
    fn store_qw(u: u64) -> Self {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            x = unsafe { core::mem::transmute(u) };
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = [u as usize, 0]
        }
        Self(x)
    }
    #[inline(always)]
    fn store_fat(a: usize, b: usize) -> Self {
        Self([a, b])
    }
    #[inline(always)]
    fn load_qw(&self) -> u64 {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            x = unsafe { core::mem::transmute_copy(self) }
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = self.0[0] as _;
        }
        x
    }
    #[inline(always)]
    fn load_fat(&self) -> [usize; 2] {
        self.0
    }
}

impl SystemTword for NativeTword {
    #[inline(always)]
    fn store_full(a: usize, b: usize, c: usize) -> Self {
        Self([a, b, c])
    }
    #[inline(always)]
    fn load_full(&self) -> [usize; 3] {
        self.0
    }
}

impl SystemDword for NativeTword {
    #[inline(always)]
    fn store_qw(u: u64) -> Self {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            let [a, b]: [usize; 2] = unsafe { core::mem::transmute(u) };
            x = [a, b, 0];
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = [u as _, 0, 0];
        }
        Self(x)
    }
    #[inline(always)]
    fn store_fat(a: usize, b: usize) -> Self {
        Self([a, b, 0])
    }
    #[inline(always)]
    fn load_qw(&self) -> u64 {
        let x;
        #[cfg(target_pointer_width = "32")]
        {
            let ab = [self.0[0], self.0[1]];
            x = unsafe { core::mem::transmute(ab) };
        }
        #[cfg(target_pointer_width = "64")]
        {
            x = self.0[0] as _;
        }
        x
    }
    #[inline(always)]
    fn load_fat(&self) -> [usize; 2] {
        [self.0[0], self.0[1]]
    }
}

impl SystemQword for NativeQword {
    fn store_full(a: usize, b: usize, c: usize, d: usize) -> Self {
        Self([a, b, c, d])
    }
    fn load_full(&self) -> [usize; 4] {
        self.0
    }
}

impl SystemTword for NativeQword {
    fn store_full(a: usize, b: usize, c: usize) -> Self {
        Self([a, b, c, 0])
    }
    fn load_full(&self) -> [usize; 3] {
        [self.0[0], self.0[1], self.0[2]]
    }
}

impl SystemDword for NativeQword {
    fn store_qw(u: u64) -> Self {
        let ret;
        #[cfg(target_pointer_width = "32")]
        {
            let [a, b]: [usize; 2] = unsafe { core::mem::transmute(u) };
            ret = <Self as SystemQword>::store_full(a, b, 0, 0);
        }
        #[cfg(target_pointer_width = "64")]
        {
            ret = <Self as SystemQword>::store_full(u as _, 0, 0, 0);
        }
        ret
    }
    fn store_fat(a: usize, b: usize) -> Self {
        <Self as SystemQword>::store_full(a, b, 0, 0)
    }
    fn load_qw(&self) -> u64 {
        let ret;
        #[cfg(target_pointer_width = "32")]
        {
            ret = unsafe { core::mem::transmute([self.0[0], self.0[1]]) };
        }
        #[cfg(target_pointer_width = "64")]
        {
            ret = self.0[0] as _;
        }
        ret
    }
    fn load_fat(&self) -> [usize; 2] {
        [self.0[0], self.0[1]]
    }
}
