/*
 * Created on Wed Mar 01 2023
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

use {
    super::{NativeDword, NativeQword, NativeTword, SpecialPaddedWord},
    core::mem::size_of,
};

pub static ZERO_BLOCK: [u8; 0] = [];

#[cfg(target_pointer_width = "32")]
fn quadsplit(q: u64) -> [usize; 2] {
    unsafe {
        // UNSAFE(@ohsayan): simple numeric ops
        core::mem::transmute(q)
    }
}
#[cfg(target_pointer_width = "32")]
fn quadmerge(v: [usize; 2]) -> u64 {
    unsafe {
        // UNSAFE(@ohsayan): simple numeric ops
        core::mem::transmute(v)
    }
}

pub trait WordIO<T> {
    fn store(v: T) -> Self;
    fn load(&self) -> T;
}

/*
    dword
    ---
    kinds: NN (word * 2), QN (qword, word)
    promotions: QN -> NNN
*/

pub trait DwordNN: Sized {
    const DWORDNN_FROM_UPPER: bool = size_of::<Self>() > size_of::<[usize; 2]>();
    fn dwordnn_store_native_full(a: usize, b: usize) -> Self;
    fn dwordnn_store_qw(a: u64) -> Self {
        debug_assert!(!Self::DWORDNN_FROM_UPPER, "NEED TO OVERRIDE STORE");
        #[cfg(target_pointer_width = "32")]
        {
            let [a, b] = quadsplit(a);
            Self::dwordnn_store_native_full(a, b)
        }
        #[cfg(target_pointer_width = "64")]
        {
            Self::dwordnn_store_native_full(a as usize, 0)
        }
    }
    fn dwordnn_load_native_full(&self) -> [usize; 2];
    fn dwordnn_load_qw(&self) -> u64 {
        debug_assert!(!Self::DWORDNN_FROM_UPPER, "NEED TO OVERRIDE LOAD");
        #[cfg(target_pointer_width = "32")]
        {
            quadmerge(self.dwordnn_load_native_full())
        }
        #[cfg(target_pointer_width = "64")]
        {
            self.dwordnn_load_native_full()[0] as u64
        }
    }
}

pub trait DwordQN: Sized {
    const DWORDQN_FROM_UPPER: bool = size_of::<Self>() > size_of::<(u64, usize)>();
    fn dwordqn_store_qw_nw(a: u64, b: usize) -> Self;
    fn dwordqn_load_qw_nw(&self) -> (u64, usize);
    // overrides
    fn overridable_dwordnn_store_qw(a: u64) -> Self {
        Self::dwordqn_store_qw_nw(a, 0)
    }
    // promotions
    fn dwordqn_promote<W: DwordQN>(&self) -> W {
        let (a, b) = self.dwordqn_load_qw_nw();
        <W as DwordQN>::dwordqn_store_qw_nw(a, b)
    }
}

/*
    dword: blanket impls
*/

impl<T: DwordQN> DwordNN for T {
    fn dwordnn_store_native_full(a: usize, b: usize) -> Self {
        Self::dwordqn_store_qw_nw(a as u64, b)
    }
    fn dwordnn_store_qw(a: u64) -> Self {
        Self::overridable_dwordnn_store_qw(a)
    }
    fn dwordnn_load_native_full(&self) -> [usize; 2] {
        let (a, b) = self.dwordqn_load_qw_nw();
        debug_assert!(a <= usize::MAX as u64, "overflowed with: `{}`", a);
        [a as usize, b]
    }
    fn dwordnn_load_qw(&self) -> u64 {
        DwordQN::dwordqn_load_qw_nw(self).0
    }
}

/*
    dword: impls
*/

impl DwordNN for NativeDword {
    fn dwordnn_store_native_full(a: usize, b: usize) -> Self {
        Self([a, b])
    }
    fn dwordnn_load_native_full(&self) -> [usize; 2] {
        self.0
    }
}

impl DwordQN for SpecialPaddedWord {
    fn dwordqn_store_qw_nw(a: u64, b: usize) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): valid construction
            Self::new(a, b)
        }
    }
    fn dwordqn_load_qw_nw(&self) -> (u64, usize) {
        (self.a, self.b)
    }
    // overrides
    fn overridable_dwordnn_store_qw(a: u64) -> Self {
        unsafe {
            // UNSAFE(@ohsayan): valid construction
            Self::new(a, ZERO_BLOCK.as_ptr() as usize)
        }
    }
}

/*
    tword
    ---
    kinds: NNN (word * 3)
    promotions: NNN -> NNNN
*/

pub trait TwordNNN: Sized {
    const TWORDNNN_FROM_UPPER: bool = size_of::<Self>() > size_of::<[usize; 3]>();
    fn twordnnn_store_native_full(a: usize, b: usize, c: usize) -> Self;
    fn twordnnn_load_native_full(&self) -> [usize; 3];
    // promotions
    fn tword_promote<W: TwordNNN>(&self) -> W {
        let [a, b, c] = self.twordnnn_load_native_full();
        <W as TwordNNN>::twordnnn_store_native_full(a, b, c)
    }
}

/*
    tword: blanket impls
*/

impl<T: TwordNNN> DwordQN for T {
    fn dwordqn_store_qw_nw(a: u64, b: usize) -> Self {
        #[cfg(target_pointer_width = "32")]
        {
            let [qw_1, qw_2] = quadsplit(a);
            Self::twordnnn_store_native_full(qw_1, qw_2, b)
        }
        #[cfg(target_pointer_width = "64")]
        {
            Self::twordnnn_store_native_full(a as usize, b, 0)
        }
    }
    fn dwordqn_load_qw_nw(&self) -> (u64, usize) {
        #[cfg(target_pointer_width = "32")]
        {
            let [w1, w2, b] = self.twordnnn_load_native_full();
            (quadmerge([w1, w2]), b)
        }
        #[cfg(target_pointer_width = "64")]
        {
            let [a, b, _] = self.twordnnn_load_native_full();
            (a as u64, b)
        }
    }
}

/*
    tword: impls
*/

impl TwordNNN for NativeTword {
    fn twordnnn_store_native_full(a: usize, b: usize, c: usize) -> Self {
        Self([a, b, c])
    }
    fn twordnnn_load_native_full(&self) -> [usize; 3] {
        self.0
    }
}

/*
    qword
    ---
    kinds: NNNN (word * 4)
    promotions: N/A
*/

pub trait QwordNNNN: Sized {
    const QWORDNNNN_FROM_UPPER: bool = size_of::<Self>() > size_of::<[usize; 4]>();
    fn qwordnnnn_store_native_full(a: usize, b: usize, c: usize, d: usize) -> Self;
    fn qwordnnnn_store_qw_qw(a: u64, b: u64) -> Self {
        #[cfg(target_pointer_width = "32")]
        {
            let [qw1_a, qw1_b] = quadsplit(a);
            let [qw2_a, qw2_b] = quadsplit(b);
            Self::qwordnnnn_store_native_full(qw1_a, qw1_b, qw2_a, qw2_b)
        }
        #[cfg(target_pointer_width = "64")]
        {
            Self::qwordnnnn_store_native_full(a as usize, b as usize, 0, 0)
        }
    }
    fn qwordnnnn_store_qw_nw_nw(a: u64, b: usize, c: usize) -> Self {
        #[cfg(target_pointer_width = "32")]
        {
            let [qw_a, qw_b] = quadsplit(a);
            Self::qwordnnnn_store_native_full(qw_a, qw_b, b, c)
        }
        #[cfg(target_pointer_width = "64")]
        {
            Self::qwordnnnn_store_native_full(a as usize, b, c, 0)
        }
    }
    fn qwordnnnn_load_native_full(&self) -> [usize; 4];
    fn qwordnnnn_load_qw_qw(&self) -> [u64; 2] {
        let [a, b, c, d] = self.qwordnnnn_load_native_full();
        #[cfg(target_pointer_width = "32")]
        {
            [quadmerge([a, b]), quadmerge([c, d])]
        }
        #[cfg(target_pointer_width = "64")]
        {
            let _ = (c, d);
            [a as u64, b as u64]
        }
    }
    fn qwordnnnn_load_qw_nw_nw(&self) -> (u64, usize, usize) {
        let [a, b, c, d] = self.qwordnnnn_load_native_full();
        #[cfg(target_pointer_width = "32")]
        {
            (quadmerge([a, b]), c, d)
        }
        #[cfg(target_pointer_width = "64")]
        {
            let _ = d;
            (a as u64, b, c)
        }
    }
}

/*
    qword: blanket impls
*/

impl<T: QwordNNNN> TwordNNN for T {
    fn twordnnn_store_native_full(a: usize, b: usize, c: usize) -> Self {
        Self::qwordnnnn_store_native_full(a, b, c, 0)
    }
    fn twordnnn_load_native_full(&self) -> [usize; 3] {
        let [a, b, c, _] = self.qwordnnnn_load_native_full();
        [a, b, c]
    }
}

/*
    qword: impls
*/

impl QwordNNNN for NativeQword {
    fn qwordnnnn_store_native_full(a: usize, b: usize, c: usize, d: usize) -> Self {
        Self([a, b, c, d])
    }
    fn qwordnnnn_load_native_full(&self) -> [usize; 4] {
        self.0
    }
}

/*
    impls: WordIO
*/

macro_rules! impl_numeric_io {
    ($trait:ident => { $($ty:ty),* $(,)? }) => {
        $(impl<T: $trait> WordIO<$ty> for T {
            fn store(v: $ty) -> Self { Self::dwordnn_store_qw(v as _) }
            fn load(&self) -> $ty { self.dwordnn_load_qw() as _ }
        })*
    }
}

impl_numeric_io!(DwordNN => { u8, u16, u32, u64, i8, i16, i32, i64 });

impl<T: DwordNN> WordIO<bool> for T {
    fn store(v: bool) -> Self {
        Self::dwordnn_store_qw(v as _)
    }
    fn load(&self) -> bool {
        self.dwordnn_load_qw() == 1
    }
}

macro_rules! impl_float_io {
    ($($float:ty),* $(,)?) => {
        $(impl<T: DwordNN> WordIO<$float> for T {
            fn store(v: $float) -> Self { Self::dwordnn_store_qw(v.to_bits() as u64) }
            fn load(&self) -> $float { <$float>::from_bits(self.dwordnn_load_qw() as _) }
        })*
    }
}

impl_float_io!(f32, f64);

impl<T: DwordNN> WordIO<(usize, usize)> for T {
    fn store((a, b): (usize, usize)) -> Self {
        Self::dwordnn_store_native_full(a, b)
    }
    fn load(&self) -> (usize, usize) {
        let [a, b] = self.dwordnn_load_native_full();
        (a, b)
    }
}

impl<T: DwordNN> WordIO<[usize; 2]> for T {
    fn store([a, b]: [usize; 2]) -> Self {
        Self::dwordnn_store_native_full(a, b)
    }
    fn load(&self) -> [usize; 2] {
        self.dwordnn_load_native_full()
    }
}

impl<T: DwordNN> WordIO<(usize, *mut u8)> for T {
    fn store((a, b): (usize, *mut u8)) -> Self {
        Self::dwordnn_store_native_full(a, b as usize)
    }
    fn load(&self) -> (usize, *mut u8) {
        let [a, b] = self.dwordnn_load_native_full();
        (a, b as *mut u8)
    }
}

impl<T: DwordNN> WordIO<(usize, *const u8)> for T {
    fn store((a, b): (usize, *const u8)) -> Self {
        Self::dwordnn_store_native_full(a, b as usize)
    }
    fn load(&self) -> (usize, *const u8) {
        let [a, b] = self.dwordnn_load_native_full();
        (a, b as *const u8)
    }
}
