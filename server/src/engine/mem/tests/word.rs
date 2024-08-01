/*
 * Created on Mon Apr 24 2023
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
    crate::engine::mem::{
        word::{DwordQN, QwordNNNN, TwordNNN, WordIO, ZERO_BLOCK},
        NativeDword, NativeQword, NativeTword, SpecialPaddedWord,
    },
    core::{slice, str},
};

fn wordld<T: PartialEq, W: WordIO<T>>(w: &W, x: T) -> (T, T) {
    (w.load(), x)
}

macro_rules! assert_wordeq {
    ($a:expr, $b:expr) => {{
        let (a, b) = wordld(&$a, $b);
        assert_eq!(a, b);
    }};
}

macro_rules! assert_wordeq_minmax {
    ($word:ty => $($ty:ty),* $(,)?; with $extramin:ident, $extramax:ident) => {{
        $(
            let x = <$word>::store(<$ty>::MIN); assert_wordeq!(x, <$ty>::MIN);
            $extramin(&x);
            let x = <$word>::store(<$ty>::MAX); assert_wordeq!(x, <$ty>::MAX);
            $extramax(&x);
        )*
    }};
}

fn check_primitives<W>(extramin: impl Fn(&W), extramax: impl Fn(&W))
where
    W: WordIO<bool>
        + WordIO<u8>
        + WordIO<u16>
        + WordIO<u32>
        + WordIO<u64>
        + WordIO<i8>
        + WordIO<i16>
        + WordIO<i32>
        + WordIO<i64>
        + WordIO<f32>
        + WordIO<f64>
        + WordIO<(usize, usize)>
        + WordIO<(usize, *mut u8)>
        + WordIO<(usize, *const u8)>,
{
    assert_wordeq_minmax!(W => u8, u16, u32, u64, i8, i16, i32, i64, f32, f64; with extramin, extramax);
    // bool
    let x = W::store(false);
    assert_wordeq!(x, false);
    extramin(&x);
    let x = W::store(true);
    assert_wordeq!(x, true);
    extramax(&x);
    // str
    let str = "hello, world";
    let x = W::store((str.len(), str.as_ptr()));
    unsafe {
        let (len, ptr) = x.load();
        assert_eq!(
            str,
            str::from_utf8_unchecked(slice::from_raw_parts(ptr, len))
        );
    }
    // string (mut)
    let mut string = String::from("hello, world");
    let x = W::store((string.len(), string.as_mut_ptr()));
    unsafe {
        let (len, ptr) = x.load();
        assert_eq!(
            string,
            str::from_utf8_unchecked(slice::from_raw_parts(ptr, len))
        );
    }
}

#[sky_macros::test]
fn dwordnn_all() {
    check_primitives::<NativeDword>(|_| {}, |_| {});
}

#[sky_macros::test]
fn dwordqn_all() {
    check_primitives::<SpecialPaddedWord>(
        |minword| {
            let (_a, b) = minword.dwordqn_load_qw_nw();
            assert_eq!(b, ZERO_BLOCK.as_ptr() as usize);
        },
        |maxword| {
            let (_a, b) = maxword.dwordqn_load_qw_nw();
            assert_eq!(b, ZERO_BLOCK.as_ptr() as usize);
        },
    );
}

#[sky_macros::test]
fn twordnnn_all() {
    check_primitives::<NativeTword>(|_| {}, |_| {});
}

#[sky_macros::test]
fn qwordnnn_all() {
    check_primitives::<NativeQword>(|_| {}, |_| {});
}

#[sky_macros::test]
fn dwordqn_promotions() {
    let x = SpecialPaddedWord::store(u64::MAX);
    let y: NativeTword = x.dwordqn_promote();
    let (uint, usize) = y.dwordqn_load_qw_nw();
    assert_eq!(uint, u64::MAX);
    assert_eq!(usize, ZERO_BLOCK.as_ptr() as usize);
    let z: NativeQword = y.tword_promote();
    let (uint, usize_1, usize_2) = z.qwordnnnn_load_qw_nw_nw();
    assert_eq!(uint, u64::MAX);
    assert_eq!(usize_1, ZERO_BLOCK.as_ptr() as usize);
    assert_eq!(usize_2, 0);
}

fn eval_special_case(x: SpecialPaddedWord, qw: u64, nw: usize) {
    let y: NativeQword = x.dwordqn_promote();
    assert_eq!(y.dwordqn_load_qw_nw(), (qw, nw));
    let z: SpecialPaddedWord = unsafe {
        let (a, b) = y.dwordqn_load_qw_nw();
        SpecialPaddedWord::new(a, b)
    };
    assert_eq!(z.dwordqn_load_qw_nw(), (qw, nw));
}

#[sky_macros::test]
fn dwordqn_special_case_ldpk() {
    let hello = "hello, world";
    eval_special_case(
        SpecialPaddedWord::store((hello.len(), hello.as_ptr())),
        hello.len() as u64,
        hello.as_ptr() as usize,
    );
    eval_special_case(
        SpecialPaddedWord::store(u64::MAX),
        u64::MAX,
        ZERO_BLOCK.as_ptr() as usize,
    );
}
