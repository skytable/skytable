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

use core::ptr;
use core::slice;

/// # Unsafe unwrapping
///
/// This trait provides a method `unsafe_unwrap` that is potentially unsafe and has
/// the ability to **violate multiple safety gurantees** that rust provides. So,
/// if you get `SIGILL`s or `SIGSEGV`s, by using this trait, blame yourself.
pub unsafe trait Unwrappable<T> {
    /// Unwrap a _nullable_ (almost) type to get its value while asserting that the value
    /// cannot ever be null
    ///
    /// ## Safety
    /// The trait is unsafe, and so is this function. You can wreck potential havoc if you
    /// use this heedlessly
    ///
    unsafe fn unsafe_unwrap(self) -> T;
}

unsafe impl<T, E> Unwrappable<T> for Result<T, E> {
    unsafe fn unsafe_unwrap(self) -> T {
        match self {
            Ok(t) => t,
            Err(_) => core::hint::unreachable_unchecked(),
        }
    }
}

unsafe impl<T> Unwrappable<T> for Option<T> {
    unsafe fn unsafe_unwrap(self) -> T {
        match self {
            Some(t) => t,
            None => core::hint::unreachable_unchecked(),
        }
    }
}

#[macro_export]
macro_rules! consts {
    ($($(#[$attr:meta])* $ident:ident : $ty:ty = $expr:expr;)*) => {
        $(
            $(#[$attr])*
            const $ident: $ty = $expr;
        )*
    };
    ($($(#[$attr:meta])* $vis:vis $ident:ident : $ty:ty = $expr:expr;)*) => {
        $(
            $(#[$attr])*
            $vis const $ident: $ty = $expr;
        )*
    };
}

#[macro_export]
macro_rules! typedef {
    ($($(#[$attr:meta])* $ident:ident = $ty:ty;)*) => {
        $($(#[$attr])* type $ident = $ty;)*
    };
    ($($(#[$attr:meta])* $vis:vis $ident:ident = $ty:ty;)*) => {
        $($(#[$attr])* $vis type $ident = $ty;)*
    };
}

#[macro_export]
macro_rules! cfg_test {
    ($($item:item)*) => {
        $(#[cfg(test)] $item)*
    };
}

/*
 32-bit integer to String parsing. This algorithm was "invented" by Ben Voigt and written in C++
 as a part of a "challenge" and was ported to Rust with some modifications by Sayan.
 NOTE: This might occassionally blow up.
*/

const PAIR_MAP_LUT: [u8; 200] = [
    0x30, 0x30, 0x30, 0x31, 0x30, 0x32, 0x30, 0x33, 0x30, 0x34, 0x30, 0x35, 0x30, 0x36, 0x30, 0x37,
    0x30, 0x38, 0x30, 0x39, // 0x30
    0x31, 0x30, 0x31, 0x31, 0x31, 0x32, 0x31, 0x33, 0x31, 0x34, 0x31, 0x35, 0x31, 0x36, 0x31, 0x37,
    0x31, 0x38, 0x31, 0x39, // 0x31
    0x32, 0x30, 0x32, 0x31, 0x32, 0x32, 0x32, 0x33, 0x32, 0x34, 0x32, 0x35, 0x32, 0x36, 0x32, 0x37,
    0x32, 0x38, 0x32, 0x39, // 0x32
    0x33, 0x30, 0x33, 0x31, 0x33, 0x32, 0x33, 0x33, 0x33, 0x34, 0x33, 0x35, 0x33, 0x36, 0x33, 0x37,
    0x33, 0x38, 0x33, 0x39, // 0x33
    0x34, 0x30, 0x34, 0x31, 0x34, 0x32, 0x34, 0x33, 0x34, 0x34, 0x34, 0x35, 0x34, 0x36, 0x34, 0x37,
    0x34, 0x38, 0x34, 0x39, // 0x34
    0x35, 0x30, 0x35, 0x31, 0x35, 0x32, 0x35, 0x33, 0x35, 0x34, 0x35, 0x35, 0x35, 0x36, 0x35, 0x37,
    0x35, 0x38, 0x35, 0x39, // 0x35
    0x36, 0x30, 0x36, 0x31, 0x36, 0x32, 0x36, 0x33, 0x36, 0x34, 0x36, 0x35, 0x36, 0x36, 0x36, 0x37,
    0x36, 0x38, 0x36, 0x39, // 0x36
    0x37, 0x30, 0x37, 0x31, 0x37, 0x32, 0x37, 0x33, 0x37, 0x34, 0x37, 0x35, 0x37, 0x36, 0x37, 0x37,
    0x37, 0x38, 0x37, 0x39, // 0x37
    0x38, 0x30, 0x38, 0x31, 0x38, 0x32, 0x38, 0x33, 0x38, 0x34, 0x38, 0x35, 0x38, 0x36, 0x38, 0x37,
    0x38, 0x38, 0x38, 0x39, // 0x38
    0x39, 0x30, 0x39, 0x31, 0x39, 0x32, 0x39, 0x33, 0x39, 0x34, 0x39, 0x35, 0x39, 0x36, 0x39, 0x37,
    0x39, 0x38, 0x39, 0x39, // 0x39
];

const BUFSIZE: usize = 10;

#[inline]
/// Convert a 32-bit unsigned integer to a String.
/// **Warning: This function will ocassionally blow up for some inputs**
pub fn it32_to_str(mut val: u32) -> String {
    unsafe {
        let mut buf: [u8; BUFSIZE] = [0u8; BUFSIZE];
        let mut it = buf.as_mut_ptr().add(BUFSIZE - 2) as *mut u8;
        let mut div = val / 100;
        while div != 0 {
            ptr::copy_nonoverlapping(
                &PAIR_MAP_LUT[(2 * (val - div * 100)) as usize] as *const u8,
                it,
                2,
            );
            val = div;
            it = it.sub(2);
            div = val / 100;
        }
        ptr::copy_nonoverlapping(&PAIR_MAP_LUT[(2 * val) as usize] as *const u8, it, 2);

        if val < 10 {
            // let y = *it;
            // *it = y + 1;
            it = it.add(1);
        }

        String::from_utf8_unchecked(
            slice::from_raw_parts(it, buf.as_ptr().add(BUFSIZE).offset_from(it) as usize)
                .to_owned(),
        )
    }
}

#[cfg(test)]
macro_rules! assert_itoa32 {
    ($e:expr) => {
        assert_eq!($e.to_string(), self::it32_to_str($e));
    };
}

#[test]
fn test_numbers() {
    // just some random funny varying length integers (except 0s) to test the function
    assert_itoa32!(1);
    assert_itoa32!(11);
    assert_itoa32!(111);
    assert_itoa32!(1111);
    assert_itoa32!(11111);
    assert_itoa32!(111111);
    assert_itoa32!(1111111);
    assert_itoa32!(11111111);
    assert_itoa32!(111111111);
    assert_itoa32!(1111111111);
    assert_itoa32!(0000000000);
    assert_itoa32!(888888888);
    assert_itoa32!(77777777);
    assert_itoa32!(6666666);
    assert_itoa32!(555555);
    assert_itoa32!(44444);
    assert_itoa32!(3333);
    assert_itoa32!(222);
    assert_itoa32!(11);
    assert_itoa32!(0);
    assert_itoa32!(9);
    assert_itoa32!(99);
    assert_itoa32!(999);
    assert_itoa32!(9999);
    assert_itoa32!(99999);
    assert_itoa32!(999999);
    assert_itoa32!(9999999);
    assert_itoa32!(99999999);
    assert_itoa32!(999999999);
    assert_itoa32!(123456789);
    assert_itoa32!(12345678);
    assert_itoa32!(1234567);
    assert_itoa32!(123456);
    assert_itoa32!(12345);
    assert_itoa32!(1234);
    assert_itoa32!(123);
    assert_itoa32!(12);
}
