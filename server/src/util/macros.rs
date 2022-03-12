/*
 * Created on Sat Jan 29 2022
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

#[macro_export]
macro_rules! impossible {
    () => {
        core::hint::unreachable_unchecked()
    };
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
    ($block:block) => {
        #[cfg(test)]
        $block
    };
    ($($item:item)*) => {
        $(#[cfg(test)] $item)*
    };
}

#[macro_export]
/// Compare two vectors irrespective of their elements' position
macro_rules! veceq {
    ($v1:expr, $v2:expr) => {
        $v1.len() == $v2.len() && $v1.iter().all(|v| $v2.contains(v))
    };
}

#[macro_export]
macro_rules! assert_veceq {
    ($v1:expr, $v2:expr) => {
        assert!(veceq!($v1, $v2))
    };
}

#[macro_export]
macro_rules! hmeq {
    ($h1:expr, $h2:expr) => {
        $h1.len() == $h2.len() && $h1.iter().all(|(k, v)| $h2.get(k).unwrap().eq(v))
    };
}

#[macro_export]
macro_rules! assert_hmeq {
    ($h1:expr, $h2: expr) => {
        assert!(hmeq!($h1, $h2))
    };
}

#[macro_export]
/// ## The action macro
///
/// A macro for adding all the _fuss_ to an action. Implementing actions should be simple
/// and should not require us to repeatedly specify generic paramters and/or trait bounds.
/// This is exactly what this macro does: does all the _magic_ behind the scenes for you,
/// including adding generic parameters, handling docs (if any), adding the correct
/// trait bounds and finally making your function async. Rest knowing that all your
/// action requirements have been happily addressed with this macro and that you don't have
/// to write a lot of code to do the exact same thing
///
///
/// ## Limitations
///
/// This macro can only handle mutable parameters for a fixed number of arguments (three)
///
macro_rules! action {
    (
        $($(#[$attr:meta])*
        fn $fname:ident($($argname:ident: $argty:ty),*)
        $block:block)*
    ) => {
            $($(#[$attr])*
            pub async fn $fname<'a, T: 'a + ClientConnection<Strm>, Strm:Stream>(
                $($argname: $argty,)*
            ) -> crate::actions::ActionResult<()>
            $block)*
    };
    (
        $($(#[$attr:meta])*
        fn $fname:ident($argone:ident: $argonety:ty,
            $argtwo:ident: $argtwoty:ty,
            mut $argthree:ident: $argthreety:ty)
        $block:block)*
    ) => {
            $($(#[$attr])*
            pub async fn $fname<'a, T: 'a + ClientConnection<Strm>, Strm:Stream>(
                $argone: $argonety,
                $argtwo: $argtwoty,
                mut $argthree: $argthreety
            ) -> crate::actions::ActionResult<()>
            $block)*
    };
}

#[macro_export]
macro_rules! byt {
    ($f:expr) => {
        bytes::Bytes::from($f)
    };
}
#[macro_export]
macro_rules! bi {
    ($($x:expr),+ $(,)?) => {{
        vec![$(bytes::Bytes::from($x),)*].into_iter()
    }};
}

#[macro_export]
macro_rules! do_sleep {
    ($dur:literal s) => {{
        std::thread::sleep(std::time::Duration::from_secs($dur));
    }};
}

/// This macro makes the first `if` expression cold (and its corresponding block) while
/// making the else expression hot
macro_rules! if_cold {
    (
        if ($coldexpr:expr) $coldblock:block
        else $hotblock:block
    ) => {
        if $crate::util::compiler::unlikely($coldexpr) {
            $crate::util::compiler::cold_err($coldblock)
        } else {
            $crate::util::compiler::hot($hotblock)
        }
    };
}

#[cfg(test)]
macro_rules! tmut_bool {
    ($e:expr) => {{
        *(&$e as *const _ as *const bool)
    }};
    ($a:expr, $b:expr) => {
        (tmut_bool!($a), tmut_bool!($b))
    };
}

macro_rules! ucidx {
    ($base:expr, $idx:expr) => {
        *($base.as_ptr().add($idx as usize))
    };
}

/// If you provide: [T; N] with M initialized elements, then you are given
/// [MaybeUninit<T>; N] with M initialized elements and N-M uninit elements
macro_rules! uninit_array {
    ($($vis:vis const $id:ident: [$ty:ty; $len:expr] = [$($init_element:expr),*];)*) => {
        $($vis const $id: [::core::mem::MaybeUninit<$ty>; $len] = {
            let mut ret = [::core::mem::MaybeUninit::uninit(); $len];
            let mut idx = 0;
            $(
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new($init_element);
            )*
            ret
        };)*
    };
}

#[macro_export]
macro_rules! def {
    (
        $(#[$attr:meta])*
        $vis:vis struct $ident:ident {
            $(
                $(#[$fattr:meta])*
                $field:ident: $ty:ty = $defexpr:expr
            ),* $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis struct $ident {
            $(
                $(#[$fattr])*
                $field: $ty,
            )*
        }
        impl ::core::default::Default for $ident {
            fn default() -> Self {
                Self {
                    $(
                        $field: $defexpr,
                    )*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! bench {
    ($vis:vis mod $modname:ident;) => {
        #[cfg(all(feature = "nightly", test))]
        $vis mod $modname;
    };
}
