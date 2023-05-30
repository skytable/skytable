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
    () => {{
        if cfg!(debug_assertions) {
            panic!(
                "reached unreachable case at: {}:{}",
                ::core::file!(),
                ::core::line!()
            );
        } else {
            ::core::hint::unreachable_unchecked()
        }
    }};
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
macro_rules! veceq_transposed {
    ($v1:expr, $v2:expr) => {
        $v1.len() == $v2.len() && $v1.iter().all(|v| $v2.contains(v))
    };
}

#[macro_export]
macro_rules! assert_veceq_transposed {
    ($v1:expr, $v2:expr) => {{
        if !veceq_transposed!($v1, $v2) {
            panic!(
                "failed to assert transposed veceq. v1: `{:#?}`, v2: `{:#?}`",
                $v1, $v2
            )
        }
    }};
}

#[cfg(test)]
macro_rules! vecstreq_exact {
    ($v1:expr, $v2:expr) => {
        $v1.iter()
            .zip($v2.iter())
            .all(|(a, b)| a.as_bytes() == b.as_bytes())
    };
}

#[cfg(test)]
macro_rules! assert_vecstreq_exact {
    ($v1:expr, $v2:expr) => {
        if !vecstreq_exact!($v1, $v2) {
            ::core::panic!(
                "failed to assert vector data equality. lhs: {:?}, rhs: {:?}",
                $v1,
                $v2
            );
        }
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
        fn $fname:ident($($argname:ident: $argty:ty),* $(,)?)
        $block:block)*
    ) => {
            $($(#[$attr])*
            pub async fn $fname<
                'a,
                C: 'a + $crate::dbnet::BufferedSocketStream,
                P: $crate::protocol::interface::ProtocolSpec,
            > (
                $($argname: $argty,)*
            ) -> $crate::actions::ActionResult<()>
            $block)*
    };
    (
        $($(#[$attr:meta])*
        fn $fname:ident(
            $argone:ident: $argonety:ty,
            $argtwo:ident: $argtwoty:ty,
            mut $argthree:ident: $argthreety:ty $(,)?
        ) $block:block)*
    ) => {
            $($(#[$attr])*
            pub async fn $fname<
            'a,
                C: 'a + $crate::dbnet::BufferedSocketStream,
                P: $crate::protocol::interface::ProtocolSpec,
            >(
                $argone: $argonety,
                $argtwo: $argtwoty,
                mut $argthree: $argthreety
            ) -> $crate::actions::ActionResult<()>
            $block)*
    };
    (
        $($(#[$attr:meta])*
        fn $fname:ident(
            $argone:ident: $argonety:ty,
            $argtwo:ident: $argtwoty:ty,
            $argthree:ident: $argthreety:ty
        ) $block:block)*
    ) => {
            $($(#[$attr])*
            pub async fn $fname<
                'a,
                T: 'a + $crate::dbnet::connection::ClientConnection<P, Strm>,
                Strm: $crate::dbnet::connection::Stream,
                P: $crate::protocol::interface::ProtocolSpec
            >(
                $argone: $argonety,
                $argtwo: $argtwoty,
                $argthree: $argthreety
            ) -> $crate::actions::ActionResult<()>
            $block)*
    };
}

#[macro_export]
macro_rules! byt {
    ($f:expr) => {
        $crate::corestore::rc::SharedSlice::from($f)
    };
}
#[macro_export]
macro_rules! bi {
    ($($x:expr),+ $(,)?) => {{
        vec![$($crate::corestore::rc::SharedSlice::from($x),)*].into_iter()
    }};
}

#[macro_export]
macro_rules! do_sleep {
    ($dur:literal s) => {{
        std::thread::sleep(std::time::Duration::from_secs($dur));
    }};
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

#[macro_export]
macro_rules! is_64b {
    () => {
        cfg!(target_pointer_width = "64")
    };
}

#[macro_export]
macro_rules! concat_array_to_array {
    ($a:expr, $b:expr) => {{
        const BUFFER_A: [u8; $a.len()] = crate::util::copy_slice_to_array($a);
        const BUFFER_B: [u8; $b.len()] = crate::util::copy_slice_to_array($b);
        const BUFFER: [u8; BUFFER_A.len() + BUFFER_B.len()] = unsafe {
            // UNSAFE(@ohsayan): safe because align = 1
            core::mem::transmute((BUFFER_A, BUFFER_B))
        };
        BUFFER
    }};
    ($a:expr, $b:expr, $c:expr) => {{
        const LA: usize = $a.len() + $b.len();
        const LB: usize = LA + $c.len();
        const S_1: [u8; LA] = concat_array_to_array!($a, $b);
        const S_2: [u8; LB] = concat_array_to_array!(&S_1, $c);
        S_2
    }};
}

#[macro_export]
macro_rules! concat_str_to_array {
    ($a:expr, $b:expr) => {
        concat_array_to_array!($a.as_bytes(), $b.as_bytes())
    };
    ($a:expr, $b:expr, $c:expr) => {{
        concat_array_to_array!($a.as_bytes(), $b.as_bytes(), $c.as_bytes())
    }};
}

#[macro_export]
macro_rules! concat_str_to_str {
    ($a:expr, $b:expr) => {{
        const BUFFER: [u8; ::core::primitive::str::len($a) + ::core::primitive::str::len($b)] =
            concat_str_to_array!($a, $b);
        const STATIC_BUFFER: &[u8] = &BUFFER;
        unsafe {
            // UNSAFE(@ohsayan): all good because of restriction to str
            core::str::from_utf8_unchecked(&STATIC_BUFFER)
        }
    }};
    ($a:expr, $b:expr, $c:expr) => {{
        const A: &str = concat_str_to_str!($a, $b);
        concat_str_to_str!(A, $c)
    }};
}
