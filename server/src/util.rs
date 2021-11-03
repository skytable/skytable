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

#[macro_export]
macro_rules! impossible {
    () => {
        core::hint::unreachable_unchecked()
    };
}

unsafe impl<T, E> Unwrappable<T> for Result<T, E> {
    unsafe fn unsafe_unwrap(self) -> T {
        match self {
            Ok(t) => t,
            Err(_) => impossible!(),
        }
    }
}

unsafe impl<T> Unwrappable<T> for Option<T> {
    unsafe fn unsafe_unwrap(self) -> T {
        match self {
            Some(t) => t,
            None => impossible!(),
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
            pub async fn $fname<'a, T: 'a + Send + Sync, Strm>($($argname: $argty,)*) -> std::io::Result<()>
            where
                T: ProtocolConnectionExt<Strm>,
                Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
                $block)*
    };
    (
        $($(#[$attr:meta])*
        fn $fname:ident($argone:ident: $argonety:ty, $argtwo:ident: $argtwoty:ty, mut $argthree:ident: $argthreety:ty)
        $block:block)*
    ) => {
            $($(#[$attr])*
            pub async fn $fname<'a, T: 'a + Send + Sync, Strm>($argone: $argonety, $argtwo: $argtwoty, mut $argthree: $argthreety) -> std::io::Result<()>
            where
                T: ProtocolConnectionExt<Strm>,
                Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
                $block)*
    };
}

#[allow(unused_macros)] // TODO(@ohsayan): Remove this if we don't need it anymore
macro_rules! afn_action {
    (
        $($(#[$attr:meta])*
        fn $fname:ident($($argname:ident: $argty:ty),*)
        $block:block)*
    ) => {
        $(
            $(#[$attr])*
            fn $fname<'a, T: 'a, Strm: 'a>($($argname: $argty,)*) ->
            core::pin::Pin<std::boxed::Box<dyn core::future::Future<Output = std::io::Result<()>> + Send + Sync + 'a>>
            where
                T: ProtocolConnectionExt<Strm> + Send + Sync,
                Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync
            {
                std::boxed::Box::pin(async move {$block})
            }
        )*
    };
}

pub mod compiler {
    //! Dark compiler arts and hackery to defy the normal. Use at your own
    //! risk

    use core::mem;

    #[cold]
    #[inline(never)]
    pub const fn cold() {}

    pub const fn likely(b: bool) -> bool {
        if !b {
            cold()
        }
        b
    }

    pub const fn unlikely(b: bool) -> bool {
        if b {
            cold()
        }
        b
    }

    #[cold]
    #[inline(never)]
    pub const fn cold_err<T>(v: T) -> T {
        v
    }
    #[inline(always)]
    pub const fn hot<T>(v: T) -> T {
        if false {
            cold()
        }
        v
    }

    pub unsafe fn extend_lifetime<'a, 'b, T>(inp: &'a T) -> &'b T {
        mem::transmute(inp)
    }
    pub unsafe fn extend_lifetime_mut<'a, 'b, T>(inp: &'a mut T) -> &'b mut T {
        mem::transmute(inp)
    }
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
