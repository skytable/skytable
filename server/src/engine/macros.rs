/*
 * Created on Wed Oct 12 2022
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
 * the Free Software fation, either version 3 of the License, or
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

macro_rules! extract {
    ($src:expr, $what:pat => $ret:expr) => {
        if let $what = $src {
            $ret
        } else {
            $crate::impossible!()
        }
    };
}

#[cfg(test)]
macro_rules! extract_safe {
    ($src:expr, $what:pat => $ret:expr) => {
        if let $what = $src {
            $ret
        } else {
            panic!("expected one {}, found {:?}", stringify!($what), $src);
        }
    };
}

#[cfg(test)]
macro_rules! multi_assert_eq {
    ($($lhs:expr),* => $rhs:expr) => {
        $(assert_eq!($lhs, $rhs);)*
    };
}

macro_rules! enum_impls {
    ($for:ident<$lt:lifetime> => {$($other:ty as $me:ident),*$(,)?}) => {
        $(impl<$lt> ::core::convert::From<$other> for $for<$lt> {fn from(v: $other) -> Self {Self::$me(v.into())}})*
    };
    ($for:ty => {$($other:ty as $me:ident),*$(,)?}) => {
        $(impl ::core::convert::From<$other> for $for {fn from(v: $other) -> Self {Self::$me(v.into())}})*
    };
}

#[allow(unused_macros)]
macro_rules! assertions {
    ($($assert:expr),*$(,)?) => {$(const _:()=::core::assert!($assert);)*}
}

macro_rules! flags {
    ($(#[$attr:meta])* $vis:vis struct $group:ident: $ty:ty { $($const:ident = $expr:expr),+ $(,)?}) => (
        $(#[$attr])* #[repr(transparent)] $vis struct $group {r#const: $ty}
        impl $group {
            $(pub const $const: Self = Self { r#const: $expr };)*
            #[inline(always)] pub const fn d(&self) -> $ty { self.r#const }
            const _BASE_HB: $ty = 1 << (<$ty>::BITS - 1);
            #[inline(always)] pub const fn name(&self) -> &'static str {
                match self.r#const {$(capture if capture == $expr => ::core::stringify!($const),)* _ => ::core::unreachable!()}
            }
            const LEN: usize = { let mut i = 0; $(let _ = $expr; i += 1;)+{i} };
            const A: [$ty; $group::LEN] = [$($expr,)+];
            const SANITY: () = {
                let ref a = Self::A; let l = a.len(); let mut i = 0;
                while i < l { let mut j = i + 1; while j < l { if a[i] == a[j] { panic!("conflict"); } j += 1; } i += 1; }
            };
            const ALL: $ty = { let mut r: $ty = 0; $( r |= $expr;)+ r };
            pub const fn has_flags_in(v: $ty) -> bool { Self::ALL & v != 0 }
            pub const fn bits() -> usize { let r: $ty = ($($expr+)+0); r.count_ones() as _ }
        }
        impl ::core::fmt::Debug for $group {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                const _V : () = $group::SANITY;
                ::core::write!(f, "{}::{}", ::core::stringify!($group), Self::name(self))
            }
        }
    );
}

#[allow(unused_macros)]
macro_rules! union {
    ($(#[$attr:meta])* $vis:vis union $name:ident $tail:tt) => (union!(@parse [$(#[$attr])* $vis union $name] [] $tail););
    ($(#[$attr:meta])* $vis:vis union $name:ident<$($lt:lifetime),*> $tail:tt) => (union!(@parse [$(#[$attr])* $vis union $name<$($lt),*>] [] $tail););
    (@parse $decl:tt [$($head:tt)*] {}) => (union!(@defeat0 $decl [$($head)*]););
    (@parse $decl:tt [$($head:tt)*] {$(#[$attr:meta])* $vis:vis !$ident:ident:$ty:ty,$($tail:tt)*}) => (
        union!(@parse $decl [$($head)* $(#[$attr])* $vis $ident: ::core::mem::ManuallyDrop::<$ty>,] {$($tail)*});
    );
    (@parse $decl:tt [$($head:tt)*] {$(#[$attr:meta])* $vis:vis $ident:ident:$ty:ty,$($tail:tt)*}) => (
        union!(@parse $decl [$($head)* $(#[$attr])* $vis $ident: $ty, ] { $($tail)* });
    );
    (@defeat0 [$($decls:tt)*] [$($head:tt)*]) => (union!(@defeat1 $($decls)* { $($head)* }););
    (@defeat1 $i:item) => ($i);
}

macro_rules! dbgfn {
    ($($vis:vis fn $fn:ident($($arg:ident: $argty:ty),* $(,)?) $(-> $ret:ty)? $block:block)*) => {
        $(dbgfn!(@int $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block {panic!("called dbg symbol in non-dbg build")});)*
    };
    ($($vis:vis fn $fn:ident($($arg:ident: $argty:ty),* $(,)?) $(-> $ret:ty)? $block:block else $block_b:block)*) => {
        $(dbgfn!(@int $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block $block_b);)*
    };
    (@int $vis:vis fn $fn:ident($($arg:ident: $argty:ty),* $(,)?) $(-> $ret:ty)? $block_a:block $block_b:block) => {
        #[cfg(debug_assertions)]
        $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block_a
        #[cfg(not(debug_assertions))]
        $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block_b
    }
}

#[allow(unused_macros)]
macro_rules! void {
    () => {
        ()
    };
}

/// Convert all the KV pairs into an iterator and then turn it into an appropriate collection
/// (inferred).
///
/// **Warning: This is going to be potentially slow due to the iterator creation**
macro_rules! into_dict {
    () => { ::core::default::Default::default() };
    ($($key:expr => $value:expr),+ $(,)?) => {{
        [$(($key.into(), $value.into())),+]
        .map(|(k, v)| (k, v))
        .into_iter()
        .collect()
    }};
}
