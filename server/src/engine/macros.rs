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

#[cfg(test)]
macro_rules! into_array {
    ($($e:expr),* $(,)?) => { [$($e.into()),*] };
}

macro_rules! as_array {
    ($($e:expr),* $(,)?) => { [$($e as _),*] };
}

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
macro_rules! multi_assert_eq {
    ($($lhs:expr),* => $rhs:expr) => {
        $(assert_eq!($lhs, $rhs);)*
    };
}

macro_rules! direct_from {
    ($for:ident<$lt:lifetime> => {$($other:ty as $me:ident),*$(,)?}) => {
        $(impl<$lt> ::core::convert::From<$other> for $for<$lt> {fn from(v: $other) -> Self {Self::$me(v.into())}})*
    };
    ($for:ty => {$($other:ty as $me:ident),*$(,)?}) => {
        $(impl ::core::convert::From<$other> for $for {fn from(v: $other) -> Self {Self::$me(v.into())}})*
    };
    ($for:ty[_] => {$($other:ty as $me:ident),*$(,)?}) => {
        $(impl ::core::convert::From<$other> for $for {fn from(_: $other) -> Self {Self::$me}})*
    };
}

macro_rules! flags {
    ($(#[$attr:meta])* $vis:vis struct $group:ident: $ty:ty { $($const:ident = $expr:expr),+ $(,)?}) => (
        $(#[$attr])* #[repr(transparent)] $vis struct $group {r#const: $ty}
        #[allow(unused)]
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
                let a = &Self::A; let l = a.len(); let mut i = 0;
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

macro_rules! __kw_misc {
    ($ident:ident) => {
        $crate::engine::ql::lex::Token::Keyword($crate::engine::ql::lex::Keyword::Misc(
            $crate::engine::ql::lex::KeywordMisc::$ident,
        ))
    };
}

macro_rules! __kw_stmt {
    ($ident:ident) => {
        $crate::engine::ql::lex::Token::Keyword($crate::engine::ql::lex::Keyword::Statement(
            $crate::engine::ql::lex::KeywordStmt::$ident,
        ))
    };
}

/*
    Frankly, this is just for lazy people like me. Do not judge
    -- Sayan (@ohsayan)
*/
macro_rules! Token {
    // misc symbol
    (@) => {
        __sym_token!(SymAt)
    };
    (#) => {
        __sym_token!(SymHash)
    };
    ($) => {
        __sym_token!(SymDollar)
    };
    (%) => {
        __sym_token!(SymPercent)
    };
    (.) => {
        __sym_token!(SymPeriod)
    };
    (,) => {
        __sym_token!(SymComma)
    };
    (_) => {
        __sym_token!(SymUnderscore)
    };
    (?) => {
        __sym_token!(SymQuestion)
    };
    (:) => {
        __sym_token!(SymColon)
    };
    (;) => {
        __sym_token!(SymSemicolon)
    };
    (~) => {
        __sym_token!(SymTilde)
    };
    // logical
    (!) => {
        __sym_token!(OpLogicalNot)
    };
    (^) => {
        __sym_token!(OpLogicalXor)
    };
    (&) => {
        __sym_token!(OpLogicalAnd)
    };
    (|) => {
        __sym_token!(OpLogicalOr)
    };
    // operator misc.
    (=) => {
        __sym_token!(OpAssign)
    };
    // arithmetic
    (+) => {
        __sym_token!(OpArithmeticAdd)
    };
    (-) => {
        __sym_token!(OpArithmeticSub)
    };
    (*) => {
        __sym_token!(OpArithmeticMul)
    };
    (/) => {
        __sym_token!(OpArithmeticDiv)
    };
    // relational
    (>) => {
        __sym_token!(OpComparatorGt)
    };
    (<) => {
        __sym_token!(OpComparatorLt)
    };
    // ddl keywords
    (use) => {
        __kw_stmt!(Use)
    };
    (create) => {
        __kw_stmt!(Create)
    };
    (alter) => {
        __kw_stmt!(Alter)
    };
    (drop) => {
        __kw_stmt!(Drop)
    };
    (model) => {
        __kw_misc!(Model)
    };
    (space) => {
        __kw_misc!(Space)
    };
    (primary) => {
        __kw_misc!(Primary)
    };
    // ddl misc
    (with) => {
        __kw_misc!(With)
    };
    (add) => {
        __kw_misc!(Add)
    };
    (remove) => {
        __kw_misc!(Remove)
    };
    (sort) => {
        __kw_misc!(Sort)
    };
    (type) => {
        __kw_misc!(Type)
    };
    // dml
    (insert) => {
        __kw_stmt!(Insert)
    };
    (select) => {
        __kw_stmt!(Select)
    };
    (update) => {
        __kw_stmt!(Update)
    };
    (delete) => {
        __kw_stmt!(Delete)
    };
    // dml misc
    (set) => {
        __kw_misc!(Set)
    };
    (limit) => {
        __kw_misc!(Limit)
    };
    (from) => {
        __kw_misc!(From)
    };
    (into) => {
        __kw_misc!(Into)
    };
    (where) => {
        __kw_misc!(Where)
    };
    (if) => {
        __kw_misc!(If)
    };
    (and) => {
        __kw_misc!(And)
    };
    (as) => {
        __kw_misc!(As)
    };
    (by) => {
        __kw_misc!(By)
    };
    (asc) => {
        __kw_misc!(Asc)
    };
    (desc) => {
        __kw_misc!(Desc)
    };
    // types
    (string) => {
        __kw_misc!(String)
    };
    (binary) => {
        __kw_misc!(Binary)
    };
    (list) => {
        __kw_misc!(List)
    };
    (map) => {
        __kw_misc!(Map)
    };
    (bool) => {
        __kw_misc!(Bool)
    };
    (int) => {
        __kw_misc!(Int)
    };
    (double) => {
        __kw_misc!(Double)
    };
    (float) => {
        __kw_misc!(Float)
    };
    // tt
    (open {}) => {
        __sym_token!(TtOpenBrace)
    };
    (close {}) => {
        __sym_token!(TtCloseBrace)
    };
    (() open) => {
        __sym_token!(TtOpenParen)
    };
    (() close) => {
        __sym_token!(TtCloseParen)
    };
    (open []) => {
        __sym_token!(TtOpenSqBracket)
    };
    (close []) => {
        __sym_token!(TtCloseSqBracket)
    };
    // misc
    (null) => {
        __kw_misc!(Null)
    };
    (not) => {
        __kw_misc!(Not)
    };
    (return) => {
        __kw_misc!(Return)
    };
    (allow) => {
        __kw_misc!(Allow)
    };
    (all) => {
        __kw_misc!(All)
    };
    (exists) => {
        __kw_stmt!(Exists)
    };
}

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
    ($($(#[$attr:meta])* $vis:vis fn $fn:ident($($arg:ident: $argty:ty),* $(,)?) $(-> $ret:ty)? $block:block)*) => {
        $(dbgfn!(@int $(#[$attr])* $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block {panic!("called dbg symbol in non-dbg build")});)*
    };
    ($($(#[$attr:meta])* $vis:vis fn $fn:ident($($arg:ident: $argty:ty),* $(,)?) $(-> $ret:ty)? $block:block else $block_b:block)*) => {
        $(dbgfn!(@int $(#[$attr])*  $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block $block_b);)*
    };
    (@int $(#[$attr:meta])* $vis:vis fn $fn:ident($($arg:ident: $argty:ty),* $(,)?) $(-> $ret:ty)? $block_a:block $block_b:block) => {
        #[cfg(debug_assertions)]
        $(#[$attr])* $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block_a
        #[cfg(not(debug_assertions))]
        $(#[$attr])* $vis fn $fn($($arg: $argty),*) $(-> $ret)? $block_b
    }
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

#[cfg(test)]
macro_rules! pairvec {
    ($($x:expr),*) => {{ let mut v = Vec::new(); $( let (a, b) = $x; v.push((a.into(), b.into())); )* v }};
}

#[cfg(test)]
macro_rules! intovec {
    ($($x:expr),* $(,)?) => { vec![$(core::convert::From::from($x),)*] };
}

macro_rules! sizeof {
    ($ty:ty) => {
        ::core::mem::size_of::<$ty>()
    };
    ($ty:ty, $by:literal) => {
        ::core::mem::size_of::<$ty>() * $by
    };
}

macro_rules! local {
    ($($vis:vis static$ident:ident:$ty:ty=$expr:expr;)*)=> {::std::thread_local!{$($vis static $ident: ::std::cell::RefCell::<$ty> = ::std::cell::RefCell::new($expr);)*}};
}

macro_rules! local_mut {
    ($ident:ident, $call:expr) => {{
        #[inline(always)]
        fn _f<T, U>(v: &::std::cell::RefCell<T>, f: impl FnOnce(&mut T) -> U) -> U {
            f(&mut *v.borrow_mut())
        }
        ::std::thread::LocalKey::with(&$ident, |v| _f(v, $call))
    }};
}

#[cfg(test)]
macro_rules! local_ref {
    ($ident:ident, $call:expr) => {{
        #[inline(always)]
        fn _f<T, U>(v: &::std::cell::RefCell<T>, f: impl FnOnce(&T) -> U) -> U {
            f(&v.borrow())
        }
        ::std::thread::LocalKey::with(&$ident, |v| _f(v, $call))
    }};
}
