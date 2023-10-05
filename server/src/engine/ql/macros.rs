/*
 * Created on Fri Sep 16 2022
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

macro_rules! __sym_token {
    ($ident:ident) => {
        $crate::engine::ql::lex::Token::Symbol($crate::engine::ql::lex::Symbol::$ident)
    };
}

macro_rules! __kw_misc {
    ($ident:ident) => {
        $crate::engine::ql::lex::Token::Keyword($crate::engine::ql::lex::Keyword::Misc($crate::engine::ql::lex::KeywordMisc::$ident))
    };
}

macro_rules! __kw_stmt {
    ($ident:ident) => {
        $crate::engine::ql::lex::Token::Keyword($crate::engine::ql::lex::Keyword::Statement($crate::engine::ql::lex::KeywordStmt::$ident))
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
}

#[cfg(test)]
macro_rules! dict {
    () => {
        <::std::collections::HashMap<_, _> as ::core::default::Default>::default()
    };
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut hm: ::std::collections::HashMap<_, _> = ::core::default::Default::default();
        $(hm.insert($key.into(), $value.into());)*
        hm
    }};
}

#[cfg(test)]
macro_rules! null_dict {
    () => {
        dict! {}
    };
    ($($key:expr => $value:expr),* $(,)?) => {
        dict! {
            $(
                $key => $crate::engine::ql::tests::NullableDictEntry::data($value),
            )*
        }
    };
}

#[cfg(test)]
macro_rules! dict_nullable {
    () => {
        <::std::collections::HashMap<_, _> as ::core::default::Default>::default()
    };
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut hm: ::std::collections::HashMap<_, _> = ::core::default::Default::default();
        $(hm.insert($key.into(), $crate::engine::ql::tests::nullable_datatype($value));)*
        hm
    }};
}

#[cfg(test)]
macro_rules! into_array_nullable {
    ($($e:expr),* $(,)?) => { [$($crate::engine::ql::tests::nullable_datatype($e)),*] };
}

#[allow(unused_macros)]
macro_rules! statictbl {
    ($name:ident: $kind:ty => [$($expr:expr),*]) => {{
        const LEN: usize = {let mut i = 0;$(let _ = $expr; i += 1;)*i};
        static $name: [$kind; LEN] = [$($expr),*];
        &'static $name
    }};
}

macro_rules! build_lut {
    (
        $(#[$attr_s:meta])* $vis_s:vis static $LUT:ident in $lut:ident; $(#[$attr_e:meta])* $vis_e:vis enum $SYM:ident {$($variant:ident = $match:literal),*$(,)?}
        |$arg:ident: $inp:ty| -> $ret:ty $block:block,
        |$arg2:ident: $inp2:ty| -> String $block2:block
    ) => {
        mod $lut {
            pub const L: usize = { let mut i = 0; $(let _ = $match;i += 1;)*i };
            pub const fn f($arg: $inp) -> $ret $block
            pub fn s($arg2: $inp2) -> String $block2
        }
        $(#[$attr_e])* $vis_e enum $SYM {$($variant),*}
        $(#[$attr_s])* $vis_s static $LUT: [($ret, $SYM); $lut::L] = {[$(($lut::f($match), $SYM::$variant)),*]};
        impl ::std::string::ToString for $SYM {
            fn to_string(&self) -> ::std::string::String {match self {$(Self::$variant => {$lut::s($match)},)*}}
        }
    }
}

#[cfg(test)]
macro_rules! into_vec {
    ($ty:ty => ($($v:expr),* $(,)?)) => {{
        let v: Vec<$ty> = std::vec![$($v.into(),)*];
        v
    }}
}

#[cfg(test)]
macro_rules! lit {
    ($lit:expr) => {
        $crate::engine::data::lit::Lit::from($lit)
    };
}
