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

#[cfg(test)]
macro_rules! assert_full_tt {
    ($a:expr, $b:expr) => {
        assert_eq!($a, $b, "full token stream not utilized")
    };
}

macro_rules! __sym_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Symbol($crate::engine::ql::lexer::Symbol::$ident)
    };
}

macro_rules! __kw {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::$ident)
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
        __kw!(Use)
    };
    (create) => {
        __kw!(Create)
    };
    (alter) => {
        __kw!(Alter)
    };
    (drop) => {
        __kw!(Drop)
    };
    (describe) => {
        __kw!(Describe)
    };
    (model) => {
        __kw!(Model)
    };
    (space) => {
        __kw!(Space)
    };
    (primary) => {
        __kw!(Primary)
    };
    // ddl misc
    (with) => {
        __kw!(With)
    };
    (add) => {
        __kw!(Add)
    };
    (remove) => {
        __kw!(Remove)
    };
    (sort) => {
        __kw!(Sort)
    };
    (type) => {
        __kw!(Type)
    };
    // dml
    (insert) => {
        __kw!(Insert)
    };
    (select) => {
        __kw!(Select)
    };
    (update) => {
        __kw!(Update)
    };
    (delete) => {
        __kw!(Delete)
    };
    (exists) => {
        __kw!(Exists)
    };
    (truncate) => {
        __kw!(Truncate)
    };
    // dml misc
    (set) => {
        __kw!(Set)
    };
    (limit) => {
        __kw!(Limit)
    };
    (from) => {
        __kw!(From)
    };
    (into) => {
        __kw!(Into)
    };
    (where) => {
        __kw!(Where)
    };
    (if) => {
        __kw!(If)
    };
    (and) => {
        __kw!(And)
    };
    (as) => {
        __kw!(As)
    };
    (by) => {
        __kw!(By)
    };
    (asc) => {
        __kw!(Asc)
    };
    (desc) => {
        __kw!(Desc)
    };
    // types
    (string) => {
        __kw!(String)
    };
    (binary) => {
        __kw!(Binary)
    };
    (list) => {
        __kw!(List)
    };
    (map) => {
        __kw!(Map)
    };
    (bool) => {
        __kw!(Bool)
    };
    (int) => {
        __kw!(Int)
    };
    (double) => {
        __kw!(Double)
    };
    (float) => {
        __kw!(Float)
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
        __kw!(Null)
    };
}

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

macro_rules! nullable_dict {
    () => {
        dict! {}
    };
    ($($key:expr => $value:expr),* $(,)?) => {
        dict! {
            $(
                $key => $crate::engine::ql::tests::NullableMapEntry::data($value),
            )*
        }
    };
}

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

macro_rules! set {
    () => {
        <::std::collections::HashSet<_> as ::core::default::Default>::default()
    };
    ($($key:expr),* $(,)?) => {{
        let mut hs: ::std::collections::HashSet<_> = ::core::default::Default::default();
        $(hs.insert($key.into());)*
        hs
    }};
}

macro_rules! into_array {
    ($($e:expr),* $(,)?) => { [$($e.into()),*] };
}

macro_rules! into_array_nullable {
    ($($e:expr),* $(,)?) => { [$($crate::engine::ql::tests::nullable_datatype($e)),*] };
}

macro_rules! statictbl {
    ($name:ident: $kind:ty => [$($expr:expr),*]) => {{
        const LEN: usize = {let mut i = 0;$(let _ = $expr; i += 1;)*i};
        static $name: [$kind; LEN] = [$($expr),*];
        &'static $name
    }};
}
