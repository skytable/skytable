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
        $crate::engine::ql::lexer::Token::Symbol($crate::engine::ql::lexer::Symbol::$ident)
    };
}

macro_rules! __ddl_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::Ddl(
            $crate::engine::ql::lexer::DdlKeyword::$ident,
        ))
    };
}

macro_rules! __ddl_misc_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::DdlMisc(
            $crate::engine::ql::lexer::DdlMiscKeyword::$ident,
        ))
    };
}

macro_rules! __dml_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::Dml(
            $crate::engine::ql::lexer::DmlKeyword::$ident,
        ))
    };
}

macro_rules! __dml_misc_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::DmlMisc(
            $crate::engine::ql::lexer::DmlMiscKeyword::$ident,
        ))
    };
}

macro_rules! __type_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::TypeId(
            $crate::engine::ql::lexer::Type::$ident,
        ))
    };
}

macro_rules! __misc_token {
    ($ident:ident) => {
        $crate::engine::ql::lexer::Token::Keyword($crate::engine::ql::lexer::Keyword::Misc(
            $crate::engine::ql::lexer::MiscKeyword::$ident,
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
        __ddl_token!(Use)
    };
    (create) => {
        __ddl_token!(Create)
    };
    (alter) => {
        __ddl_token!(Alter)
    };
    (drop) => {
        __ddl_token!(Drop)
    };
    (inspect) => {
        __ddl_token!(Inspect)
    };
    (model) => {
        __ddl_token!(Model)
    };
    (space) => {
        __ddl_token!(Space)
    };
    (primary) => {
        __ddl_token!(Primary)
    };
    // ddl misc
    (with) => {
        __ddl_misc_token!(With)
    };
    (add) => {
        __ddl_misc_token!(Add)
    };
    (remove) => {
        __ddl_misc_token!(Remove)
    };
    (sort) => {
        __ddl_misc_token!(Sort)
    };
    (type) => {
        __ddl_misc_token!(Type)
    };
    // dml
    (insert) => {
        __dml_token!(Insert)
    };
    (select) => {
        __dml_token!(Select)
    };
    (update) => {
        __dml_token!(Update)
    };
    (delete) => {
        __dml_token!(Delete)
    };
    (exists) => {
        __dml_token!(Exists)
    };
    (truncate) => {
        __dml_token!(Truncate)
    };
    // dml misc
    (limit) => {
        __dml_misc_token!(Limit)
    };
    (from) => {
        __dml_misc_token!(From)
    };
    (into) => {
        __dml_misc_token!(Into)
    };
    (where) => {
        __dml_misc_token!(Where)
    };
    (if) => {
        __dml_misc_token!(If)
    };
    (and) => {
        __dml_misc_token!(And)
    };
    (as) => {
        __dml_misc_token!(As)
    };
    (by) => {
        __dml_misc_token!(By)
    };
    (asc) => {
        __dml_misc_token!(Asc)
    };
    (desc) => {
        __dml_misc_token!(Desc)
    };
    // types
    (string) => {
        __type_token!(String)
    };
    (binary) => {
        __type_token!(Binary)
    };
    (list) => {
        __type_token!(List)
    };
    (map) => {
        __type_token!(Map)
    };
    (bool) => {
        __type_token!(Bool)
    };
    (int) => {
        __type_token!(Int)
    };
    (double) => {
        __type_token!(Double)
    };
    (float) => {
        __type_token!(Float)
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
        __misc_token!(Null)
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
