/*
 * Created on Tue Sep 07 2021
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

macro_rules! lset {
    ($con:expr, $listname:expr, $($val:expr),*) => {
        let mut q = skytable::Query::from("LSET");
        q.push($listname);
        $(q.push($val);)*
        runeq!($con, q, skytable::Element::RespCode(skytable::RespCode::Okay));
    };
    ($con:expr, $listname:expr) => {
        lset!($con, $listname, )
    }
}

#[sky_macros::dbtest(table = "keymap(str,list<str>)")]
mod __private {
    use skytable::{query, types::Array, Element, RespCode};

    // lset tests
    async fn test_lset_empty_okay() {
        lset!(con, "mylist");
    }
    async fn test_lset_with_values() {
        lset!(con, "mylist", "a", "b", "c", "d");
    }
    async fn test_lset_syntax_error() {
        let q = query!("LSET");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    async fn test_lset_overwrite_error() {
        lset!(con, "mylist");
        let q = query!("lset", "mylist");
        runeq!(con, q, Element::RespCode(RespCode::OverwriteError));
    }

    // lget tests
    async fn test_lget_emptylist_okay() {
        lset!(con, "mysuperlist");
        let q = query!("lget", "mysuperlist");
        runeq!(con, q, Element::Array(Array::Str(vec![])));
    }
    async fn test_lget_list_with_elements_okay() {
        lset!(con, "mysuperlist", "elementa", "elementb", "elementc");
        let q = query!("lget", "mysuperlist");
        assert_skyhash_arrayeq!(str, con, q, "elementa", "elementb", "elementc");
    }
    /// lget limit
    async fn test_lget_list_with_limit() {
        lset!(con, "mysuperlist", "elementa", "elementb", "elementc");
        let q = query!("lget", "mysuperlist", "LIMIT", "2");
        assert_skyhash_arrayeq!(str, con, q, "elementa", "elementb");
    }
    /// lget bad limit
    async fn test_lget_list_with_bad_limit() {
        lset!(con, "mysuperlist", "elementa", "elementb", "elementc");
        let q = query!("lget", "mylist", "LIMIT", "badlimit");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
    }
    /// lget huge limit
    async fn test_lget_with_huge_limit() {
        lset!(con, "mysuperlist", "elementa", "elementb", "elementc");
        let q = query!("lget", "mysuperlist", "LIMIT", "100");
        assert_skyhash_arrayeq!(str, con, q, "elementa", "elementb", "elementc");
    }
    /// lget syntax error
    async fn test_lget_with_limit_syntax_error() {
        let q = query!("lget", "mylist", "LIMIT", "100", "200");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    /// lget limit non-existent key
    async fn test_lget_with_limit_nil() {
        let q = query!("lget", "mylist", "LIMIT", "100");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
    /// lget len
    async fn test_lget_with_len_okay() {
        lset!(con, "mysuperlist", "elementa", "elementb", "elementc");
        let q = query!("lget", "mysuperlist", "len");
        runeq!(con, q, Element::UnsignedInt(3));
    }
    /// lget len syntax error
    async fn test_lget_with_len_syntax_error() {
        let q = query!("lget", "mysuperlist", "len", "whatthe");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    /// lget len nil
    async fn test_lget_with_len_nil() {
        let q = query!("lget", "mysuperlist", "len");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
}
