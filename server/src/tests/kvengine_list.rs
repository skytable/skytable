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

#[sky_macros::dbtest_module(table = "keymap(str,list<str>)")]
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
    /// lget valueat
    async fn test_lget_with_valueat_okay() {
        lset!(con, "mylist", "v1");
        let q = query!("lget", "mylist", "valueat", "0");
        runeq!(con, q, Element::String("v1".to_owned()));
    }
    /// lget valueat (non-existent index)
    async fn test_lget_with_valueat_non_existent_index() {
        lset!(con, "mylist", "v1");
        let q = query!("lget", "mylist", "valueat", "1");
        runeq!(
            con,
            q,
            Element::RespCode(RespCode::ErrorString("bad-list-index".to_owned()))
        )
    }
    /// lget valueat (invalid index)
    async fn test_lget_with_valueat_bad_index() {
        lset!(con, "mylist", "v1");
        let q = query!("lget", "mylist", "valueat", "1a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype))
    }
    /// lget valueat (nil)
    async fn test_lget_with_valueat_nil() {
        let q = query!("lget", "mybadlist", "valueat", "2");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
    /// lget valueat (nil + bad index)
    async fn test_lget_with_bad_index_but_nil_key() {
        let q = query!("lget", "mybadlist", "valueat", "2a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
    }
    /// lget valueat (syntax error)
    async fn test_lget_with_valueat_syntax_error() {
        let q = query!("lget", "mybadlist", "valueat", "2", "3");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lget last
    /// lget last with one element
    async fn test_lget_last_with_last_one_element() {
        lset!(con, "mylist", "a");
        let q = query!("lget", "mylist", "last");
        runeq!(con, q, Element::String("a".to_owned()));
    }
    /// lget last with multiple elements
    async fn test_lget_last_with_last_many_elements() {
        lset!(con, "mylist", "a", "b", "c");
        let q = query!("lget", "mylist", "last");
        runeq!(con, q, Element::String("c".to_owned()));
    }
    /// lget last with empty list
    async fn test_lget_last_with_empty_list() {
        lset!(con, "mylist");
        let q = query!("lget", "mylist", "last");
        runeq!(
            con,
            q,
            Element::RespCode(RespCode::ErrorString("list-is-empty".to_owned()))
        );
    }
    /// lget last syntax error
    async fn test_lget_last_syntax_error() {
        let q = query!("lget", "mylist", "last", "abcd");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lget first
    /// lget first with one element
    async fn test_lget_first_with_last_one_element() {
        lset!(con, "mylist", "a");
        let q = query!("lget", "mylist", "first");
        runeq!(con, q, Element::String("a".to_owned()));
    }
    /// lget first with multiple elements
    async fn test_lget_first_with_last_many_elements() {
        lset!(con, "mylist", "a", "b", "c");
        let q = query!("lget", "mylist", "first");
        runeq!(con, q, Element::String("a".to_owned()));
    }
    /// lget first with empty list
    async fn test_lget_first_with_empty_list() {
        lset!(con, "mylist");
        let q = query!("lget", "mylist", "first");
        runeq!(
            con,
            q,
            Element::RespCode(RespCode::ErrorString("list-is-empty".to_owned()))
        );
    }
    /// lget last syntax error
    async fn test_lget_first_syntax_error() {
        let q = query!("lget", "mylist", "first", "abcd");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lmod tests
    // lmod push
    /// lmod push (okay)
    async fn test_lmod_push_okay() {
        lset!(con, "mylist");
        let q = query!("lmod", "mylist", "push", "v1");
        runeq!(con, q, Element::RespCode(RespCode::Okay));
    }
    /// lmod push multiple (okay)
    async fn test_lmod_push_multiple_okay() {
        lset!(con, "mylist");
        assert_okay!(con, query!("lmod", "mylist", "push", "v1", "v2"));
    }
    /// lmod push (nil)
    async fn test_lmod_push_nil() {
        let q = query!("lmod", "mylist", "push", "v1");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
    /// lmod push (syntax error)
    async fn test_lmod_syntax_error() {
        let q = query!("lmod", "mylist", "push");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lmod pop
    /// lmod pop (okay)
    async fn test_lmod_pop_noindex_okay() {
        lset!(con, "mylist", "value");
        let q = query!("lmod", "mylist", "pop");
        runeq!(con, q, Element::String("value".to_owned()));
    }
    /// lmod pop (good index; okay)
    async fn test_lmod_pop_goodindex_okay() {
        lset!(con, "mylist", "value1", "value2");
        let q = query!("lmod", "mylist", "pop", "1");
        runeq!(con, q, Element::String("value2".to_owned()));
    }
    /// lmod pop (bad index + existent key & non-existent key)
    async fn test_lmod_pop_badindex_fail() {
        lset!(con, "mylist", "v1", "v2");
        let q = query!("lmod", "mylist", "pop", "12badidx");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));

        // this is post-execution; so the error must be pointed out first
        let q = query!("lmod", "mymissinglist", "pop", "12badidx");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
    }
    /// lmod pop (nil)
    async fn test_lmod_pop_nil() {
        let q = query!("lmod", "mylist", "pop");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
    /// lmod pop (syntax error)
    async fn test_lmod_pop_syntax_error() {
        let q = query!("lmod", "mylist", "pop", "whatthe", "whatthe2");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lmod clear
    /// lmod clear (okay)
    async fn test_lmod_clear_okay() {
        lset!(con, "mylist", "v1", "v2");
        let q = query!("lmod", "mylist", "clear");
        runeq!(con, q, Element::RespCode(RespCode::Okay));
    }
    /// lmod clear (nil)
    async fn test_lmod_clear_nil() {
        let q = query!("lmod", "mylist", "clear");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
    /// lmod clear (syntax error)
    async fn test_lmod_clear_syntax_error() {
        let q = query!("lmod", "mylist", "clear", "unneeded arg");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lmod remove
    /// lmod remove (okay)
    async fn test_lmod_remove_okay() {
        lset!(con, "mylist", "v1");
        let q = query!("lmod", "mylist", "remove", "0");
        runeq!(con, q, Element::RespCode(RespCode::Okay));
    }
    /// lmod remove (nil)
    async fn test_lmod_remove_nil() {
        let q = query!("lmod", "mylist", "remove", "0");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }
    /// lmod remove (bad index; nil + existent)
    async fn test_lmod_remove_bad_index() {
        // non-existent key + bad idx
        let q = query!("lmod", "mylist", "remove", "1a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
        // existent key + bad idx
        lset!(con, "mylist");
        let q = query!("lmod", "mylist", "remove", "1a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
    }
    /// lmod remove (syntax error)
    async fn test_lmod_remove_syntax_error() {
        let q = query!("lmod", "mylist", "remove", "a", "b");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    // lmod insert
    /// lmod insert (okay)
    async fn test_lmod_insert_okay() {
        lset!(con, "mylist", "a", "c");
        let q = query!("lmod", "mylist", "insert", "1", "b");
        runeq!(con, q, Element::RespCode(RespCode::Okay));
        let q = query!("lget", "mylist");
        assert_skyhash_arrayeq!(str, con, q, "a", "b", "c");
    }
    /// lmod insert (bad index; present + nil)
    async fn test_lmod_insert_bad_index() {
        // nil
        let q = query!("lmod", "mylist", "insert", "1badindex", "b");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
        // present
        lset!(con, "mylist", "a", "c");
        let q = query!("lmod", "mylist", "insert", "1badindex", "b");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
    }
    /// lmod insert (syntax error)
    async fn test_lmod_insert_syntax_error() {
        let q = query!("lmod", "mylist", "insert", "1");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
        let q = query!("lmod", "mylist", "insert");
        runeq!(con, q, Element::RespCode(RespCode::ActionError));
    }
    /// lmod insert (present; non-existent index)
    async fn test_lmod_insert_non_existent_index() {
        lset!(con, "mylist", "a", "b");
        let q = query!(
            "lmod",
            "mylist",
            "insert",
            "125",
            "my-value-that-will-never-go-in"
        );
        runeq!(
            con,
            q,
            Element::RespCode(RespCode::ErrorString("bad-list-index".to_owned()))
        )
    }
    /// del <list> (existent; non-existent)
    async fn test_list_del() {
        // try an existent key
        lset!(con, "mylist", "v1", "v2");
        let q = query!("del", "mylist");
        runeq!(con, q, Element::UnsignedInt(1));
        // try the now non-existent key
        let q = query!("del", "mylist");
        runeq!(con, q, Element::UnsignedInt(0));
    }
    /// exists <list> (existent; non-existent)
    async fn test_list_exists() {
        lset!(con, "mylist");
        lset!(con, "myotherlist");
        let q = query!("exists", "mylist", "myotherlist", "badlist");
        runeq!(con, q, Element::UnsignedInt(2));
    }

    // tests for range
    async fn test_list_range_nil() {
        let q = query!("lget", "sayan", "range", "1", "10");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
        let q = query!("lget", "sayan", "range", "1");
        runeq!(con, q, Element::RespCode(RespCode::NotFound));
    }

    async fn test_list_range_bounded_okay() {
        lset!(con, "mylist", "1", "2", "3", "4", "5");
        let q = query!("lget", "mylist", "range", "0", "5");
        assert_skyhash_arrayeq!(str, con, q, "1", "2", "3", "4", "5");
    }

    async fn test_list_range_bounded_fail() {
        lset!(con, "mylist", "1", "2", "3", "4", "5");
        let q = query!("lget", "mylist", "range", "0", "165");
        runeq!(
            con,
            q,
            Element::RespCode(RespCode::ErrorString("bad-list-index".to_owned()))
        )
    }

    async fn test_list_range_unbounded_okay() {
        lset!(con, "mylist", "1", "2", "3", "4", "5");
        let q = query!("lget", "mylist", "range", "0");
        assert_skyhash_arrayeq!(str, con, q, "1", "2", "3", "4", "5");
    }

    async fn test_list_range_unbounded_fail() {
        lset!(con, "mylist", "1", "2", "3", "4", "5");
        let q = query!("lget", "mylist", "range", "165");
        runeq!(
            con,
            q,
            Element::RespCode(RespCode::ErrorString("bad-list-index".to_owned()))
        )
    }

    async fn test_list_range_parse_fail() {
        let q = query!("lget", "mylist", "range", "1", "2a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
        let q = query!("lget", "mylist", "range", "2a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
        // now do the same with an existing key
        lset!(con, "mylist", "a", "b", "c");
        let q = query!("lget", "mylist", "range", "1", "2a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
        let q = query!("lget", "mylist", "range", "2a");
        runeq!(con, q, Element::RespCode(RespCode::Wrongtype));
    }

    // sanity tests
    async fn test_get_model_error() {
        query.push("GET");
        query.push("mylist");
        runeq!(
            con,
            query,
            Element::RespCode(RespCode::ErrorString("wrong-model".to_owned()))
        );
    }
    async fn test_set_model_error() {
        query.push("SET");
        query.push("mylist");
        query.push("myvalue");
        runeq!(
            con,
            query,
            Element::RespCode(RespCode::ErrorString("wrong-model".to_owned()))
        );
    }
    async fn test_update_model_error() {
        query.push("UPDATE");
        query.push("mylist");
        query.push("myvalue");
        runeq!(
            con,
            query,
            Element::RespCode(RespCode::ErrorString("wrong-model".to_owned()))
        );
    }
}
