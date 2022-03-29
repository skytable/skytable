/*
 * Created on Thu Mar 17 2022
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

use sky_macros::dbtest_func as dbtest;
use skytable::{
    aio::Connection,
    query,
    types::{Array, RawString},
    Element, Query, RespCode,
};

#[dbtest(skip_if_cfg = "persist-suite", norun = true, port = 2007)]
async fn store_keyspace() {
    assert_okay!(con, query!("create", "keyspace", "universe"));
    switch_entity!(con, "universe");
    assert_okay!(con, query!("create", "table", "warp", "keymap(str,str)"));
    switch_entity!(con, "universe:warp");
    assert_okay!(con, query!("set", "x", "100"));
}
#[dbtest(run_if_cfg = "persist-suite", norun = true, port = 2007)]
async fn load_keyspace() {
    switch_entity!(con, "universe:warp");
    runeq!(con, query!("get", "x"), Element::String("100".to_owned()));
    switch_entity!(con, "default");
    assert_okay!(con, query!("drop", "table", "universe:warp"));
    assert_okay!(con, query!("drop", "keyspace", "universe"));
}

macro_rules! bin {
    ($input:expr) => {{
        const INVALID_SEQ: [u8; 2] = *b"\x80\x81";
        const RETLEN: usize = 2 + $input.len();
        const RET0: [u8; RETLEN] = {
            let mut iret: [u8; RETLEN] = [0u8; RETLEN];
            let mut idx = 0;
            while idx < $input.len() {
                iret[idx] = $input[idx];
                idx += 1;
            }
            iret[RETLEN - 2] = INVALID_SEQ[0];
            iret[RETLEN - 1] = INVALID_SEQ[1];
            iret
        };
        &RET0
    }};
}

mod auth;
mod kv;
mod kvlist;

const PERSIST_TEST_SET_SIZE: usize = 4;

trait PushIntoQuery {
    fn push_into(&self, query: &mut Query);
}

impl PushIntoQuery for &str {
    fn push_into(&self, q: &mut Query) {
        q.push(*self);
    }
}

impl PushIntoQuery for &[u8] {
    fn push_into(&self, q: &mut Query) {
        q.push(RawString::from(self.to_vec()))
    }
}

impl<T: PushIntoQuery, const N: usize> PushIntoQuery for [T; N] {
    fn push_into(&self, q: &mut Query) {
        for element in self {
            element.push_into(q)
        }
    }
}

impl<T: PushIntoQuery> PushIntoQuery for &[T] {
    fn push_into(&self, q: &mut Query) {
        for element in self.iter() {
            element.push_into(q)
        }
    }
}

trait PersistKey: PushIntoQuery {
    fn action_store() -> &'static str;
    fn action_load() -> &'static str;
}

macro_rules! impl_persist_key {
    ($($ty:ty => ($store:expr, $load:expr)),*) => {
        $(impl PersistKey for $ty {
            fn action_store() -> &'static str {
                $store
            }
            fn action_load() -> &'static str {
                $load
            }
        })*
    };
}

impl_persist_key!(
    &str => ("set", "get"),
    &[u8] => ("set", "get"),
    ListIDBin => ("lset", "lget"),
    ListIDStr => ("lset", "lget")
);

trait PersistValue: PushIntoQuery {
    fn response_store(&self) -> Element;
    fn response_load(&self) -> Element;
}

impl PersistValue for &str {
    fn response_store(&self) -> Element {
        Element::RespCode(RespCode::Okay)
    }
    fn response_load(&self) -> Element {
        Element::String(self.to_string())
    }
}

impl PersistValue for &[u8] {
    fn response_store(&self) -> Element {
        Element::RespCode(RespCode::Okay)
    }
    fn response_load(&self) -> Element {
        Element::Binstr(self.to_vec())
    }
}

impl<const N: usize> PersistValue for [&[u8]; N] {
    fn response_store(&self) -> Element {
        Element::RespCode(RespCode::Okay)
    }
    fn response_load(&self) -> Element {
        let mut flat = Vec::with_capacity(N);
        for item in self {
            flat.push(Some(item.to_vec()));
        }
        Element::Array(Array::Bin(flat))
    }
}

impl<const N: usize> PersistValue for [&str; N] {
    fn response_store(&self) -> Element {
        Element::RespCode(RespCode::Okay)
    }
    fn response_load(&self) -> Element {
        let mut flat = Vec::with_capacity(N);
        for item in self {
            flat.push(Some(item.to_string()));
        }
        Element::Array(Array::Str(flat))
    }
}

type Bin = &'static [u8];
type Str = &'static str;

#[derive(Debug)]
struct ListIDStr(Str);
#[derive(Debug)]
struct ListIDBin(Bin);

impl PushIntoQuery for ListIDStr {
    fn push_into(&self, q: &mut Query) {
        self.0.push_into(q)
    }
}

impl PushIntoQuery for ListIDBin {
    fn push_into(&self, q: &mut Query) {
        self.0.push_into(q)
    }
}

async fn persist_store<K: PersistKey, V: PersistValue>(
    con: &mut Connection,
    table_id: &str,
    declaration: &str,
    input: [(K, V); PERSIST_TEST_SET_SIZE],
) {
    create_table_and_switch!(con, table_id, declaration);
    for (key, value) in input {
        let mut query = Query::from(K::action_store());
        key.push_into(&mut query);
        value.push_into(&mut query);
        runeq!(con, query, value.response_store())
    }
}

async fn persist_load<K: PersistKey, V: PersistValue>(
    con: &mut Connection,
    table_id: &str,
    input: [(K, V); PERSIST_TEST_SET_SIZE],
) {
    switch_entity!(con, table_id);
    for (key, value) in input {
        let mut q = Query::from(K::action_load());
        key.push_into(&mut q);
        runeq!(con, q, value.response_load());
    }
    // now delete this table, freeing it up for the next suite run
    switch_entity!(con, "default:default");
    runeq!(
        con,
        query!("drop", "table", table_id),
        Element::RespCode(RespCode::Okay)
    );
}
