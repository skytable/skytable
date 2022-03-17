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
use skytable::{aio::Connection, query, types::RawString, Element, Query, RespCode};

const PERIST_TEST_SET_SIZE: usize = 4;

trait AsQueryItem {
    fn push_into_query(&self, query: &mut Query);
    fn as_element(&self) -> Element;
}

impl AsQueryItem for &'static [u8] {
    fn push_into_query(&self, query: &mut Query) {
        query.push(RawString::from(self.to_vec()));
    }
    fn as_element(&self) -> Element {
        Element::Binstr(self.to_vec())
    }
}

impl AsQueryItem for &'static str {
    fn push_into_query(&self, query: &mut Query) {
        query.push(*self);
    }
    fn as_element(&self) -> Element {
        Element::String(self.to_string())
    }
}

async fn persist_store<K: AsQueryItem, V: AsQueryItem>(
    con: &mut Connection,
    table_id: &str,
    declaration: &str,
    input: [(K, V); PERIST_TEST_SET_SIZE],
) {
    create_table_and_switch!(con, table_id, declaration);
    for (key, value) in input {
        let mut query = Query::from("set");
        key.push_into_query(&mut query);
        value.push_into_query(&mut query);
        runeq!(con, query, Element::RespCode(RespCode::Okay))
    }
}

async fn persist_load<K: AsQueryItem, V: AsQueryItem>(
    con: &mut Connection,
    table_id: &str,
    input: [(K, V); PERIST_TEST_SET_SIZE],
) {
    switch_entity!(con, table_id);
    for (key, value) in input {
        let mut q = Query::from("get");
        key.push_into_query(&mut q);
        runeq!(con, q, value.as_element());
    }
    // now delete this table, freeing it up for the next suite run
    switch_entity!(con, "default:default");
    runeq!(
        con,
        query!("drop", "table", table_id),
        Element::RespCode(RespCode::Okay)
    );
}

const PERSIST_CFG_KEYMAP_BIN_BIN_TABLE: &str = "testsuite:persist_bin_bin_tbl";
const PERSIST_DATA_KEYMAP_BIN_BIN_TABLE: [(&[u8], &[u8]); PERIST_TEST_SET_SIZE] = [
    (b"mykey1\xF0\x90\x80", b"myval1\xF0\x90\x80"),
    (b"mykey2\xF0\x90\x80", b"myval2\xF0\x90\x80"),
    (b"mykey3\xF0\x90\x80", b"myval3\xF0\x90\x80"),
    (b"mykey4\xF0\x90\x80", b"myval4\xF0\x90\x80"),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn persist_store_keymap_bin_bin() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_BIN_TABLE,
        "keymap(binstr,binstr)",
        PERSIST_DATA_KEYMAP_BIN_BIN_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn persist_load_keymap_bin_bin() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_BIN_TABLE,
        PERSIST_DATA_KEYMAP_BIN_BIN_TABLE,
    )
    .await;
}

const PERSIST_CFG_KEYMAP_BIN_STR_TABLE: &str = "testsuite:persist_bin_str_tbl";
const PERSIST_DATA_KEYMAP_BIN_STR_TABLE: [(&[u8], &str); PERIST_TEST_SET_SIZE] = [
    (b"mykey1\xF0\x90\x80", "myval1"),
    (b"mykey2\xF0\x90\x80", "myval2"),
    (b"mykey3\xF0\x90\x80", "myval3"),
    (b"mykey4\xF0\x90\x80", "myval4"),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn persist_store_keymap_bin_str() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_STR_TABLE,
        "keymap(binstr,str)",
        PERSIST_DATA_KEYMAP_BIN_STR_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn persist_load_keymap_bin_str() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_STR_TABLE,
        PERSIST_DATA_KEYMAP_BIN_STR_TABLE,
    )
    .await;
}

const PERSIST_CFG_KEYMAP_STR_STR_TABLE: &str = "testsuite:persist_str_str_tbl";
const PERSIST_DATA_KEYMAP_STR_STR_TABLE: [(&str, &str); PERIST_TEST_SET_SIZE] = [
    ("mykey1", "myval1"),
    ("mykey2", "myval2"),
    ("mykey3", "myval3"),
    ("mykey4", "myval4"),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn persist_store_keymap_str_str() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_STR_TABLE,
        "keymap(str,str)",
        PERSIST_DATA_KEYMAP_STR_STR_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn persist_load_keymap_str_str() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_STR_TABLE,
        PERSIST_DATA_KEYMAP_STR_STR_TABLE,
    )
    .await;
}

const PERSIST_CFG_KEYMAP_STR_BIN_TABLE: &str = "testsuite:persist_str_bin_tbl";
const PERSIST_DATA_KEYMAP_STR_BIN_TABLE: [(&str, &[u8]); PERIST_TEST_SET_SIZE] = [
    ("mykey1", b"myval1\xF0\x90\x80"),
    ("mykey2", b"myval2\xF0\x90\x80"),
    ("mykey3", b"myval3\xF0\x90\x80"),
    ("mykey4", b"myval4\xF0\x90\x80"),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn persist_store_keymap_str_bin() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_BIN_TABLE,
        "keymap(str,binstr)",
        PERSIST_DATA_KEYMAP_STR_BIN_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn persist_load_keymap_str_bin() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_BIN_TABLE,
        PERSIST_DATA_KEYMAP_STR_BIN_TABLE,
    )
    .await;
}
