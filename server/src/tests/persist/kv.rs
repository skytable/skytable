/*
 * Created on Sat Mar 19 2022
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

use super::{persist_load, persist_store, PERSIST_TEST_SET_SIZE};
use sky_macros::dbtest_func as dbtest;

const PERSIST_CFG_KEYMAP_BIN_BIN_TABLE: &str = "testsuite:persist_bin_bin_tbl";
const PERSIST_DATA_KEYMAP_BIN_BIN_TABLE: [(&[u8], &[u8]); PERSIST_TEST_SET_SIZE] = [
    (bin!(b"mykey1"), bin!(b"myval1")),
    (bin!(b"mykey2"), bin!(b"myval2")),
    (bin!(b"mykey3"), bin!(b"myval3")),
    (bin!(b"mykey4"), bin!(b"myval4")),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_keymap_bin_bin() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_BIN_TABLE,
        "keymap(binstr,binstr)",
        PERSIST_DATA_KEYMAP_BIN_BIN_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_keymap_bin_bin() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_BIN_TABLE,
        PERSIST_DATA_KEYMAP_BIN_BIN_TABLE,
    )
    .await;
}

const PERSIST_CFG_KEYMAP_BIN_STR_TABLE: &str = "testsuite:persist_bin_str_tbl";
const PERSIST_DATA_KEYMAP_BIN_STR_TABLE: [(&[u8], &str); PERSIST_TEST_SET_SIZE] = [
    (bin!(b"mykey1"), "myval1"),
    (bin!(b"mykey2"), "myval2"),
    (bin!(b"mykey3"), "myval3"),
    (bin!(b"mykey4"), "myval4"),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_keymap_bin_str() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_STR_TABLE,
        "keymap(binstr,str)",
        PERSIST_DATA_KEYMAP_BIN_STR_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_keymap_bin_str() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_BIN_STR_TABLE,
        PERSIST_DATA_KEYMAP_BIN_STR_TABLE,
    )
    .await;
}

const PERSIST_CFG_KEYMAP_STR_STR_TABLE: &str = "testsuite:persist_str_str_tbl";
const PERSIST_DATA_KEYMAP_STR_STR_TABLE: [(&str, &str); PERSIST_TEST_SET_SIZE] = [
    ("mykey1", "myval1"),
    ("mykey2", "myval2"),
    ("mykey3", "myval3"),
    ("mykey4", "myval4"),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_keymap_str_str() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_STR_TABLE,
        "keymap(str,str)",
        PERSIST_DATA_KEYMAP_STR_STR_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_keymap_str_str() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_STR_TABLE,
        PERSIST_DATA_KEYMAP_STR_STR_TABLE,
    )
    .await;
}

const PERSIST_CFG_KEYMAP_STR_BIN_TABLE: &str = "testsuite:persist_str_bin_tbl";
const PERSIST_DATA_KEYMAP_STR_BIN_TABLE: [(&str, &[u8]); PERSIST_TEST_SET_SIZE] = [
    ("mykey1", bin!(b"myval1")),
    ("mykey2", bin!(b"myval2")),
    ("mykey3", bin!(b"myval3")),
    ("mykey4", bin!(b"myval4")),
];

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_keymap_str_bin() {
    persist_store(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_BIN_TABLE,
        "keymap(str,binstr)",
        PERSIST_DATA_KEYMAP_STR_BIN_TABLE,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_keymap_str_bin() {
    persist_load(
        &mut con,
        PERSIST_CFG_KEYMAP_STR_BIN_TABLE,
        PERSIST_DATA_KEYMAP_STR_BIN_TABLE,
    )
    .await;
}
