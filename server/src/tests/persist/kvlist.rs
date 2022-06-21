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

use super::{persist_load, persist_store, Bin, ListIDBin, ListIDStr, Str, PERSIST_TEST_SET_SIZE};
use sky_macros::dbtest_func as dbtest;

type ListData<K, V> = [(K, [V; PERSIST_TEST_SET_SIZE]); PERSIST_TEST_SET_SIZE];

macro_rules! listdata {
    (
        $(
            $listid:expr => $element:expr
        ),*
    ) => {
        [
            $(
                (
                    $listid,
                    $element
                ),
            )*
        ]
    };
}

macro_rules! binid {
    ($id:expr) => {
        ListIDBin(bin!($id))
    };
}

macro_rules! binlist {
    ($($elem:expr),*) => {
        [
            $(
                bin!($elem),
            )*
        ]
    };
}

// bin,list<bin>
const DATA_BIN_LISTBIN: ListData<ListIDBin, Bin> = listdata!(
    binid!(b"list1") => binlist!(b"e1", b"e2", b"e3", b"e4"),
    binid!(b"list2") => binlist!(b"e1", b"e2", b"e3", b"e4"),
    binid!(b"list3") => binlist!(b"e1", b"e2", b"e3", b"e4"),
    binid!(b"list4") => binlist!(b"e1", b"e2", b"e3", b"e4")
);
const TABLE_BIN_LISTBIN: &str = "testsuite.persist_bin_listbin";

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_bin_bin() {
    persist_store(
        &mut con,
        TABLE_BIN_LISTBIN,
        "(binary, list<binary>)",
        DATA_BIN_LISTBIN,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_bin_bin() {
    persist_load(&mut con, TABLE_BIN_LISTBIN, DATA_BIN_LISTBIN).await;
}

// bin,list<str>
const DATA_BIN_LISTSTR: ListData<ListIDBin, Str> = listdata!(
    binid!(b"list1") => ["e1", "e2", "e3", "e4"],
    binid!(b"list2") => ["e1", "e2", "e3", "e4"],
    binid!(b"list3") => ["e1", "e2", "e3", "e4"],
    binid!(b"list4") => ["e1", "e2", "e3", "e4"]
);

const TABLE_BIN_LISTSTR: &str = "testsuite.persist_bin_liststr";

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_bin_str() {
    persist_store(
        &mut con,
        TABLE_BIN_LISTSTR,
        "(binary, list<string>)",
        DATA_BIN_LISTSTR,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_bin_str() {
    persist_load(&mut con, TABLE_BIN_LISTSTR, DATA_BIN_LISTSTR).await;
}

// str,list<bin>
const DATA_STR_LISTBIN: ListData<ListIDStr, Bin> = listdata!(
    ListIDStr("list1") => binlist!(b"e1", b"e2", b"e3", b"e4"),
    ListIDStr("list2") => binlist!(b"e1", b"e2", b"e3", b"e4"),
    ListIDStr("list3") => binlist!(b"e1", b"e2", b"e3", b"e4"),
    ListIDStr("list4") => binlist!(b"e1", b"e2", b"e3", b"e4")
);

const TABLE_STR_LISTBIN: &str = "testsuite.persist_str_listbin";

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_str_bin() {
    persist_store(
        &mut con,
        TABLE_STR_LISTBIN,
        "(string, list<binary>)",
        DATA_STR_LISTBIN,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_str_bin() {
    persist_load(&mut con, TABLE_STR_LISTBIN, DATA_STR_LISTBIN).await;
}

// str,list<str>
const DATA_STR_LISTSTR: ListData<ListIDStr, Str> = listdata!(
    ListIDStr("list1") => ["e1", "e2", "e3", "e4"],
    ListIDStr("list2") => ["e1", "e2", "e3", "e4"],
    ListIDStr("list3") => ["e1", "e2", "e3", "e4"],
    ListIDStr("list4") => ["e1", "e2", "e3", "e4"]
);

const TABLE_STR_LISTSTR: &str = "testsuite.persist_str_liststr";

#[dbtest(skip_if_cfg = "persist-suite", norun = true)]
async fn store_str_str() {
    persist_store(
        &mut con,
        TABLE_STR_LISTSTR,
        "(string, list<string>)",
        DATA_STR_LISTSTR,
    )
    .await;
}

#[dbtest(run_if_cfg = "persist-suite", norun = true)]
async fn load_str_str() {
    persist_load(&mut con, TABLE_STR_LISTSTR, DATA_STR_LISTSTR).await;
}
