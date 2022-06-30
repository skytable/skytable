/*
 * Created on Tue Mar 29 2022
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

use {
    sky_macros::dbtest_func as dbtest,
    skytable::{query, Element, RespCode},
};

const SNAPSHOT_DISABLED: &str = "err-snapshot-disabled";

#[dbtest]
async fn snapshot_fail_because_local_disabled() {
    runeq!(
        con,
        query!("mksnap"),
        Element::RespCode(RespCode::ErrorString(SNAPSHOT_DISABLED.to_owned()))
    )
}

#[dbtest(skip_if_cfg = "persist-suite")]
async fn rsnap_okay() {
    loop {
        match con.run_query_raw(query!("mksnap", "myremo")).await.unwrap() {
            Element::RespCode(RespCode::Okay) => break,
            Element::RespCode(RespCode::ErrorString(estr)) if estr.eq("err-snapshot-busy") => {}
            x => panic!("snapshot failed: {:?}", x),
        }
    }
}

#[dbtest(port = 2007)]
async fn local_snapshot_from_remote_okay() {
    assert_okay!(con, query!("mksnap"))
}

#[dbtest(port = 2007, skip_if_cfg = "persist-suite")]
async fn remote_snapshot_okay_with_local_enabled() {
    loop {
        match con.run_query_raw(query!("mksnap", "myremo")).await.unwrap() {
            Element::RespCode(RespCode::Okay) => break,
            Element::RespCode(RespCode::ErrorString(estr)) if estr.eq("err-snapshot-busy") => {}
            x => panic!("snapshot failed: {:?}", x),
        }
    }
}

#[dbtest(port = 2007, skip_if_cfg = "persist-suite")]
async fn remote_snapshot_fail_because_already_exists() {
    loop {
        match con.run_query_raw(query!("mksnap", "dupe")).await.unwrap() {
            Element::RespCode(RespCode::Okay) => break,
            Element::RespCode(RespCode::ErrorString(estr)) if estr.eq("err-snapshot-busy") => {}
            x => panic!("snapshot failed: {:?}", x),
        }
    }
    loop {
        match con.run_query_raw(query!("mksnap", "dupe")).await.unwrap() {
            Element::RespCode(RespCode::ErrorString(estr)) => match estr.as_str() {
                "err-snapshot-busy" => {}
                "duplicate-snapshot" => break,
                _ => panic!("Got error string: {estr} instead"),
            },
            x => panic!("snapshot failed: {:?}", x),
        }
    }
}
