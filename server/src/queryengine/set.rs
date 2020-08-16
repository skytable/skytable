/*
 * Created on Fri Aug 14 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # `SET` queries
//! This module provides functions to work with `SET` queries

use crate::coredb::CoreDB;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::RespCodes;

/// Run a `SET` query
pub fn set(handle: &CoreDB, act: DataGroup) -> Response {
    if (act.len() - 1) & 1 != 0 {
        return RespCodes::ActionError.into_response();
    }
    let mut resp = SResp::new();
    let mut respgroup = RespGroup::new();
    act[1..]
        .chunks_exact(2)
        .for_each(|key| match handle.set(&key[0], &key[1]) {
            Ok(_) => respgroup.add_item(RespCodes::Okay),
            Err(e) => respgroup.add_item(e),
        });
    resp.add_group(respgroup);
    resp.into_response()
}

#[test]
fn test_set() {
    let db = CoreDB::new().unwrap();
    let act = DataGroup::new(vec![
        "SET".to_owned(),
        "foo1".to_owned(),
        "bar".to_owned(),
        "foo2".to_owned(),
        "bar".to_owned(),
    ]);
    let (r1, r2, r3) = set(&db, act);
    let r = [r1, r2, r3].concat();
    assert!(db.exists("foo1") && db.exists("foo2"));
    db.finish_db(true, true, true);
    assert_eq!("*!9!7\n#2#2#2\n&2\n!0\n!0\n".as_bytes().to_owned(), r);
}
