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

//! # `GET` queries
//! This module provides functions to work with `GET` queries

use crate::coredb::CoreDB;
use libtdb::builders::response::*;
use libtdb::de::DataGroup;
use libtdb::terrapipe::RespCodes;

/// Run a `GET` query
pub fn get(handle: &CoreDB, act: DataGroup) -> Response {
    if act.len() < 2 {
        return RespCodes::ActionError.into_response();
    }
    let mut resp = SResp::new();
    let mut respgroup = RespGroup::new();
    act.into_iter()
        .skip(1)
        .for_each(|key| match handle.get(&key) {
            Ok(byts) => respgroup.add_item(BytesWrapper(byts)),
            Err(e) => respgroup.add_item(e),
        });
    resp.add_group(respgroup);
    resp.into_response()
}

#[cfg(test)]
#[test]
fn test_get() {
    let db = CoreDB::new().unwrap();
    let _ = db.set(&"foo1".to_owned(), &"bar".to_owned()).unwrap();
    let _ = db.set(&"foo2".to_owned(), &"bar".to_owned()).unwrap();
    let (r1, r2, r3) = get(
        &db,
        DataGroup::new(vec!["get".to_owned(), "foo1".to_owned(), "foo2".to_owned()]),
    );
    let r = [r1, r2, r3].concat();
    db.finish_db(true, true, true);
    assert_eq!("*!13!7\n#2#4#4\n&2\n+bar\n+bar\n".as_bytes().to_owned(), r);
}
