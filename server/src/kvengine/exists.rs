/*
 * Created on Wed Aug 19 2020
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

//! # `EXISTS` queries
//! This module provides functions to work with `EXISTS` queries

use crate::coredb::CoreDB;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::RespCodes;

/// Run an `EXISTS` query
pub fn exists(handle: &CoreDB, act: DataGroup) -> Response {
    if act.len() < 2 {
        return RespCodes::ActionError.into_response();
    }
    let mut resp = SResp::new();
    let mut respgroup = RespGroup::new();
    act.into_iter().skip(1).for_each(|key| {
        if handle.exists(&key) {
            respgroup.add_item(RespCodes::Okay);
        } else {
            respgroup.add_item(RespCodes::NotFound);
        }
    });
    resp.add_group(respgroup);
    resp.into_response()
}

#[cfg(test)]
#[test]
fn test_exists() {
    let db = CoreDB::new().unwrap();
    db.set(&"foo".to_owned(), &"foobar".to_owned()).unwrap();
    db.set(&"superfoo".to_owned(), &"superbar".to_owned())
        .unwrap();
    let query = vec!["EXISTS".to_owned(), "foo".to_owned(), "superfoo".to_owned()];
    let (r1, r2, r3) = exists(&db, DataGroup::new(query));
    db.finish_db(true, true, true);
    let r = [r1, r2, r3].concat();
    assert_eq!("*!9!7\n#2#2#2\n&2\n!0\n!0\n".as_bytes().to_owned(), r);
}
