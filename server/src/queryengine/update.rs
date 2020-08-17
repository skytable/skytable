/*
 * Created on Mon Aug 17 2020
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

//! # `UPDATE` queries
//! This module provides functions to work with `UPDATE` queries

use crate::coredb::CoreDB;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::RespCodes;

/// Run an `UPDATE` query
pub fn update(handle: &CoreDB, act: DataGroup) -> Response {
    if (act.len() - 1) & 1 != 0 {
        return RespCodes::ActionError.into_response();
    }
    let mut resp = SResp::new();
    let mut respgroup = RespGroup::new();
    act[1..]
        .chunks_exact(2)
        .for_each(|key| match handle.update(&key[0], &key[1]) {
            Ok(_) => respgroup.add_item(RespCodes::Okay),
            Err(e) => respgroup.add_item(e),
        });
    resp.add_group(respgroup);
    resp.into_response()
}

#[test]
fn test_update() {
    let db = CoreDB::new().unwrap();
    db.set("foo", &"bar".to_owned()).unwrap();
    assert_eq!(db.get("foo").unwrap(), "bar");
    let act = DataGroup::new(vec![
        "UPDATE".to_owned(),
        "foo".to_owned(),
        "newbar".to_owned(),
        "foo".to_owned(),
        "latestbar".to_owned(),
    ]);
    let (r1, r2, r3) = update(&db, act);
    let r = [r1, r2, r3].concat();
    assert_eq!(db.get("foo").unwrap(), "latestbar");
    db.finish_db(true, true, true);
    assert_eq!("*!9!7\n#2#2#2\n&2\n!0\n!0\n".as_bytes().to_owned(), r);
}
