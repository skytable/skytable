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

//! # `DEL` queries
//! This module provides functions to work with `DEL` queries

use crate::coredb::{self, CoreDB};
use crate::resputil::*;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::responses;

/// Run a `DEL` query
pub fn del(handle: &CoreDB, act: DataGroup) -> Response {
    let howmany = act.len() - 1;
    if howmany == 0 {
        // What's the use of just a `del`? Tell us more!
        return responses::ARG_ERR.to_owned();
    }
    // Get a write lock
    let mut db_handle = handle.acquire_write();
    // Assume that half of the actions will fail
    let mut except_for = ExceptFor::with_space_for(howmany / 2);
    act.into_iter().skip(1).enumerate().for_each(|(idx, key)| {
        if db_handle.remove(&key).is_none() {
            // In the event this is none -> the key didn't exist
            // so we add this to `except_for`
            except_for.add(idx);
        }
    });
    if except_for.no_failures() {
        return responses::OKAY.to_owned();
    } else if except_for.did_all_fail(howmany) {
        return responses::NOT_FOUND.to_owned();
    } else {
        return except_for.into_response();
    }
}

#[cfg(test)]
#[test]
fn test_kvengine_del_allfailed() {
    let db = CoreDB::new().unwrap();
    let action = DataGroup::new(vec!["DEL".to_owned(), "x".to_owned(), "y".to_owned()]);
    let r = del(&db, action);
    db.finish_db(true, true, true);
    let resp_should_be = responses::NOT_FOUND.to_owned();
    assert_eq!(resp_should_be, r);
}

#[cfg(test)]
#[test]
fn test_kvenegine_del_allokay() {
    let db = CoreDB::new().unwrap();
    let mut write_handle = db.acquire_write();
    assert!(write_handle
        .insert(
            "foo".to_owned(),
            coredb::Data::from_string(&"bar".to_owned()),
        )
        .is_none());
    assert!(write_handle
        .insert(
            "foo2".to_owned(),
            coredb::Data::from_string(&"bar2".to_owned()),
        )
        .is_none());
    drop(write_handle); // Drop the write lock
    let action = DataGroup::new(vec!["DEL".to_owned(), "foo".to_owned(), "foo2".to_owned()]);
    let r = del(&db, action);
    db.finish_db(true, true, true);
    assert_eq!(r, responses::OKAY.to_owned());
}

#[cfg(test)]
#[test]
fn test_kvenegine_del_exceptfor() {
    let db = CoreDB::new().unwrap();
    let mut write_handle = db.acquire_write();
    assert!(write_handle
        .insert(
            "foo".to_owned(),
            coredb::Data::from_string(&"bar2".to_owned())
        )
        .is_none());
    assert!(write_handle
        .insert(
            "foo3".to_owned(),
            coredb::Data::from_string(&"bar3".to_owned())
        )
        .is_none());
    // For us `foo2` is the missing key, which should fail to delete
    drop(write_handle); // Drop the write lock
    let action = DataGroup::new(vec![
        "DEL".to_owned(),
        "foo".to_owned(),
        "foo2".to_owned(),
        "foo3".to_owned(),
    ]);
    let r = del(&db, action);
    db.finish_db(true, true, true);
    let mut except_for = ExceptFor::new();
    except_for.add(1);
    let resp_should_be = except_for.into_response();
    assert_eq!(resp_should_be, r);
}
