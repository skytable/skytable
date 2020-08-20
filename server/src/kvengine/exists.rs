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
use crate::resputil::*;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::responses;
/// Run an `EXISTS` query
pub fn exists(handle: &CoreDB, act: DataGroup) -> Response {
    let howmany = act.len() - 1;
    if howmany == 0 {
        // No arguments? Come on!
        return responses::ARG_ERR.to_owned();
    }
    // Get a read lock
    let read_lock = handle.acquire_read();
    // Assume that half of the actions will fail
    let mut except_for = ExceptFor::with_space_for(howmany / 2);
    act.into_iter().skip(1).enumerate().for_each(|(idx, key)| {
        if !read_lock.contains_key(&key) {
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
mod tests {
    use super::*;
    use crate::coredb::{self, CoreDB};
    use corelib::de::DataGroup;
    use corelib::terrapipe::responses;
    #[cfg(test)]
    #[test]
    fn test_kvengine_exists_allfailed() {
        let db = CoreDB::new().unwrap();
        let action = DataGroup::new(vec!["EXISTS".to_owned(), "x".to_owned(), "y".to_owned()]);
        let r = exists(&db, action);
        db.finish_db(true, true, true);
        let resp_should_be = responses::NOT_FOUND.to_owned();
        assert_eq!(r, resp_should_be);
    }

    #[cfg(test)]
    #[test]
    fn test_kvengine_exists_allokay() {
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
        let action = DataGroup::new(vec![
            "EXISTS".to_owned(),
            "foo".to_owned(),
            "foo2".to_owned(),
        ]);
        let r = exists(&db, action);
        db.finish_db(true, true, true);
        assert_eq!(r, responses::OKAY.to_owned());
    }

    #[cfg(test)]
    #[test]
    fn test_kvengine_exists_exceptfor() {
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
        // For us `foo2` is the missing key,
        drop(write_handle); // Drop the write lock
        let action = DataGroup::new(vec![
            "EXISTS".to_owned(),
            "foo".to_owned(),
            "foo2".to_owned(),
            "foo3".to_owned(),
        ]);
        let r = exists(&db, action);
        db.finish_db(true, true, true);
        let mut exceptfor = ExceptFor::new();
        exceptfor.add(1);
        assert_eq!(exceptfor.into_response(), r);
    }
}
