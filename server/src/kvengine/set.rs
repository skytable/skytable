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

use crate::coredb::{self, CoreDB};
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resp::GroupBegin;
use libtdb::terrapipe::RespCodes;
use libtdb::TResult;
use std::collections::hash_map::Entry;

/// Run a `SET` query
pub async fn set(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany & 1 == 1 {
        // An odd number of arguments means that the number of keys
        // is not the same as the number of values, we won't run this
        // action at all
        return con.write_response(responses::ACTION_ERR.to_owned()).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    // It is howmany/2 since we will be writing howmany/2 number of responses
    con.write_response(GroupBegin(howmany / 2)).await?;
    let mut kviter = act.into_iter();
    while let (Some(key), Some(val)) = (kviter.next(), kviter.next()) {
        let was_done = {
            let mut whandle = handle.acquire_write();
            let res = if let Entry::Vacant(v) = whandle.entry(key) {
                let _ = v.insert(coredb::Data::from_string(val));
                true
            } else {
                false
            };
            drop(whandle);
            res
        };
        if was_done {
            con.write_response(RespCodes::Okay).await?;
        } else {
            con.write_response(RespCodes::OverwriteError).await?;
        }
    }
    #[cfg(debug_assertions)]
    {
        handle.print_debug_table();
    }
    Ok(())
}
