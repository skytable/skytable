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

use crate::coredb::CoreDB;
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resputil::GroupBegin;
use libtdb::terrapipe::RespCodes;
use libtdb::TResult;

/// Run a `DEL` query
///
/// Do note that this function is blocking since it acquires a write lock.
/// It will write an entire datagroup, for this `del` action
pub async fn del(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany == 0 {
        return con.write_response(responses::ACTION_ERR.to_owned()).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    con.write_response(GroupBegin(howmany)).await?;
    let mut keys = act.into_iter();
    let mut handle = handle.acquire_write(); // Get a write handle
    while let Some(key) = keys.next() {
        if handle.remove(&key).is_some() {
            con.write_response(RespCodes::Okay).await?;
        } else {
            con.write_response(RespCodes::NotFound).await?;
        }
    }
    // We're done here
    Ok(())
}
