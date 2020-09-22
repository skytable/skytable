/*
 * Created on Mon Sep 21 2020
 *
 * This file is a part of TerrabaseDB
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

//! # Strong Actions
//! Strong actions are like "do all" or "fail all" actions, built specifically for
//! multiple keys. So let's say you used `SSET` instead of `MSET` for setting keys:
//! what'd be the difference?
//! In this case, if all the keys are non-existing, which is a requirement for `MSET`,
//! only then would the keys be set. That is, only if all the keys can be set, will the action
//! run and return code `0` - otherwise the action won't do anything and return an overwrite error.  
//! Do note that this isn't the same as the gurantees provided by ACID transactions

use crate::coredb::{CoreDB, Data};
use crate::protocol::{responses, ActionGroup, Connection};
use libtdb::TResult;
use std::hint::unreachable_unchecked;

/// Run an `SSET` query
///
/// This either returns `Okay` if all the keys were set, or it returns an
/// `Overwrite Error` or code `2`
pub async fn sset(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany & 1 == 1 || howmany == 0 {
        return con
            .write_response(responses::fresp::R_ACTION_ERR.to_owned())
            .await;
    }
    let mut failed = false;
    {
        // We use this additional scope to tell the compiler that the write lock
        // doesn't go beyond the scope of this function - and is never used across
        // an await: cause, the compiler ain't as smart as we are ;)
        let mut key_iter = act
            .get_ref()
            .get(1..)
            .unwrap_or_else(|| unsafe { unreachable_unchecked() })
            .iter();
        let mut whandle = handle.acquire_write();
        let mut_table = whandle.get_mut_ref();
        while let Some(key) = key_iter.next() {
            if mut_table.contains_key(key.as_str()) {
                // With one of the keys existing - this action can't clearly be done
                // So we'll set `failed` to true and ensure that we check this while
                // writing a response back to the client
                failed = true;
                break;
            }
            // Skip the next value that is coming our way, as we don't need it
            // right now
            let _ = key_iter
                .next()
                .unwrap_or_else(|| unsafe { unreachable_unchecked() });
        }
        if !failed {
            // Since the failed flag is false, none of the keys existed
            // So we can safely set the keys
            let mut iter = act.into_iter();
            while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                if mut_table.insert(key, Data::from_string(value)).is_some() {
                    // Tell the compiler that this will never be the case
                    unsafe { unreachable_unchecked() }
                }
            }
        }
    }
    if failed {
        con.write_response(responses::fresp::R_OVERWRITE_ERR.to_owned())
            .await
    } else {
        con.write_response(responses::fresp::R_OKAY.to_owned())
            .await
    }
}
