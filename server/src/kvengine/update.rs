/*
 * Created on Mon Aug 17 2020
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

//! # `UPDATE` queries
//! This module provides functions to work with `UPDATE` queries
//!
use crate::coredb::{self, CoreDB};
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resp::GroupBegin;
use coredb::Data;
use libtdb::TResult;
use std::collections::hash_map::Entry;
use std::hint::unreachable_unchecked;

/// Run an `UPDATE` query
pub async fn update(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany != 2 {
        // There should be exactly 2 arguments
        return con.write_response(responses::ACTION_ERR.to_owned()).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    // It is howmany/2 since we will be writing 1 response
    con.write_response(GroupBegin(1)).await?;
    let mut it = act.into_iter();
    let did_we = {
        let mut whandle = handle.acquire_write();
        if let Entry::Occupied(mut e) = whandle.entry(
            it.next()
                .unwrap_or_else(|| unsafe { unreachable_unchecked() }),
        ) {
            e.insert(Data::from_string(
                it.next()
                    .unwrap_or_else(|| unsafe { unreachable_unchecked() }),
            ));
            true
        } else {
            false
        }
    };
    if did_we {
        con.write_response(responses::OKAY.to_owned()).await?;
    } else {
        con.write_response(responses::NIL.to_owned()).await?;
    }
    #[cfg(debug_assertions)]
    {
        handle.print_debug_table();
    }
    Ok(())
}
