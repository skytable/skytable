/*
 * Created on Wed Aug 19 2020
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

//! # `DEL` queries
//! This module provides functions to work with `DEL` queries

use crate::coredb::CoreDB;
use crate::dbnet::Con;
use crate::protocol::{responses, ActionGroup};
use crate::resp::GroupBegin;
use libtdb::TResult;

/// Run a `DEL` query
///
/// Do note that this function is blocking since it acquires a write lock.
/// It will write an entire datagroup, for this `del` action
pub async fn del(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany == 0 {
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    con.write_response(GroupBegin(1)).await?;
    let done_howmany: Option<usize>;
    {
        if let Some(mut whandle) = handle.acquire_write() {
            let mut many = 0;
            let cmap = (*whandle).get_mut_ref();
            act.into_iter().for_each(|key| {
                if cmap.remove(&key).is_some() {
                    many += 1
                }
            });
            drop(cmap);
            drop(whandle);
            done_howmany = Some(many);
        } else {
            done_howmany = None;
        }
    }
    if let Some(done_howmany) = done_howmany {
        con.write_response(done_howmany).await
    } else {
        con.write_response(&**responses::fresp::R_SERVER_ERR).await
    }
}
