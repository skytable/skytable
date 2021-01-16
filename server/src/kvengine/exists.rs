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

//! # `EXISTS` queries
//! This module provides functions to work with `EXISTS` queries

use crate::coredb::CoreDB;
use crate::dbnet::Con;
use crate::protocol::{responses, ActionGroup};
use crate::resp::GroupBegin;
use libtdb::TResult;

/// Run an `EXISTS` query
pub async fn exists(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany == 0 {
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    // Write #<m>\n#<n>\n&1\n to the stream
    con.write_response(GroupBegin(1)).await?;
    let mut how_many_of_them_exist = 0usize;
    {
        let rhandle = handle.acquire_read();
        let cmap = rhandle.get_ref();
        act.into_iter().for_each(|key| {
            if cmap.contains_key(&key) {
                how_many_of_them_exist += 1;
            }
        });
        drop(cmap);
        drop(rhandle);
    }
    con.write_response(how_many_of_them_exist).await?;
    Ok(())
}
