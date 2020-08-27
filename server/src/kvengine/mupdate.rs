/*
 * Created on Thu Aug 27 2020
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

use crate::coredb::{self, CoreDB};
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resp::GroupBegin;
use libtdb::TResult;
use std::collections::hash_map::Entry;

// Run an `MUPDATE` query
pub async fn mupdate(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany & 1 == 1 {
        // An odd number of arguments means that the number of keys
        // is not the same as the number of values, we won't run this
        // action at all
        return con.write_response(responses::ACTION_ERR.to_owned()).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    // It is howmany/2 since we will be writing howmany/2 number of responses
    con.write_response(GroupBegin(1)).await?;
    let mut kviter = act.into_iter();
    let mut done_howmany = 0usize;
    {
        let mut whandle = handle.acquire_write();
        while let (Some(key), Some(val)) = (kviter.next(), kviter.next()) {
            if let Entry::Occupied(mut v) = whandle.entry(key) {
                let _ = v.insert(coredb::Data::from_string(val));
                done_howmany += 1;
            }
        }
        drop(whandle);
    }
    con.write_response(done_howmany).await?;
    #[cfg(debug_assertions)]
    {
        handle.print_debug_table();
    }
    Ok(())
}
