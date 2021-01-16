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
use crate::dbnet::Con;
use crate::protocol::{responses, ActionGroup};
use crate::resp::GroupBegin;
use libtdb::TResult;
use std::collections::hash_map::Entry;

/// Run an `MUPDATE` query
pub async fn mupdate(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany & 1 == 1 || howmany == 0 {
        // An odd number of arguments means that the number of keys
        // is not the same as the number of values, we won't run this
        // action at all
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    // It is howmany/2 since we will be writing howmany/2 number of responses
    con.write_response(GroupBegin(1)).await?;
    let mut kviter = act.into_iter();
    let done_howmany: Option<usize>;
    {
        if let Some(mut whandle) = handle.acquire_write() {
            let writer = whandle.get_mut_ref();
            let mut didmany = 0;
            while let (Some(key), Some(val)) = (kviter.next(), kviter.next()) {
                if let Entry::Occupied(mut v) = writer.entry(key) {
                    let _ = v.insert(coredb::Data::from_string(val));
                    didmany += 1;
                }
            }
            drop(writer);
            drop(whandle);
            done_howmany = Some(didmany);
        } else {
            done_howmany = None;
        }
    }
    if let Some(done_howmany) = done_howmany {
        return con.write_response(done_howmany as usize).await;
    } else {
        return con.write_response(&**responses::fresp::R_SERVER_ERR).await;
    }
}
