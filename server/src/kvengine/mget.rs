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

use crate::coredb::CoreDB;
use crate::dbnet::Con;
use crate::protocol::{responses, ActionGroup};
use crate::resp::{BytesWrapper, GroupBegin};
use bytes::Bytes;
use libtdb::terrapipe::RespCodes;
use libtdb::TResult;

/// Run an `MGET` query
///
pub async fn mget(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany == 0 {
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    // Write #<m>\n#<n>\n&<howmany>\n to the stream
    con.write_response(GroupBegin(howmany)).await?;
    let mut keys = act.into_iter();
    while let Some(key) = keys.next() {
        let res: Option<Bytes> = {
            let rhandle = handle.acquire_read();
            let reader = rhandle.get_ref();
            reader.get(&key).map(|b| b.get_blob().clone())
        };
        if let Some(value) = res {
            // Good, we got the value, write it off to the stream
            con.write_response(BytesWrapper(value)).await?;
        } else {
            // Ah, couldn't find that key
            con.write_response(RespCodes::NotFound).await?;
        }
    }
    drop(handle);
    Ok(())
}
