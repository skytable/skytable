/*
 * Created on Thu Sep 24 2020
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
use crate::resp::GroupBegin;
use libtdb::TResult;

/// Get the number of keys in the database
pub async fn dbsize(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    if act.howmany() != 0 {
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    let len;
    {
        len = handle.acquire_read().get_ref().len();
    }
    con.write_response(GroupBegin(1)).await?;
    con.write_response(len).await?;
    Ok(())
}
