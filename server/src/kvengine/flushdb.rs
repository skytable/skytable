/*
 * Created on Thu Sep 24 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

use crate::dbnet::connection::prelude::*;
use crate::protocol::responses;

/// Delete all the keys in the database
pub async fn flushdb<T, Strm>(
    handle: &crate::coredb::CoreDB,
    con: &mut T,
    act: Vec<String>,
) -> std::io::Result<()>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    crate::err_if_len_is!(act, con, != 0);
    let failed;
    {
        if let Some(mut table) = handle.acquire_write() {
            table.get_mut_ref().clear();
            failed = false;
        } else {
            failed = true;
        }
    }
    if failed {
        con.write_response(&**responses::groups::SERVER_ERR).await
    } else {
        con.write_response(&**responses::groups::OKAY).await
    }
}
