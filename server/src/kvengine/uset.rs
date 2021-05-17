/*
 * Created on Fri Sep 25 2020
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

use crate::coredb::Data;
use crate::dbnet::connection::prelude::*;
use crate::protocol::responses;

/// Run an `USET` query
///
/// This is like "INSERT or UPDATE"
pub async fn uset<T, Strm>(
    handle: &crate::coredb::CoreDB,
    con: &mut T,
    act: Vec<String>,
) -> std::io::Result<()>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    let howmany = act.len() - 1;
    if howmany & 1 == 1 || howmany == 0 {
        // An odd number of arguments means that the number of keys
        // is not the same as the number of values, we won't run this
        // action at all
        return con.write_response(&**responses::groups::ACTION_ERR).await;
    }
    let mut kviter = act.into_iter().skip(1);
    let failed = {
        if let Some(mut whandle) = handle.acquire_write() {
            let writer = whandle.get_mut_ref();
            while let (Some(key), Some(val)) = (kviter.next(), kviter.next()) {
                let _ = writer.insert(Data::from(key), Data::from(val));
            }
            drop(writer);
            drop(whandle);
            false
        } else {
            true
        }
    };
    if failed {
        con.write_response(&**responses::groups::SERVER_ERR).await
    } else {
        con.write_response(howmany / 2).await
    }
}
