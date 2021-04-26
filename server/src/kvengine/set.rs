/*
 * Created on Fri Aug 14 2020
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

//! # `SET` queries
//! This module provides functions to work with `SET` queries

use crate::coredb::{self};
use crate::protocol::con::prelude::*;
use crate::protocol::responses;
use coredb::Data;

use std::collections::hash_map::Entry;
use std::hint::unreachable_unchecked;

/// Run a `SET` query
pub async fn set<T, Strm>(
    handle: &crate::coredb::CoreDB,
    con: &mut T,
    act: crate::protocol::ActionGroup,
) -> std::io::Result<()>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    let howmany = act.howmany();
    if howmany != 2 {
        // There should be exactly 2 arguments
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    let mut it = act.into_iter();
    let did_we = {
        if let Some(mut writer) = handle.acquire_write() {
            let writer = writer.get_mut_ref();
            if let Entry::Vacant(e) = writer.entry(it.next().unwrap_or_else(|| unsafe {
                // UNSAFE(@ohsayan): This is completely safe as we've already checked
                // that there are exactly 2 arguments
                unreachable_unchecked()
            })) {
                e.insert(Data::from_string(it.next().unwrap_or_else(|| unsafe {
                    // UNSAFE(@ohsayan): This is completely safe as we've already checked
                    // that there are exactly 2 arguments
                    unreachable_unchecked()
                })));
                Some(true)
            } else {
                Some(false)
            }
        } else {
            None
        }
    };
    if let Some(did_we) = did_we {
        if did_we {
            con.write_response(&**responses::fresp::R_OKAY).await?;
        } else {
            con.write_response(&**responses::fresp::R_OVERWRITE_ERR)
                .await?;
        }
    } else {
        con.write_response(&**responses::fresp::R_SERVER_ERR)
            .await?;
    }
    Ok(())
}
