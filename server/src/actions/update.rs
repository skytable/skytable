/*
 * Created on Mon Aug 17 2020
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

//! # `UPDATE` queries
//! This module provides functions to work with `UPDATE` queries
//!

use crate::coredb::{self};
use crate::dbnet::connection::prelude::*;
use crate::protocol::responses;
use coredb::Data;
use std::hint::unreachable_unchecked;

/// Run an `UPDATE` query
pub async fn update<T, Strm>(
    handle: &crate::coredb::CoreDB,
    con: &mut T,
    act: Vec<String>,
) -> std::io::Result<()>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    let howmany = act.len() - 1;
    if howmany != 2 {
        // There should be exactly 2 arguments
        return con.write_response(&**responses::groups::ACTION_ERR).await;
    }
    let mut it = act.into_iter().skip(1);
    let did_we = {
        if handle.is_poisoned() {
            None
        } else {
            let writer = handle.get_ref();
            if writer.true_if_update(
                Data::from(it.next().unwrap_or_else(|| unsafe {
                    // UNSAFE(@ohsayan): We've already checked that the action contains exactly
                    // two arguments (excluding the action itself). So, this branch won't ever be reached
                    unreachable_unchecked()
                })),
                Data::from_string(it.next().unwrap_or_else(|| unsafe {
                    // UNSAFE(@ohsayan): We've already checked that the action contains exactly
                    // two arguments (excluding the action itself). So, this branch won't ever be reached
                    unreachable_unchecked()
                })),
            ) {
                Some(true)
            } else {
                Some(false)
            }
        }
    };
    if let Some(did_we) = did_we {
        if did_we {
            con.write_response(&**responses::groups::OKAY).await?;
        } else {
            con.write_response(&**responses::groups::NIL).await?;
        }
    } else {
        con.write_response(&**responses::groups::SERVER_ERR).await?;
    }
    Ok(())
}
