/*
 * Created on Mon Aug 17 2020
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

//! # `UPDATE` queries
//! This module provides functions to work with `UPDATE` queries
//!
use crate::coredb::{self, CoreDB};
use crate::dbnet::Con;
use crate::protocol::{responses, ActionGroup};
use coredb::Data;
use libtdb::TResult;
use std::collections::hash_map::Entry;
use std::hint::unreachable_unchecked;

/// Run an `UPDATE` query
pub async fn update(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany != 2 {
        // There should be exactly 2 arguments
        return con.write_response(&**responses::fresp::R_ACTION_ERR).await;
    }
    let mut it = act.into_iter();
    let did_we = {
        if let Some(mut whandle) = handle.acquire_write() {
            let writer = whandle.get_mut_ref();
            if let Entry::Occupied(mut e) = writer.entry(
                it.next()
                    .unwrap_or_else(|| unsafe { unreachable_unchecked() }),
            ) {
                e.insert(Data::from_string(
                    it.next()
                        .unwrap_or_else(|| unsafe { unreachable_unchecked() }),
                ));
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
            con.write_response(&**responses::fresp::R_NIL).await?;
        }
    } else {
        con.write_response(&**responses::fresp::R_SERVER_ERR)
            .await?;
    }
    Ok(())
}
