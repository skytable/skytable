/*
 * Created on Tue Oct 13 2020
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
use crate::diskstore::snapshot::SnapshotEngine;
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resp::GroupBegin;
use libtdb::terrapipe::RespCodes;
use libtdb::TResult;
use std::hint::unreachable_unchecked;

/// Create a snapshot
///
pub async fn mksnap(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    if act.howmany() == 0 {
        if !handle.is_snapshot_enabled() {
            // Since snapshotting is disabled, we can't create a snapshot!
            // We'll just return an error returning the same
            let error = "err-snapshot-disabled";
            con.write_response(GroupBegin(1)).await?;
            let error = RespCodes::OtherError(Some(error.to_string()));
            return con.write_response(error).await;
        }
        // We will just follow the standard convention of creating snapshots
        let mut was_engine_error = false;
        let mut outerror = None;
        let mut engine_was_busy = false;
        {
            let snaphandle = handle.snapcfg.clone();
            let snapstatus = (*snaphandle)
                .as_ref()
                .unwrap_or_else(|| unsafe { unreachable_unchecked() });
            let snapengine = SnapshotEngine::new(snapstatus.max, &handle, None);
            if snapengine.is_err() {
                was_engine_error = true;
            } else {
                if snapstatus.is_busy() {
                    engine_was_busy = true;
                } else {
                    let mut snapengine =
                        snapengine.unwrap_or_else(|_| unsafe { unreachable_unchecked() });

                    outerror = snapengine.mksnap();
                }
            }
        }
        if was_engine_error {
            return con
                .write_response(responses::fresp::R_SERVER_ERR.to_owned())
                .await;
        }
        if engine_was_busy {
            con.write_response(GroupBegin(1)).await?;
            let error = RespCodes::OtherError(Some("err-snapshot-busy".to_owned()));
            return con.write_response(error).await;
        }
        if let Some(val) = outerror {
            if val {
                // Snapshotting succeeded, return Okay
                return con
                    .write_response(responses::fresp::R_OKAY.to_owned())
                    .await;
            } else {
                // Nope, something happened while creating a snapshot
                // return a server error
                return con
                    .write_response(responses::fresp::R_SERVER_ERR.to_owned())
                    .await;
            }
        } else {
            // We shouldn't ever reach here if all our logic is correct
            // but if we do, something is wrong with the runtime
            con.write_response(GroupBegin(1)).await?;
            let error = RespCodes::OtherError(Some("err-access-after-termsig".to_owned()));
            return con.write_response(error).await;
        }
    } else {
        return con
            .write_response(responses::fresp::R_ACTION_ERR.to_owned())
            .await;
    }
}
