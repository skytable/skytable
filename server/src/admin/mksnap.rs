/*
 * Created on Tue Oct 13 2020
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

use crate::coredb::CoreDB;
use crate::dbnet::Con;
use crate::diskstore;
use crate::diskstore::snapshot::SnapshotEngine;
use crate::diskstore::snapshot::DIR_SNAPSHOT;
use crate::protocol::{responses, ActionGroup};
use crate::resp::GroupBegin;
use libsky::terrapipe::RespCodes;
use libsky::TResult;
use std::hint::unreachable_unchecked;
use std::path::{Component, PathBuf};

/// Create a snapshot
///
pub async fn mksnap(handle: &CoreDB, con: &mut Con<'_>, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany == 0 {
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
        let mut snap_result = None;
        let mut engine_was_busy = false;
        {
            let snaphandle = handle.snapcfg.clone();
            let snapstatus = (*snaphandle).as_ref().unwrap_or_else(|| unsafe {
                // UNSAFE(@ohsayan) This is safe as we've already checked
                // if snapshots are enabled or not with `is_snapshot_enabled`
                unreachable_unchecked()
            });
            let snapengine = SnapshotEngine::new(snapstatus.max, &handle, None);
            if snapengine.is_err() {
                was_engine_error = true;
            } else {
                if snapstatus.is_busy() {
                    engine_was_busy = true;
                } else {
                    let mut snapengine = snapengine.unwrap_or_else(|_| unsafe {
                        // UNSAFE(@ohsayan) This is safe as we've already checked
                        // if snapshots are enabled or not with `is_snapshot_enabled`
                        unreachable_unchecked()
                    });

                    snap_result = snapengine.mksnap();
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
        if let Some(succeeded) = snap_result {
            if succeeded {
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
        if howmany == 1 {
            // This means that the user wants to create a 'named' snapshot
            let snapname = act
                .get_ref()
                .get(1)
                .unwrap_or_else(|| unsafe {
                    // UNSAFE(@ohsayan): We've already checked that the action
                    // contains a second argument, so this can't be reached  
                    unreachable_unchecked()
                });
            let mut path = PathBuf::from(DIR_SNAPSHOT);
            path.push("remote");
            path.push(snapname.to_owned() + ".snapshot");
            let illegal_snapshot = path
                .components()
                .filter(|dir| {
                    // Sanitize snapshot name, to avoid directory traversal attacks
                    // If the snapshot name has any root directory or parent directory, then
                    // we'll allow it to pass through this adaptor.
                    // As a result, this iterator will give us a count of the 'bad' components
                    dir == &Component::RootDir || dir == &Component::ParentDir
                })
                .count()
                != 0;
            if illegal_snapshot {
                con.write_response(GroupBegin(1)).await?;
                return con
                    .write_response(RespCodes::OtherError(Some(
                        "err-invalid-snapshot-name".to_owned(),
                    )))
                    .await;
            }
            let failed;
            {
                match diskstore::flush_data(&path, &handle.acquire_read().get_ref()) {
                    Ok(_) => failed = false,
                    Err(e) => {
                        log::error!("Error while creating snapshot: {}", e);
                        failed = true;
                    }
                }
            }
            if failed {
                return con
                    .write_response(responses::fresp::R_SERVER_ERR.to_owned())
                    .await;
            } else {
                return con
                    .write_response(responses::fresp::R_OKAY.to_owned())
                    .await;
            }
        } else {
            return con
                .write_response(responses::fresp::R_ACTION_ERR.to_owned())
                .await;
        }
    }
}