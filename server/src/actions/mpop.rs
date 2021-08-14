/*
 * Created on Wed Aug 11 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use crate::corestore;
use crate::dbnet::connection::prelude::*;
use crate::protocol::responses;
use crate::queryengine::ActionIter;
use crate::resp::writer::FlatArrayWriter;
use crate::util::compiler;

action!(
    /// Run an MPOP action
    fn mpop(handle: &corestore::Corestore, con: &mut T, act: ActionIter) {
        err_if_len_is!(act, con, eq 0);
        if registry::state_okay() {
            let kve = kve!(con, handle);
            let encoding_is_okay = if kve.needs_key_encoding() {
                true
            } else {
                let encoder = kve.get_key_encoder();
                act.as_ref().iter().all(|k| encoder.is_ok(k))
            };
            if compiler::likely(encoding_is_okay) {
                let mut writer = unsafe {
                    // SAFETY: We have verified the tsymbol ourselves
                    FlatArrayWriter::new(con, kve.get_vt(), act.len())
                }
                .await?;
                for key in act {
                    if registry::state_okay() {
                        match kve.pop_unchecked(&key) {
                            Some((_key, val)) => writer.write_element(val).await?,
                            None => writer.write_nil().await?,
                        }
                    } else {
                        // we keep this check just in case the server fails in-between running a
                        // pop operation
                        writer.write_server_error().await?;
                    }
                }
            } else {
                conwrite!(con, groups::ENCODING_ERROR)?;
            }
        } else {
            // don't begin the operation at all if the database is poisoned
            return con.write_response(responses::groups::SERVER_ERR).await;
        }
        Ok(())
    }
);
