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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # `SET` queries
//! This module provides functions to work with `SET` queries

use crate::{corestore::SharedSlice, dbnet::connection::prelude::*, queryengine::ActionIter};

action!(
    /// Run a `SET` query
    fn set(handle: &crate::corestore::Corestore, con: &mut T, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len == 2)?;
        if registry::state_okay() {
            let did_we = {
                let writer = handle.get_table_with::<P, KVEBlob>()?;
                match unsafe {
                    // UNSAFE(@ohsayan): This is completely safe as we've already checked
                    // that there are exactly 2 arguments
                    writer.set(
                        SharedSlice::new(act.next().unsafe_unwrap()),
                        SharedSlice::new(act.next().unsafe_unwrap()),
                    )
                } {
                    Ok(true) => Some(true),
                    Ok(false) => Some(false),
                    Err(()) => None,
                }
            };
            con._write_raw(P::SET_NLUT[did_we]).await?;
        } else {
            con._write_raw(P::RCODE_SERVER_ERR).await?;
        }
        Ok(())
    }
);
