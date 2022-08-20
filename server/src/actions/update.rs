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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # `UPDATE` queries
//! This module provides functions to work with `UPDATE` queries
//!

use crate::{corestore::SharedSlice, dbnet::connection::prelude::*};

action!(
    /// Run an `UPDATE` query
    fn update(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len == 2)?;
        if registry::state_okay() {
            let did_we = {
                let writer = handle.get_table_with::<P, KVEBlob>()?;
                match unsafe {
                    // UNSAFE(@ohsayan): This is completely safe as we've already checked
                    // that there are exactly 2 arguments
                    writer.update(
                        SharedSlice::new(act.next_unchecked()),
                        SharedSlice::new(act.next_unchecked()),
                    )
                } {
                    Ok(true) => Some(true),
                    Ok(false) => Some(false),
                    Err(()) => None,
                }
            };
            con._write_raw(P::UPDATE_NLUT[did_we]).await?;
        } else {
            return util::err(P::RCODE_SERVER_ERR);
        }
        Ok(())
    }
);
