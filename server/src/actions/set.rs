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

use crate::corestore;
use crate::corestore::booltable::BytesNicheLUT;
use crate::dbnet::connection::prelude::*;
use crate::queryengine::ActionIter;
use corestore::Data;

const SET_NLUT: BytesNicheLUT =
    BytesNicheLUT::new(groups::ENCODING_ERROR, groups::OKAY, groups::OVERWRITE_ERR);

action!(
    /// Run a `SET` query
    fn set(handle: &crate::corestore::Corestore, con: &mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len == 2)?;
        if registry::state_okay() {
            let did_we = {
                let writer = handle.get_table_with::<KVEBlob>()?;
                match unsafe {
                    // UNSAFE(@ohsayan): This is completely safe as we've already checked
                    // that there are exactly 2 arguments
                    writer.set(
                        Data::copy_from_slice(act.next().unsafe_unwrap()),
                        Data::copy_from_slice(act.next().unsafe_unwrap()),
                    )
                } {
                    Ok(true) => Some(true),
                    Ok(false) => Some(false),
                    Err(()) => None,
                }
            };
            con._write_raw(SET_NLUT[did_we]).await?;
        } else {
            con._write_raw(groups::SERVER_ERR).await?;
        }
        Ok(())
    }
);
