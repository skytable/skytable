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

use crate::corestore::booltable::BoolTable;
use crate::corestore::Data;
use crate::dbnet::connection::prelude::*;
use crate::util::compiler;

const UPDATE_BOOLTABLE: BoolTable = BoolTable::new(groups::OKAY, groups::NIL);

action!(
    /// Run an `UPDATE` query
    fn update(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        err_if_len_is!(act, con, not 2);
        if registry::state_okay() {
            let did_we = {
                let writer = kve!(con, handle);
                match unsafe {
                    // UNSAFE(@ohsayan): This is completely safe as we've already checked
                    // that there are exactly 2 arguments
                    writer.update(
                        Data::copy_from_slice(act.next_unchecked()),
                        Data::copy_from_slice(act.next_unchecked()),
                    )
                } {
                    Ok(true) => Some(true),
                    Ok(false) => Some(false),
                    Err(()) => None,
                }
            };
            if let Some(did_we) = did_we {
                con.write_response(UPDATE_BOOLTABLE[did_we]).await?;
            } else {
                compiler::cold_err(con.write_response(responses::groups::ENCODING_ERROR)).await?;
            }
        } else {
            conwrite!(con, groups::SERVER_ERR)?;
        }
        Ok(())
    }
);
