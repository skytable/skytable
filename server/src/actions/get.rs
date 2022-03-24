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

//! # `GET` queries
//! This module provides functions to work with `GET` queries

use crate::dbnet::connection::prelude::*;
use crate::resp::writer;
use crate::util::compiler;

action!(
    /// Run a `GET` query
    fn get(handle: &crate::corestore::Corestore, con: &mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len == 1)?;
        let kve = handle.get_table_with::<KVEBlob>()?;
        unsafe {
            match kve.get_cloned(act.next_unchecked()) {
                Ok(Some(val)) => writer::write_raw_mono(con, kve.get_value_tsymbol(), &val).await?,
                Err(_) => compiler::cold_err(conwrite!(con, groups::ENCODING_ERROR))?,
                Ok(_) => conwrite!(con, groups::NIL)?,
            }
        }
        Ok(())
    }
);
