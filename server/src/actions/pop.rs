/*
 * Created on Mon Jun 14 2021
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

use crate::dbnet::connection::prelude::*;
use crate::resp::writer;
use crate::util::compiler;

action! {
    fn pop(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len == 1)?;
        let key = unsafe {
            // SAFETY: We have checked for there to be one arg
            act.next_unchecked()
        };
        if registry::state_okay() {
            let kve = handle.get_table_with::<KVE>()?;
            let tsymbol = kve.get_value_tsymbol();
            match kve.pop(key) {
                Ok(Some(val)) => unsafe {
                    // SAFETY: We have verified the tsymbol ourselves
                    writer::write_raw_mono(con, tsymbol, &val).await?
                },
                Ok(None) => conwrite!(con, groups::NIL)?,
                Err(()) => compiler::cold_err(conwrite!(con, groups::ENCODING_ERROR))?,
            }
        } else {
            conwrite!(con, groups::SERVER_ERR)?;
        }
        Ok(())
    }
}
