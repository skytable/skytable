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
use crate::resp::BytesWrapper;

action! {
    fn pop(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        err_if_len_is!(act, con, not 1);
        let key = unsafe {
            // SAFETY: We have checked for there to be one arg
            act.next().unsafe_unwrap()
        };
        if registry::state_okay() {
            match kve!(con, handle).pop(key) {
                Ok(Some((_key, val))) => conwrite!(con, BytesWrapper(val.into_inner()))?,
                Ok(None) => conwrite!(con, groups::NIL)?,
                Err(()) => conwrite!(con, groups::ENCODING_ERROR)?,
            }
        } else {
            conwrite!(con, groups::SERVER_ERR)?;
        }
        Ok(())
    }
}
