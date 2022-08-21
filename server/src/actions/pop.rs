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

use crate::dbnet::prelude::*;

action! {
    fn pop(handle: &Corestore, con: &mut Connection<C, P>, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len == 1)?;
        let key = unsafe {
            // SAFETY: We have checked for there to be one arg
            act.next_unchecked()
        };
        if registry::state_okay() {
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            match kve.pop(key) {
                Ok(Some(val)) => con.write_mono_length_prefixed_with_tsymbol(
                    &val, kve.get_value_tsymbol()
                ).await?,
                Ok(None) => return util::err(P::RCODE_NIL),
                Err(()) => return util::err(P::RCODE_ENCODING_ERROR),
            }
        } else {
            return util::err(P::RCODE_SERVER_ERR);
        }
        Ok(())
    }
}
