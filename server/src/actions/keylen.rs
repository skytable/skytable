/*
 * Created on Sun Sep 27 2020
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

use crate::corestore::table::DataModel;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::KVTable;

action!(
    /// Run a `KEYLEN` query
    ///
    /// At this moment, `keylen` only supports a single key
    fn keylen(handle: &crate::corestore::Corestore, con: &mut T, mut act: ActionIter<'a>) {
        err_if_len_is!(act, con, not 1);
        let res: Option<usize> = {
            let tbl = get_tbl!(handle, con);
            let key = unsafe { act.next_unchecked() };
            match tbl.get_model_ref() {
                DataModel::KV(kv) => kv.kve_keylen(key),
                DataModel::KVExtListmap(kv) => kv.kve_keylen(key),
            }
        };
        if let Some(value) = res {
            // Good, we got the key's length, write it off to the stream
            con.write_response(value).await?;
        } else {
            // Ah, couldn't find that key
            con.write_response(responses::groups::NIL).await?;
        }
        Ok(())
    }
);
