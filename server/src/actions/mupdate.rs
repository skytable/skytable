/*
 * Created on Thu Aug 27 2020
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

use crate::corestore::Data;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::encoding::ENCODING_LUT_ITER_PAIR;
use crate::util::compiler;

action!(
    /// Run an `MUPDATE` query
    fn mupdate(handle: &crate::corestore::Corestore, con: &mut T, mut act: ActionIter<'a>) {
        let howmany = act.len();
        ensure_length(howmany, |size| size & 1 == 0 && size != 0)?;
        let kve = handle.get_table_with::<KVEBlob>()?;
        let encoding_is_okay = ENCODING_LUT_ITER_PAIR[kve.get_encoding_tuple()](&act);
        let done_howmany: Option<usize>;
        if compiler::likely(encoding_is_okay) {
            if registry::state_okay() {
                let mut didmany = 0;
                while let (Some(key), Some(val)) = (act.next(), act.next()) {
                    didmany += kve
                        .update_unchecked(Data::copy_from_slice(key), Data::copy_from_slice(val))
                        as usize;
                }
                done_howmany = Some(didmany);
            } else {
                done_howmany = None;
            }
            if let Some(done_howmany) = done_howmany {
                con.write_usize(done_howmany).await?;
            } else {
                return util::err(groups::SERVER_ERR);
            }
        } else {
            return util::err(groups::ENCODING_ERROR);
        }
        Ok(())
    }
);
