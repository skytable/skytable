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

use crate::dbnet::connection::prelude::*;
use crate::kvengine::encoding::ENCODING_LUT_ITER;
use crate::queryengine::ActionIter;
use crate::util::compiler;

action!(
    /// Run an `MGET` query
    ///
    fn mget(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter<'a>) {
        ensure_length(act.len(), |size| size != 0)?;
        let kve = handle.get_table_with::<KVEBlob>()?;
        let encoding_is_okay = ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
        if compiler::likely(encoding_is_okay) {
            con.write_typed_array_header(act.len(), kve.get_value_tsymbol())
                .await?;
            for key in act {
                match kve.get_cloned_unchecked(key) {
                    Some(v) => con.write_typed_array_element(&v).await?,
                    None => con.write_typed_array_element_null().await?,
                }
            }
        } else {
            return util::err(groups::ENCODING_ERROR);
        }
        Ok(())
    }
);
