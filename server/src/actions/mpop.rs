/*
 * Created on Wed Aug 11 2021
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

use crate::{
    corestore, dbnet::prelude::*, kvengine::encoding::ENCODING_LUT_ITER, queryengine::ActionIter,
    util::compiler,
};

action!(
    /// Run an MPOP action
    fn mpop(handle: &corestore::Corestore, con: &mut Connection<C, P>, act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len != 0)?;
        if registry::state_okay() {
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            let encoding_is_okay = ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
            if compiler::likely(encoding_is_okay) {
                con.write_typed_array_header(act.len(), kve.get_value_tsymbol())
                    .await?;
                for key in act {
                    match kve.pop_unchecked(key) {
                        Some(val) => con.write_typed_array_element(&val).await?,
                        None => con.write_typed_array_element_null().await?,
                    }
                }
            } else {
                return util::err(P::RCODE_ENCODING_ERROR);
            }
        } else {
            // don't begin the operation at all if the database is poisoned
            return util::err(P::RCODE_SERVER_ERR);
        }
        Ok(())
    }
);
