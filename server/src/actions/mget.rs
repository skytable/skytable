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
use crate::kvengine::{encoding::ENCODING_LUT_ITER, KVTable};
use crate::queryengine::ActionIter;
use crate::resp::writer::TypedArrayWriter;
use crate::util::compiler;

action!(
    /// Run an `MGET` query
    ///
    fn mget(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter<'a>) {
        crate::err_if_len_is!(act, con, eq 0);
        let kve = kve!(con, handle);
        let encoding_is_okay = ENCODING_LUT_ITER[kve.kve_key_encoded()](act.as_ref());
        if compiler::likely(encoding_is_okay) {
            let mut writer = unsafe {
                // SAFETY: We are getting the value type ourselves
                TypedArrayWriter::new(con, kve.get_vt(), act.len())
            }
            .await?;
            for key in act {
                match kve.get_cloned_unchecked(key) {
                    Some(v) => writer.write_element(&v).await?,
                    None => writer.write_null().await?,
                }
            }
        } else {
            compiler::cold_err(conwrite!(con, groups::ENCODING_ERROR))?;
        }
        Ok(())
    }
);
