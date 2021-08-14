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
use crate::queryengine::ActionIter;
use crate::resp::writer::TypedArrayWriter;

action!(
    /// Run an `MGET` query
    ///
    fn mget(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter) {
        crate::err_if_len_is!(act, con, eq 0);
        let kve = kve!(con, handle);
        let mut writer = unsafe {
            // SAFETY: We are getting the value type ourselves
            TypedArrayWriter::new(con, kve.get_vt())
        };
        // write len
        writer.write_length(act.len()).await?;
        for key in act {
            match kve.get_cloned(&key) {
                Ok(Some(v)) => writer.write_element(&v).await?,
                Ok(None) => writer.write_nil().await?,
                Err(_) => writer.write_encoding_error().await?,
            }
        }
        Ok(())
    }
);
