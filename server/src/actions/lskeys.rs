/*
 * Created on Thu May 13 2021
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

use crate::corestore::memstore::DdlError;
use crate::dbnet::connection::prelude::*;
use crate::resp::writer::TypedArrayWriter;
use bytes::Bytes;

const DEFAULT_COUNT: usize = 10;

action!(
    /// Run an `LSKEYS` query
    fn lskeys(handle: &crate::corestore::Corestore, con: &mut T, mut act: ActionIter<'a>) {
        err_if_len_is!(act, con, gt 3);
        let (table, count) = if act.len() == 0 {
            (get_tbl!(handle, con), DEFAULT_COUNT)
        } else if act.len() == 1 {
            // two args, could either be count or an entity
            let nextret = unsafe { act.next().unsafe_unwrap() };
            if unsafe { nextret.get_unchecked(0) }.is_ascii_digit() {
                // noice, this is a number; let's try to parse it
                let count = if let Ok(cnt) = String::from_utf8_lossy(nextret).parse::<usize>() {
                    cnt
                } else {
                    return con.write_response(responses::groups::WRONGTYPE_ERR).await;
                };
                (get_tbl!(handle, con), count)
            } else {
                // sigh, an entity
                let entity = handle_entity!(con, nextret);
                (get_tbl!(entity, handle, con), DEFAULT_COUNT)
            }
        } else {
            // an entity and a count, gosh this fella is really trying us
            let entity_ret = unsafe { act.next().unsafe_unwrap() };
            let count_ret = unsafe { act.next().unsafe_unwrap() };
            let entity = handle_entity!(con, entity_ret);
            let count = if let Ok(cnt) = String::from_utf8_lossy(count_ret).parse::<usize>() {
                cnt
            } else {
                return con.write_response(responses::groups::WRONGTYPE_ERR).await;
            };
            (get_tbl!(entity, handle, con), count)
        };
        let kve = match table.get_kvstore() {
            Ok(kv) => kv,
            Err(DdlError::WrongModel) => return conwrite!(con, responses::groups::WRONG_MODEL),
            Err(_) => unsafe { impossible!() },
        };
        let items: Vec<Bytes> = kve.__get_inner_ref().get_keys(count);
        let tsymbol = kve.get_kt();
        let mut writer = unsafe {
            // SAFETY: We have checked kty ourselves
            TypedArrayWriter::new(con, tsymbol, items.len())
        }
        .await?;
        for key in items {
            writer.write_element(key).await?;
        }
        Ok(())
    }
);
