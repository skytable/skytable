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

use crate::{
    corestore::{table::DataModel, SharedSlice},
    dbnet::prelude::*,
};

const DEFAULT_COUNT: usize = 10;

action!(
    /// Run an `LSKEYS` query
    fn lskeys(handle: &crate::corestore::Corestore, con: &mut Connection<C, P>, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |size| size < 4)?;
        let (table, count) = if act.is_empty() {
            (get_tbl!(handle, con), DEFAULT_COUNT)
        } else if act.len() == 1 {
            // two args, could either be count or an entity
            let nextret = unsafe { act.next_unchecked() };
            if unsafe { ucidx!(nextret, 0) }.is_ascii_digit() {
                // noice, this is a number; let's try to parse it
                let count = if let Ok(cnt) = String::from_utf8_lossy(nextret).parse::<usize>() {
                    cnt
                } else {
                    return util::err(P::RCODE_WRONGTYPE_ERR);
                };
                (get_tbl!(handle, con), count)
            } else {
                // sigh, an entity
                let entity = handle_entity!(con, nextret);
                (get_tbl!(&entity, handle, con), DEFAULT_COUNT)
            }
        } else {
            // an entity and a count, gosh this fella is really trying us
            let entity_ret = unsafe { act.next().unsafe_unwrap() };
            let count_ret = unsafe { act.next().unsafe_unwrap() };
            let entity = handle_entity!(con, entity_ret);
            let count = if let Ok(cnt) = String::from_utf8_lossy(count_ret).parse::<usize>() {
                cnt
            } else {
                return util::err(P::RCODE_WRONGTYPE_ERR);
            };
            (get_tbl!(&entity, handle, con), count)
        };
        let tsymbol = match table.get_model_ref() {
            DataModel::KV(kv) => kv.get_value_tsymbol(),
            DataModel::KVExtListmap(kv) => kv.get_value_tsymbol(),
        };
        let items: Vec<SharedSlice> = match table.get_model_ref() {
            DataModel::KV(kv) => kv.get_inner_ref().get_keys(count),
            DataModel::KVExtListmap(kv) => kv.get_inner_ref().get_keys(count),
        };
        con.write_typed_non_null_array_header(items.len(), tsymbol)
            .await?;
        for key in items {
            con.write_typed_non_null_array_element(&key).await?;
        }
        Ok(())
    }
);
