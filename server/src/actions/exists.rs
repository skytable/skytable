/*
 * Created on Wed Aug 19 2020
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

//! # `EXISTS` queries
//! This module provides functions to work with `EXISTS` queries

use crate::corestore::table::DataModel;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::KVTable;
use crate::queryengine::ActionIter;
use crate::util::compiler;

action!(
    /// Run an `EXISTS` query
    fn exists(handle: &Corestore, con: &'a mut T, act: ActionIter<'a>) {
        err_if_len_is!(act, con, eq 0);
        let mut how_many_of_them_exist = 0usize;
        macro_rules! exists {
            ($engine:expr) => {{
                let encoding_is_okay = if $engine.kve_key_encoded() {
                    let encoder = $engine.kve_get_key_encoder();
                    act.as_ref().all(|k| encoder.is_ok(k))
                } else {
                    true
                };
                if compiler::likely(encoding_is_okay) {
                    act.for_each(|key| {
                        if $engine.kve_exists(key) {
                            how_many_of_them_exist += 1;
                        }
                    });
                    conwrite!(con, how_many_of_them_exist)?;
                } else {
                    compiler::cold_err(conwrite!(con, groups::ENCODING_ERROR))?;
                }
            }};
        }
        let tbl = get_tbl!(handle, con);
        match tbl.get_model_ref() {
            DataModel::KV(kve) => exists!(kve),
            DataModel::KVExtListmap(kve) => exists!(kve),
            #[allow(unreachable_patterns)]
            _ => conwrite!(con, groups::WRONG_MODEL)?,
        }
        Ok(())
    }
);
