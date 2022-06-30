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

use crate::{
    corestore::table::DataModel, dbnet::connection::prelude::*,
    kvengine::encoding::ENCODING_LUT_ITER, queryengine::ActionIter, util::compiler,
};

action!(
    /// Run an `EXISTS` query
    fn exists(handle: &Corestore, con: &'a mut T, act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len != 0)?;
        let mut how_many_of_them_exist = 0usize;
        macro_rules! exists {
            ($engine:expr) => {{
                let encoding_is_okay = ENCODING_LUT_ITER[$engine.is_key_encoded()](act.as_ref());
                if compiler::likely(encoding_is_okay) {
                    act.for_each(|key| {
                        how_many_of_them_exist += $engine.exists_unchecked(key) as usize;
                    });
                    con.write_usize(how_many_of_them_exist).await?;
                } else {
                    return util::err(P::RCODE_ENCODING_ERROR);
                }
            }};
        }
        let tbl = get_tbl_ref!(handle, con);
        match tbl.get_model_ref() {
            DataModel::KV(kve) => exists!(kve),
            DataModel::KVExtListmap(kve) => exists!(kve),
            #[allow(unreachable_patterns)]
            _ => return util::err(P::RSTRING_WRONG_MODEL),
        }
        Ok(())
    }
);
