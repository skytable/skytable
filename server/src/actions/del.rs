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

//! # `DEL` queries
//! This module provides functions to work with `DEL` queries

use crate::{
    corestore::table::DataModel, dbnet::connection::prelude::*,
    kvengine::encoding::ENCODING_LUT_ITER, util::compiler,
};

action!(
    /// Run a `DEL` query
    ///
    /// Do note that this function is blocking since it acquires a write lock.
    /// It will write an entire datagroup, for this `del` action
    fn del(handle: &Corestore, con: &'a mut T, act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |size| size != 0)?;
        let table = get_tbl_ref!(handle, con);
        macro_rules! remove {
            ($engine:expr) => {{
                let encoding_is_okay = ENCODING_LUT_ITER[$engine.is_key_encoded()](act.as_ref());
                if compiler::likely(encoding_is_okay) {
                    let done_howmany: Option<usize>;
                    {
                        if registry::state_okay() {
                            let mut many = 0;
                            act.for_each(|key| {
                                many += $engine.remove_unchecked(key) as usize;
                            });
                            done_howmany = Some(many);
                        } else {
                            done_howmany = None;
                        }
                    }
                    if let Some(done_howmany) = done_howmany {
                        con.write_usize(done_howmany).await?;
                    } else {
                        con._write_raw(P::RCODE_SERVER_ERR).await?;
                    }
                } else {
                    return util::err(P::RCODE_ENCODING_ERROR);
                }
            }};
        }
        match table.get_model_ref() {
            DataModel::KV(kve) => {
                remove!(kve)
            }
            DataModel::KVExtListmap(kvlmap) => {
                remove!(kvlmap)
            }
            #[allow(unreachable_patterns)]
            _ => return util::err(P::RSTRING_WRONG_MODEL),
        }
        Ok(())
    }
);
