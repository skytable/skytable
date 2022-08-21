/*
 * Created on Tue Sep 07 2021
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

#[macro_use]
mod macros;
// modules
pub mod lget;
pub mod lmod;

use crate::{corestore::SharedSlice, dbnet::prelude::*, kvengine::LockedVec};

action! {
    /// Handle an `LSET` query for the list model
    /// Syntax: `LSET <listname> <values ...>`
    fn lset(handle: &Corestore, con: &mut Connection<C, P>, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len > 0)?;
        let listmap = handle.get_table_with::<P, KVEList>()?;
        let listname = unsafe { act.next_unchecked_bytes() };
        let list = listmap.get_inner_ref();
        if registry::state_okay() {
            let did = if let Some(entry) = list.fresh_entry(listname) {
                let v: Vec<SharedSlice> = act.map(SharedSlice::new).collect();
                entry.insert(LockedVec::new(v));
                true
            } else {
                false
            };
            con._write_raw(P::OKAY_OVW_BLUT[did]).await?
        } else {
            con._write_raw(P::RCODE_SERVER_ERR).await?
        }
        Ok(())
    }
}
