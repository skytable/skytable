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

use crate::corestore::booltable::BytesBoolTable;
use crate::corestore::booltable::BytesNicheLUT;
use crate::corestore::Data;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::listmap::LockedVec;
use crate::kvengine::KVTable;
use crate::resp::writer;

const OKAY_OVW_BLUT: BytesBoolTable = BytesBoolTable::new(groups::OKAY, groups::OVERWRITE_ERR);
const OKAY_BADIDX_NIL_NLUT: BytesNicheLUT =
    BytesNicheLUT::new(groups::NIL, groups::OKAY, groups::LISTMAP_BAD_INDEX);

action! {
    /// Handle an `LSET` query for the list model
    /// Syntax: `LSET <listname> <values ...>`
    fn lset(handle: &Corestore, con: &mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len > 0)?;
        let listmap = handle.get_table_with::<KVEList>()?;
        let listname = unsafe { act.next_unchecked_bytes() };
        let list = listmap.kve_inner_ref();
        if registry::state_okay() {
            let did = if let Some(entry) = list.fresh_entry(listname.into()) {
                let v: Vec<Data> = act.map(Data::copy_from_slice).collect();
                entry.insert(LockedVec::new(v));
                true
            } else {
                false
            };
            conwrite!(con, OKAY_OVW_BLUT[did])?;
        } else {
            conwrite!(con, groups::SERVER_ERR)?;
        }
        Ok(())
    }
}
