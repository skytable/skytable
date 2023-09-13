/*
 * Created on Thu May 11 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

use crate::engine::{
    core::index::DcFieldIndex,
    data::cell::{Datacell, VirtualDatacell},
    error::{DatabaseError, DatabaseResult},
    fractal::GlobalInstanceLike,
    idx::{STIndex, STIndexSeq},
    ql::dml::sel::SelectStatement,
    sync,
};

pub fn select_custom<F>(
    global: &impl GlobalInstanceLike,
    mut select: SelectStatement,
    mut cellfn: F,
) -> DatabaseResult<()>
where
    F: FnMut(&Datacell),
{
    global.namespace().with_model(select.entity(), |mdl| {
        let irm = mdl.intent_read_model();
        let target_key = mdl.resolve_where(select.clauses_mut())?;
        let pkdc = VirtualDatacell::new(target_key.clone());
        let g = sync::atm::cpin();
        let mut read_field = |key, fields: &DcFieldIndex| {
            match fields.st_get(key) {
                Some(dc) => cellfn(dc),
                None if key == mdl.p_key() => cellfn(&pkdc),
                None => return Err(DatabaseError::FieldNotFound),
            }
            Ok(())
        };
        match mdl.primary_index().select(target_key.clone(), &g) {
            Some(row) => {
                let r = row.resolve_schema_deltas_and_freeze(mdl.delta_state());
                if select.is_wildcard() {
                    for key in irm.fields().stseq_ord_key() {
                        read_field(key.as_ref(), r.fields())?;
                    }
                } else {
                    for key in select.into_fields() {
                        read_field(key.as_str(), r.fields())?;
                    }
                }
            }
            None => return Err(DatabaseError::DmlEntryNotFound),
        }
        Ok(())
    })
}
