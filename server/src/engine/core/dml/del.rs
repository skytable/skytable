/*
 * Created on Sat May 06 2023
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
    core::{model::delta::DataDeltaKind, GlobalNS},
    error::{DatabaseError, DatabaseResult},
    idx::MTIndex,
    ql::dml::del::DeleteStatement,
    sync,
};

pub fn delete(gns: &GlobalNS, mut delete: DeleteStatement) -> DatabaseResult<()> {
    gns.with_model(delete.entity(), |model| {
        let g = sync::atm::cpin();
        let schema_version = model.delta_state().schema_current_version();
        let delta_state = model.delta_state();
        // create new version
        let new_version = delta_state.create_new_data_delta_version();
        match model
            .primary_index()
            .__raw_index()
            .mt_delete_return_entry(&model.resolve_where(delete.clauses_mut())?, &g)
        {
            Some(row) => {
                delta_state.append_new_data_delta(
                    DataDeltaKind::Delete,
                    row.clone(),
                    schema_version,
                    new_version,
                    &g,
                );
                Ok(())
            }
            None => Err(DatabaseError::DmlEntryNotFound),
        }
    })
}
