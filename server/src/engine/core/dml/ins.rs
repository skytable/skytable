/*
 * Created on Mon May 01 2023
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
    core::{
        self,
        dml::QueryExecMeta,
        index::{DcFieldIndex, PrimaryIndexKey, Row},
        model::{delta::DataDeltaKind, ModelData},
    },
    error::{QueryError, QueryResult},
    fractal::GlobalInstanceLike,
    idx::{IndexBaseSpec, MTIndex, STIndex, STIndexExt, STIndexSeq},
    net::protocol::Response,
    ql::dml::ins::{InsertData, InsertStatement},
    sync::atm::cpin,
};

pub fn insert_resp(
    global: &impl GlobalInstanceLike,
    insert: InsertStatement,
) -> QueryResult<Response> {
    self::insert(global, insert).map(|_| Response::Empty)
}

pub fn insert(global: &impl GlobalInstanceLike, insert: InsertStatement) -> QueryResult<()> {
    core::with_model_for_data_update(global, insert.entity(), |mdl| {
        let (pk, data) = prepare_insert(mdl, insert.data())?;
        let _idx_latch = mdl.primary_index().acquire_cd();
        let g = cpin();
        let ds = mdl.delta_state();
        // create new version
        let new_version = ds.create_new_data_delta_version();
        let row = Row::new(pk, data, ds.schema_current_version(), new_version);
        if mdl.primary_index().__raw_index().mt_insert(row.clone(), &g) {
            // append delta for new version
            let dp = ds.append_new_data_delta_with(DataDeltaKind::Insert, row, new_version, &g);
            Ok(QueryExecMeta::new(dp))
        } else {
            Err(QueryError::QExecDmlDuplicate)
        }
    })
}

// TODO(@ohsayan): optimize null case
fn prepare_insert(
    model: &ModelData,
    insert: InsertData,
) -> QueryResult<(PrimaryIndexKey, DcFieldIndex)> {
    let fields = model.fields();
    let mut okay = fields.len() == insert.column_count();
    let mut prepared_data = DcFieldIndex::idx_init_cap(fields.len());
    match insert {
        InsertData::Ordered(tuple) => {
            let mut fields = fields.stseq_ord_kv();
            let mut tuple = tuple.into_iter();
            while (tuple.len() != 0) & okay {
                let mut data;
                let field;
                unsafe {
                    // UNSAFE(@ohsayan): safe because of invariant
                    data = tuple.next().unwrap_unchecked();
                    // UNSAFE(@ohsayan): safe because of flag
                    field = fields.next().unwrap_unchecked();
                }
                let (field_id, field) = field;
                okay &= field.vt_data_fpath(&mut data);
                okay &= prepared_data.st_insert(
                    unsafe {
                        // UNSAFE(@ohsayan): the model is right here, so we're good
                        field_id.clone()
                    },
                    data,
                );
            }
        }
        InsertData::Map(map) => {
            let mut inserted = 0;
            let mut map = map.into_iter();
            while (map.len() != 0) & okay {
                let (field_id, mut data) = unsafe {
                    // UNSAFE(@ohsayan): loop precondition
                    map.next().unwrap_unchecked()
                };
                let (spec_field_name, spec_field) =
                    match fields.stext_get_key_value(field_id.as_str()) {
                        Some(f) => f,
                        None => {
                            okay = false;
                            break;
                        }
                    };
                okay &= spec_field.vt_data_fpath(&mut data);
                prepared_data.st_insert(
                    unsafe {
                        // UNSAFE(@ohsayan): as long as model lives, we're good
                        spec_field_name.clone()
                    },
                    data,
                );
                inserted += 1;
            }
            okay &= inserted == fields.len();
        }
    }
    let primary_key = prepared_data.remove(model.p_key());
    okay &= primary_key.is_some();
    if okay {
        let primary_key = unsafe {
            // UNSAFE(@ohsayan): okay check above
            PrimaryIndexKey::new_from_dc(primary_key.unwrap_unchecked())
        };
        Ok((primary_key, prepared_data))
    } else {
        Err(QueryError::QExecDmlValidationError)
    }
}
