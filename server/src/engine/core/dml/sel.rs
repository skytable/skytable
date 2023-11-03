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
    data::{
        cell::{Datacell, VirtualDatacell},
        tag::{DataTag, TagClass},
    },
    error::{QueryError, QueryResult},
    fractal::GlobalInstanceLike,
    idx::{STIndex, STIndexSeq},
    net::protocol::Response,
    ql::dml::sel::SelectStatement,
    sync,
};

pub fn select_resp(
    global: &impl GlobalInstanceLike,
    select: SelectStatement,
) -> QueryResult<Response> {
    let mut resp_b = vec![];
    let mut resp_a = vec![];
    let mut i = 0u64;
    self::select_custom(global, select, |item| {
        encode_cell(&mut resp_b, item);
        i += 1;
    })?;
    resp_a.push(0x11);
    resp_a.extend(i.to_string().as_bytes());
    resp_a.push(b'\n');
    Ok(Response::EncodedAB(
        resp_a.into_boxed_slice(),
        resp_b.into_boxed_slice(),
    ))
}

fn encode_cell(resp: &mut Vec<u8>, item: &Datacell) {
    resp.push((item.tag().tag_selector().value_u8() + 1) * (item.is_init() as u8));
    if item.is_null() {
        return;
    }
    unsafe {
        // UNSAFE(@ohsayan): +tagck
        // NOTE(@ohsayan): optimize out unwanted alloc
        match item.tag().tag_class() {
            TagClass::Bool => resp.push(item.read_bool() as _),
            TagClass::UnsignedInt => resp.extend(item.read_uint().to_string().as_bytes()),
            TagClass::SignedInt => resp.extend(item.read_sint().to_string().as_bytes()),
            TagClass::Float => resp.extend(item.read_float().to_string().as_bytes()),
            TagClass::Bin | TagClass::Str => {
                let slc = item.read_bin();
                resp.extend(slc.len().to_string().as_bytes());
                resp.push(b'\n');
                resp.extend(slc);
                return;
            }
            TagClass::List => {
                let list = item.read_list();
                let ls = list.read();
                resp.extend(ls.len().to_string().as_bytes());
                resp.push(b'\n');
                for item in ls.iter() {
                    encode_cell(resp, item);
                }
                return;
            }
        }
    }
    resp.push(b'\n');
}

pub fn select_custom<F>(
    global: &impl GlobalInstanceLike,
    mut select: SelectStatement,
    mut cellfn: F,
) -> QueryResult<()>
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
                None => return Err(QueryError::QExecUnknownField),
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
            None => return Err(QueryError::QExecDmlRowNotFound),
        }
        Ok(())
    })
}
