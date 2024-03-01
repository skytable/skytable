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
    core::{
        index::{
            DcFieldIndex, IndexLatchHandleExclusive, PrimaryIndexKey, Row, RowData, RowDataLck,
        },
        model::ModelData,
    },
    data::{
        cell::{Datacell, VirtualDatacell},
        tag::{DataTag, TagClass},
    },
    error::{QueryError, QueryResult},
    fractal::GlobalInstanceLike,
    idx::{IndexMTRaw, MTIndexExt, STIndex, STIndexSeq},
    mem::IntegerRepr,
    net::protocol::{Response, ResponseType},
    ql::dml::sel::{SelectAllStatement, SelectStatement},
    sync,
};

pub fn select_resp(
    global: &impl GlobalInstanceLike,
    select: SelectStatement,
) -> QueryResult<Response> {
    let mut data = vec![];
    let mut i = 0usize;
    self::select_custom(global, select, |item| {
        encode_cell(&mut data, item);
        i += 1;
    })?;
    Ok(Response::Serialized {
        ty: ResponseType::Row,
        size: i,
        data,
    })
}

pub fn select_all_resp(
    global: &impl GlobalInstanceLike,
    select: SelectAllStatement,
) -> QueryResult<Response> {
    let mut ret_buf = Vec::new();
    let i = self::select_all(
        global,
        select,
        &mut ret_buf,
        |buf, _, col_c| {
            IntegerRepr::scoped(col_c as u64, |repr| buf.extend(repr));
            buf.push(b'\n');
        },
        |buf, data, _| encode_cell(buf, data),
    )?;
    Ok(Response::Serialized {
        ty: ResponseType::MultiRow,
        size: i,
        data: ret_buf,
    })
}

pub fn select_all<Fm, F, T>(
    global: &impl GlobalInstanceLike,
    select: SelectAllStatement,
    serialize_target: &mut T,
    mut f_mdl: Fm,
    mut f: F,
) -> QueryResult<usize>
where
    Fm: FnMut(&mut T, &ModelData, usize),
    F: FnMut(&mut T, &Datacell, usize),
{
    global.state().namespace().with_model(select.entity, |mdl| {
        let g = sync::atm::cpin();
        let mut i = 0;
        if select.wildcard {
            f_mdl(serialize_target, mdl, mdl.fields().len());
            for (key, data) in RowIteratorAll::new(&g, mdl, select.limit as usize) {
                let vdc = VirtualDatacell::new_pk(key, mdl.p_tag());
                for key in mdl.fields().stseq_ord_key() {
                    let r = if key.as_str() == mdl.p_key() {
                        &*vdc
                    } else {
                        data.fields().get(key).unwrap()
                    };
                    f(serialize_target, r, mdl.fields().len());
                }
                i += 1;
            }
        } else {
            // schema check
            if select.fields.len() > mdl.fields().len()
                || select
                    .fields
                    .iter()
                    .any(|f| !mdl.fields().st_contains(f.as_str()))
            {
                return Err(QueryError::QExecUnknownField);
            }
            f_mdl(serialize_target, mdl, select.fields.len());
            for (key, data) in RowIteratorAll::new(&g, mdl, select.limit as usize) {
                let vdc = VirtualDatacell::new_pk(key, mdl.p_tag());
                for key in select.fields.iter() {
                    let r = if key.as_str() == mdl.p_key() {
                        &*vdc
                    } else {
                        data.fields().st_get(key.as_str()).unwrap()
                    };
                    f(serialize_target, r, select.fields.len());
                }
                i += 1;
            }
        }
        Ok(i)
    })
}

fn encode_cell(resp: &mut Vec<u8>, item: &Datacell) {
    resp.push((item.tag().tag_selector().value_u8() + 1) * (item.is_init() as u8));
    if item.is_null() {
        return;
    }
    unsafe {
        // UNSAFE(@ohsayan): +tagck
        match item.tag().tag_class() {
            TagClass::Bool => return resp.push(item.read_bool() as _),
            TagClass::UnsignedInt => IntegerRepr::scoped(item.read_uint(), |b| resp.extend(b)),
            TagClass::SignedInt => IntegerRepr::scoped(item.read_sint(), |b| resp.extend(b)),
            TagClass::Float => resp.extend(item.read_float().to_string().as_bytes()),
            TagClass::Bin | TagClass::Str => {
                let slc = item.read_bin();
                IntegerRepr::scoped(slc.len() as u64, |b| resp.extend(b));
                resp.push(b'\n');
                resp.extend(slc);
                return;
            }
            TagClass::List => {
                let list = item.read_list();
                let ls = list.read();
                IntegerRepr::scoped(ls.len() as u64, |b| resp.extend(b));
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
    global
        .state()
        .namespace()
        .with_model(select.entity(), |mdl| {
            let target_key = mdl.resolve_where(select.clauses_mut())?;
            let pkdc = VirtualDatacell::new(target_key.clone(), mdl.p_tag().tag_unique());
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
                        for key in mdl.fields().stseq_ord_key() {
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

struct RowIteratorAll<'g> {
    _g: &'g sync::atm::Guard,
    mdl: &'g ModelData,
    iter: <IndexMTRaw<Row> as MTIndexExt<Row, PrimaryIndexKey, RowDataLck>>::IterEntry<'g, 'g, 'g>,
    _latch: IndexLatchHandleExclusive<'g>,
    limit: usize,
}

impl<'g> RowIteratorAll<'g> {
    fn new(g: &'g sync::atm::Guard, mdl: &'g ModelData, limit: usize) -> Self {
        let idx = mdl.primary_index();
        let latch = idx.acquire_exclusive();
        Self {
            _g: g,
            mdl,
            iter: idx.__raw_index().mt_iter_entry(g),
            _latch: latch,
            limit,
        }
    }
    fn _next(
        &mut self,
    ) -> Option<(
        &'g PrimaryIndexKey,
        parking_lot::RwLockReadGuard<'g, RowData>,
    )> {
        if self.limit == 0 {
            return None;
        }
        self.limit -= 1;
        self.iter.next().map(|row| {
            (
                row.d_key(),
                row.resolve_schema_deltas_and_freeze(self.mdl.delta_state()),
            )
        })
    }
}

impl<'g> Iterator for RowIteratorAll<'g> {
    type Item = (
        &'g PrimaryIndexKey,
        parking_lot::RwLockReadGuard<'g, RowData>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        self._next()
    }
}
