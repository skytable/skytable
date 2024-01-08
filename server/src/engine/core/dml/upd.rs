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

use {
    crate::{
        engine::{
            core::{
                self, dml::QueryExecMeta, model::delta::DataDeltaKind,
                query_meta::AssignmentOperator,
            },
            data::{
                cell::Datacell,
                lit::Lit,
                tag::{DataTag, FloatSpec, SIntSpec, TagClass, UIntSpec},
            },
            error::{QueryError, QueryResult},
            fractal::GlobalInstanceLike,
            idx::STIndex,
            net::protocol::Response,
            ql::dml::upd::{AssignmentExpression, UpdateStatement},
            sync,
        },
        util::compiler,
    },
    std::mem,
};

#[inline(always)]
unsafe fn dc_op_fail(_: &Datacell, _: Lit) -> (bool, Datacell) {
    (false, Datacell::null())
}
// bool
unsafe fn dc_op_bool_ass(_: &Datacell, rhs: Lit) -> (bool, Datacell) {
    (true, Datacell::new_bool(rhs.bool()))
}
// uint
unsafe fn dc_op_uint_ass(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let uint = rhs.uint();
    let kind = UIntSpec::from_full(dc.tag());
    (kind.check(uint), Datacell::new_uint(uint, kind))
}
unsafe fn dc_op_uint_add(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = UIntSpec::from_full(dc.tag());
    let (uint, did_of) = dc.uint().overflowing_add(rhs.uint());
    (kind.check(uint) & !did_of, Datacell::new_uint(uint, kind))
}
unsafe fn dc_op_uint_sub(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = UIntSpec::from_full(dc.tag());
    let (uint, did_of) = dc.uint().overflowing_sub(rhs.uint());
    (kind.check(uint) & !did_of, Datacell::new_uint(uint, kind))
}
unsafe fn dc_op_uint_mul(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = UIntSpec::from_full(dc.tag());
    let (uint, did_of) = dc.uint().overflowing_mul(rhs.uint());
    (kind.check(uint) & !did_of, Datacell::new_uint(uint, kind))
}
unsafe fn dc_op_uint_div(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = UIntSpec::from_full(dc.tag());
    let (uint, did_of) = dc.uint().overflowing_div(rhs.uint());
    (kind.check(uint) & !did_of, Datacell::new_uint(uint, kind))
}
// sint
unsafe fn dc_op_sint_ass(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let sint = rhs.sint();
    let kind = SIntSpec::from_full(dc.tag());
    (kind.check(sint), Datacell::new_sint(sint, kind))
}
unsafe fn dc_op_sint_add(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = SIntSpec::from_full(dc.tag());
    let (sint, did_of) = dc.sint().overflowing_add(rhs.sint());
    (kind.check(sint) & !did_of, Datacell::new_sint(sint, kind))
}
unsafe fn dc_op_sint_sub(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = SIntSpec::from_full(dc.tag());
    let (sint, did_of) = dc.sint().overflowing_sub(rhs.sint());
    (kind.check(sint) & !did_of, Datacell::new_sint(sint, kind))
}
unsafe fn dc_op_sint_mul(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = SIntSpec::from_full(dc.tag());
    let (sint, did_of) = dc.sint().overflowing_mul(rhs.sint());
    (kind.check(sint) & !did_of, Datacell::new_sint(sint, kind))
}
unsafe fn dc_op_sint_div(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let kind = SIntSpec::from_full(dc.tag());
    let (sint, did_of) = dc.sint().overflowing_div(rhs.sint());
    (kind.check(sint) & !did_of, Datacell::new_sint(sint, kind))
}
/*
    float
    ---
    FIXME(@ohsayan): floating point always upsets me now and then, this time its
    the silent overflow boom and I think I should implement a strict mode (no MySQL,
    not `STRICT_ALL_TABLES` unless we do actually end up going down that route. In
    that case, oops)
    --
    TODO(@ohsayan): account for float32 overflow
*/
unsafe fn dc_op_float_ass(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let float = rhs.float();
    let kind = FloatSpec::from_full(dc.tag());
    (kind.check(float), Datacell::new_float(float, kind))
}
unsafe fn dc_op_float_add(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let result = dc.read_float() + rhs.float();
    let kind = FloatSpec::from_full(dc.tag());
    (kind.check(result), Datacell::new_float(result, kind))
}
unsafe fn dc_op_float_sub(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let result = dc.read_float() - rhs.float();
    let kind = FloatSpec::from_full(dc.tag());
    (kind.check(result), Datacell::new_float(result, kind))
}
unsafe fn dc_op_float_mul(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let result = dc.read_float() * rhs.float();
    let kind = FloatSpec::from_full(dc.tag());
    (kind.check(result), Datacell::new_float(result, kind))
}
unsafe fn dc_op_float_div(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let result = dc.read_float() / rhs.float();
    let kind = FloatSpec::from_full(dc.tag());
    (kind.check(result), Datacell::new_float(result, kind))
}
// binary
unsafe fn dc_op_bin_ass(_dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let new_bin = rhs.bin();
    let mut v = Vec::new();
    if v.try_reserve_exact(new_bin.len()).is_err() {
        return dc_op_fail(_dc, rhs);
    }
    v.extend_from_slice(new_bin);
    (true, Datacell::new_bin(v.into_boxed_slice()))
}
unsafe fn dc_op_bin_add(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let push_into_bin = rhs.bin();
    let mut bin = Vec::new();
    if compiler::unlikely(bin.try_reserve_exact(push_into_bin.len()).is_err()) {
        return dc_op_fail(dc, rhs);
    }
    bin.extend_from_slice(dc.read_bin());
    bin.extend_from_slice(push_into_bin);
    (true, Datacell::new_bin(bin.into_boxed_slice()))
}
// string
unsafe fn dc_op_str_ass(_dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let new_str = rhs.str();
    let mut v = String::new();
    if v.try_reserve_exact(new_str.len()).is_err() {
        return dc_op_fail(_dc, rhs);
    }
    v.push_str(new_str);
    (true, Datacell::new_str(v.into_boxed_str()))
}
unsafe fn dc_op_str_add(dc: &Datacell, rhs: Lit) -> (bool, Datacell) {
    let push_into_str = rhs.str();
    let mut str = String::new();
    if compiler::unlikely(str.try_reserve_exact(push_into_str.len()).is_err()) {
        return dc_op_fail(dc, rhs);
    }
    str.push_str(dc.read_str());
    str.push_str(push_into_str);
    (true, Datacell::new_str(str.into_boxed_str()))
}

static OPERATOR: [unsafe fn(&Datacell, Lit) -> (bool, Datacell); {
    TagClass::MAX as usize * AssignmentOperator::VARIANTS
}] = [
    // bool
    dc_op_bool_ass,
    // -- pad: 4
    dc_op_fail,
    dc_op_fail,
    dc_op_fail,
    dc_op_fail,
    // uint
    dc_op_uint_ass,
    dc_op_uint_add,
    dc_op_uint_sub,
    dc_op_uint_mul,
    dc_op_uint_div,
    // sint
    dc_op_sint_ass,
    dc_op_sint_add,
    dc_op_sint_sub,
    dc_op_sint_mul,
    dc_op_sint_div,
    // float
    dc_op_float_ass,
    dc_op_float_add,
    dc_op_float_sub,
    dc_op_float_mul,
    dc_op_float_div,
    // bin
    dc_op_bin_ass,
    dc_op_bin_add,
    // -- pad: 3
    dc_op_fail,
    dc_op_fail,
    dc_op_fail,
    // str
    dc_op_str_ass,
    dc_op_str_add,
    // -- pad: 3
    dc_op_fail,
    dc_op_fail,
    dc_op_fail,
];

#[inline(always)]
const fn opc(opr: TagClass, ope: AssignmentOperator) -> usize {
    (AssignmentOperator::VARIANTS * opr.value_word()) + ope.value_word()
}

#[cfg(test)]
local! {
    pub(super) static ROUTE_TRACE: Vec<&'static str> = Vec::new();
}

#[inline(always)]
fn input_trace(v: &'static str) {
    #[cfg(test)]
    {
        local_mut!(ROUTE_TRACE, |rtrace| rtrace.push(v))
    }
    let _ = v;
}
#[cfg(test)]
pub fn collect_trace_path() -> Vec<&'static str> {
    local_ref!(ROUTE_TRACE, |rtrace| rtrace.iter().cloned().collect())
}
pub fn update_resp(
    global: &impl GlobalInstanceLike,
    update: UpdateStatement,
) -> QueryResult<Response> {
    self::update(global, update).map(|_| Response::Empty)
}

pub fn update(global: &impl GlobalInstanceLike, mut update: UpdateStatement) -> QueryResult<()> {
    core::with_model_for_data_update(global, update.entity(), |mdl| {
        let mut ret = Ok(QueryExecMeta::zero());
        // prepare row fetch
        let key = mdl.resolve_where(update.clauses_mut())?;
        // fetch row
        let g = sync::atm::cpin();
        let Some(row) = mdl.primary_index().select(key, &g) else {
            return Err(QueryError::QExecDmlRowNotFound);
        };
        // lock row
        let mut row_data_wl = row.d_data().write();
        // create new version
        let ds = mdl.delta_state();
        let new_version = ds.create_new_data_delta_version();
        // process changes
        let mut rollback_now = false;
        let mut rollback_data = Vec::with_capacity(update.expressions().len());
        let mut assn_expressions = update.into_expressions().into_iter();
        /*
            FIXME(@ohsayan): where's my usual magic? I'll do it once we have the SE stabilized
        */
        // apply changes
        while (assn_expressions.len() != 0) & (!rollback_now) {
            let AssignmentExpression {
                lhs,
                rhs,
                operator_fn,
            } = unsafe {
                // UNSAFE(@ohsayan): pre-loop cond
                assn_expressions.next().unwrap_unchecked()
            };
            let field_definition;
            let field_data;
            match (
                mdl.fields().st_get(lhs.as_str()),
                row_data_wl.fields_mut().st_get_mut(lhs.as_str()),
            ) {
                (Some(fdef), Some(fdata)) => {
                    field_definition = fdef;
                    field_data = fdata;
                }
                _ => {
                    input_trace("fieldnotfound");
                    rollback_now = true;
                    ret = Err(QueryError::QExecUnknownField);
                    break;
                }
            }
            match (
                field_definition.layers()[0].tag().tag_class(),
                rhs.kind().tag_class(),
            ) {
                (tag_a, tag_b)
                    if (tag_a == tag_b) & (tag_a < TagClass::List) & field_data.is_init() =>
                {
                    let (okay, new) = unsafe { OPERATOR[opc(tag_a, operator_fn)](field_data, rhs) };
                    rollback_now &= !okay;
                    rollback_data.push((lhs.as_str(), mem::replace(field_data, new)));
                    input_trace("sametag;nonnull");
                }
                (tag_a, tag_b)
                    if (tag_a == tag_b)
                        & field_data.is_null()
                        & (operator_fn == AssignmentOperator::Assign) =>
                {
                    rollback_data.push((lhs.as_str(), mem::replace(field_data, rhs.into())));
                    input_trace("sametag;orignull");
                }
                (TagClass::List, tag_b) if operator_fn == AssignmentOperator::AddAssign => {
                    if field_definition.layers()[1].tag().tag_class() == tag_b {
                        unsafe {
                            // UNSAFE(@ohsayan): matched tags
                            let mut list = field_data.read_list().write();
                            if list.try_reserve(1).is_ok() {
                                input_trace("list;sametag");
                                list.push(rhs.into());
                            } else {
                                rollback_now = true;
                                ret = Err(QueryError::SysOutOfMemory);
                                break;
                            }
                        }
                    } else {
                        input_trace("list;badtag");
                        rollback_now = true;
                        ret = Err(QueryError::QExecDmlValidationError);
                        break;
                    }
                }
                _ => {
                    input_trace("unknown_reason;exitmainloop");
                    ret = Err(QueryError::QExecDmlValidationError);
                    rollback_now = true;
                    break;
                }
            }
        }
        if compiler::unlikely(rollback_now) {
            input_trace("rollback");
            rollback_data
                .into_iter()
                .for_each(|(field_id, restored_data)| {
                    row_data_wl.fields_mut().st_update(field_id, restored_data);
                });
        } else {
            // update revised tag
            row_data_wl.set_txn_revised(new_version);
            // publish delta
            let dp =
                ds.append_new_data_delta_with(DataDeltaKind::Update, row.clone(), new_version, &g);
            ret = Ok(QueryExecMeta::new(dp))
        }
        ret
    })
}
