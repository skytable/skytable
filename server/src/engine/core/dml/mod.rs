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

mod del;
mod ins;
mod sel;
mod upd;

use crate::{
    engine::{
        control_flow::{errors::QueryError, QueryResult},
        core::model::ModelData,
        data::{lit::Lit, tag::DataTag},
        fractal::GlobalInstanceLike,
        idx::MTIndex,
        ql::dml::{trunc::TruncateStmt, WhereClause},
        storage::{BatchStats, Truncate},
    },
    util::compiler,
};

#[cfg(test)]
pub use {
    del::delete,
    ins::insert,
    sel::{select_all, select_custom},
    upd::{collect_trace_path as update_flow_trace, update},
};
pub use {
    del::delete_resp,
    ins::{insert_resp, upsert_resp},
    sel::{select_all_resp, select_resp},
    upd::update_resp,
};

impl ModelData {
    pub(self) fn resolve_where<'a>(
        &self,
        where_clause: &mut WhereClause<'a>,
    ) -> QueryResult<Lit<'a>> {
        match where_clause.clauses_mut().remove(self.p_key().as_bytes()) {
            Some(clause)
                if clause.filter_hint_none()
                    & (clause.rhs().kind().tag_unique() == self.p_tag().tag_unique()) =>
            {
                Ok(clause.rhs())
            }
            _ => compiler::cold_rerr(QueryError::QExecDmlWhereHasUnindexedColumn),
        }
    }
}

#[derive(Debug)]
pub struct QueryExecMeta {
    delta_hint: usize,
}

impl QueryExecMeta {
    pub fn new(delta_hint: usize) -> Self {
        Self { delta_hint }
    }
    pub fn zero() -> Self {
        Self::new(0)
    }
    pub fn delta_hint(&self) -> usize {
        self.delta_hint
    }
}

pub fn truncate(g: &impl GlobalInstanceLike, stmt: TruncateStmt) -> QueryResult<()> {
    match stmt {
        TruncateStmt::Model(mdl_id) => {
            g.state()
                .namespace()
                .with_full_model_for_ddl(mdl_id, |_, mdl| {
                    // commit truncate
                    {
                        let mut drv = mdl.driver().batch_driver().lock();
                        drv.as_mut()
                            .unwrap()
                            .commit_with_ctx(Truncate, BatchStats::new())?;
                        // good now clear delta state
                    }
                    // wipe the index
                    mdl.data()
                        .primary_index()
                        .__raw_index()
                        .mt_clear(&crossbeam_epoch::pin());
                    // reset delta state
                    mdl.data_mut().delta_state_mut().__reset();
                    // increment runtime ID to invalidate previously queued tasks
                    mdl.data_mut().increment_runtime_id();
                    // done
                    Ok(())
                })
        }
    }
}
