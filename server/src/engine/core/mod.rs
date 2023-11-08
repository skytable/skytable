/*
 * Created on Wed Oct 12 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use self::dml::QueryExecMeta;
pub use self::util::{EntityID, EntityIDRef};
use super::{fractal::GlobalInstanceLike, ql::ast::Entity};
pub(in crate::engine) mod dml;
pub mod exec;
pub(in crate::engine) mod index;
pub(in crate::engine) mod model;
pub(in crate::engine) mod query_meta;
pub mod space;
mod util;
// test
#[cfg(test)]
pub(super) mod tests;
// imports
use {
    self::model::Model,
    crate::engine::{
        core::space::Space,
        error::{QueryError, QueryResult},
        idx::IndexST,
    },
    parking_lot::RwLock,
    std::collections::HashMap,
};

/// Use this for now since it substitutes for a file lock (and those syscalls are expensive),
/// but something better is in the offing
type RWLIdx<K, V> = RwLock<IndexST<K, V>>;

#[cfg_attr(test, derive(Debug))]
pub struct GlobalNS {
    idx_mdl: RWLIdx<EntityID, Model>,
    idx: RWLIdx<Box<str>, RwLock<Space>>,
}

impl GlobalNS {
    pub fn empty() -> Self {
        Self {
            idx_mdl: RWLIdx::default(),
            idx: RWLIdx::default(),
        }
    }
    pub fn ddl_with_spaces_write<T>(
        &self,
        f: impl FnOnce(&mut HashMap<Box<str>, RwLock<Space>>) -> T,
    ) -> T {
        let mut spaces = self.idx.write();
        f(&mut spaces)
    }
    pub fn ddl_with_space_mut<T>(
        &self,
        space: &str,
        f: impl FnOnce(&mut Space) -> QueryResult<T>,
    ) -> QueryResult<T> {
        let spaces = self.idx.read();
        let Some(space) = spaces.get(space) else {
            return Err(QueryError::QExecObjectNotFound);
        };
        let mut space = space.write();
        f(&mut space)
    }
    pub fn with_model_space<'a, T, F>(&self, entity: Entity<'a>, f: F) -> QueryResult<T>
    where
        F: FnOnce(&Space, &Model) -> QueryResult<T>,
    {
        let (space, model_name) = entity.into_full_result()?;
        let mdl_idx = self.idx_mdl.read();
        let Some(model) = mdl_idx.get(&EntityIDRef::new(&space, &model_name)) else {
            return Err(QueryError::QExecObjectNotFound);
        };
        let space_read = self.idx.read();
        let space = space_read.get(space.as_str()).unwrap().read();
        f(&space, model)
    }
    pub fn with_model<'a, T, F>(&self, entity: Entity<'a>, f: F) -> QueryResult<T>
    where
        F: FnOnce(&Model) -> QueryResult<T>,
    {
        let (space, model_name) = entity.into_full_result()?;
        let mdl_idx = self.idx_mdl.read();
        let Some(model) = mdl_idx.get(&EntityIDRef::new(&space, &model_name)) else {
            return Err(QueryError::QExecObjectNotFound);
        };
        f(model)
    }
    pub fn idx_models(&self) -> &RWLIdx<EntityID, Model> {
        &self.idx_mdl
    }
    pub fn idx(&self) -> &RWLIdx<Box<str>, RwLock<Space>> {
        &self.idx
    }
    #[cfg(test)]
    pub fn create_empty_test_space(&self, space_name: &str) {
        let _ = self
            .idx()
            .write()
            .insert(space_name.into(), Space::new_auto_all().into());
    }
}

pub(self) fn with_model_for_data_update<'a, F>(
    global: &impl GlobalInstanceLike,
    entity: Entity<'a>,
    f: F,
) -> QueryResult<()>
where
    F: FnOnce(&Model) -> QueryResult<QueryExecMeta>,
{
    let (space, model_name) = entity.into_full_result()?;
    let mdl_idx = global.namespace().idx_mdl.read();
    let Some(model) = mdl_idx.get(&EntityIDRef::new(&space, &model_name)) else {
        return Err(QueryError::QExecObjectNotFound);
    };
    let r = f(model)?;
    model::DeltaState::guard_delta_overflow(global, &space, &model_name, model, r);
    Ok(())
}
