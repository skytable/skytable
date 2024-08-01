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

pub(in crate::engine) mod dcl;
mod ddl_misc;
pub(in crate::engine) mod dml;
pub(in crate::engine) mod exec;
pub(in crate::engine) mod index;
pub(in crate::engine) mod model;
pub(in crate::engine) mod query_meta;
pub(in crate::engine) mod space;
pub(in crate::engine) mod system_db;
// util
mod util;
// test
#[cfg(test)]
pub(super) mod tests;

// re-exports
pub use self::util::{EntityID, EntityIDRef};

// imports
use {
    self::{
        dml::QueryExecMeta,
        model::{Model, ModelData},
    },
    crate::{
        engine::{
            core::space::Space,
            error::{QueryError, QueryResult},
            fractal::{FractalGNSDriver, GlobalInstanceLike},
            idx::IndexST,
            mem::unsafe_apis::BoxStr,
        },
        util::compiler,
    },
    parking_lot::RwLock,
    std::collections::HashMap,
};

/// Use this for now since it substitutes for a file lock (and those syscalls are expensive),
/// but something better is in the offing
type RWLIdx<K, V> = RwLock<IndexST<K, V>>;

#[derive(Debug)]
pub struct GlobalNS {
    data: GNSData,
    driver: FractalGNSDriver,
}

impl GlobalNS {
    pub fn new(data: GNSData, driver: FractalGNSDriver) -> Self {
        Self { data, driver }
    }
    pub fn namespace(&self) -> &GNSData {
        &self.data
    }
    pub fn gns_driver(&self) -> &FractalGNSDriver {
        &self.driver
    }
    #[cfg(test)]
    pub fn into_inner(self) -> (GNSData, FractalGNSDriver) {
        (self.data, self.driver)
    }
}

#[derive(Debug)]
pub struct GNSData {
    idx_mdl: RWLIdx<EntityID, Model>,
    idx: RWLIdx<BoxStr, Space>,
    sys_db: system_db::SystemDatabase,
}

impl GNSData {
    pub fn empty() -> Self {
        Self {
            idx_mdl: RWLIdx::default(),
            idx: RWLIdx::default(),
            sys_db: system_db::SystemDatabase::empty(),
        }
    }
    pub fn ddl_with_all_mut<T>(
        &self,
        f: impl FnOnce(&mut HashMap<BoxStr, Space>, &mut HashMap<EntityID, Model>) -> T,
    ) -> T {
        let mut spaces = self.idx.write();
        let mut models = self.idx_mdl.write();
        f(&mut spaces, &mut models)
    }
    pub fn ddl_with_spaces_write<T>(&self, f: impl FnOnce(&mut HashMap<BoxStr, Space>) -> T) -> T {
        let mut spaces = self.idx.write();
        f(&mut spaces)
    }
    pub fn ddl_with_space_mut<T>(
        &self,
        space: &str,
        f: impl FnOnce(&mut Space) -> QueryResult<T>,
    ) -> QueryResult<T> {
        let mut spaces = self.idx.write();
        let Some(space) = spaces.get_mut(space) else {
            return Err(QueryError::QExecObjectNotFound);
        };
        f(space)
    }
    pub fn with_full_model_for_ddl<'a, T, F>(&self, entity: EntityIDRef<'a>, f: F) -> QueryResult<T>
    where
        F: FnOnce(&Space, &mut Model) -> QueryResult<T>,
    {
        let mut mdl_idx = self.idx_mdl.write();
        let Some(model) = mdl_idx.get_mut(&entity) else {
            return Err(QueryError::QExecObjectNotFound);
        };
        let space_read = self.idx.read();
        let space = space_read.get(entity.space()).unwrap();
        f(space, model)
    }
    pub fn with_model_space_mut_for_ddl<'a, T, F>(
        &self,
        entity: EntityIDRef<'a>,
        f: F,
    ) -> QueryResult<T>
    where
        F: FnOnce(&Space, &mut ModelData) -> QueryResult<T>,
    {
        self.with_full_model_for_ddl(entity, |space, mdl| f(space, mdl.data_mut()))
    }
    pub fn with_model<'a, T, F>(&self, entity: EntityIDRef<'a>, f: F) -> QueryResult<T>
    where
        F: FnOnce(&ModelData) -> QueryResult<T>,
    {
        let mdl_idx = self.idx_mdl.read();
        let Some(model) = mdl_idx.get(&entity) else {
            return Err(QueryError::QExecObjectNotFound);
        };
        f(model.data())
    }
    pub fn idx_models(&self) -> &RWLIdx<EntityID, Model> {
        &self.idx_mdl
    }
    pub fn idx(&self) -> &RWLIdx<BoxStr, Space> {
        &self.idx
    }
    #[cfg(test)]
    pub fn create_empty_test_space(&self, space_name: &str) {
        let _ = self
            .idx()
            .write()
            .insert(space_name.into(), Space::new_auto_all().into());
    }
    pub fn contains_space(&self, name: &str) -> bool {
        self.idx.read().contains_key(name)
    }
    pub fn sys_db(&self) -> &system_db::SystemDatabase {
        &self.sys_db
    }
}

pub(self) fn with_model_for_data_update<'a, F>(
    global: &impl GlobalInstanceLike,
    entity: EntityIDRef<'a>,
    f: F,
) -> QueryResult<()>
where
    F: FnOnce(&ModelData) -> QueryResult<QueryExecMeta>,
{
    let mdl_idx = global.state().namespace().idx_mdl.read();
    let Some(model) = mdl_idx.get(&entity) else {
        return Err(QueryError::QExecObjectNotFound);
    };
    if compiler::likely(model.driver().status().is_healthy()) {
        let r = f(model.data())?;
        model::DeltaState::guard_delta_overflow(
            global,
            entity.space(),
            entity.entity(),
            model.data(),
            r,
        );
        Ok(())
    } else {
        compiler::cold_call(|| Err(QueryError::SysServerError))
    }
}
