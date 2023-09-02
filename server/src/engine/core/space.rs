/*
 * Created on Mon Feb 06 2023
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
    crate::engine::{
        core::{model::Model, RWLIdx},
        data::{dict, uuid::Uuid, DictEntryGeneric, DictGeneric},
        error::{DatabaseError, DatabaseResult},
        idx::{IndexST, STIndex},
        ql::ddl::{alt::AlterSpace, crt::CreateSpace, drop::DropSpace},
        txn::gns as gnstxn,
    },
    parking_lot::RwLock,
};

#[derive(Debug)]
/// A space with the model namespace
pub struct Space {
    uuid: Uuid,
    mns: RWLIdx<Box<str>, Model>,
    pub(super) meta: SpaceMeta,
}

#[derive(Debug, Default)]
/// Space metadata
pub struct SpaceMeta {
    pub(super) props: RwLock<DictGeneric>,
}

impl SpaceMeta {
    pub const KEY_ENV: &'static str = "env";
    pub fn new_with_meta(props: DictGeneric) -> Self {
        Self {
            props: RwLock::new(props),
        }
    }
    pub fn with_env(env: DictGeneric) -> Self {
        Self {
            props: RwLock::new(into_dict!("env" => DictEntryGeneric::Map(env))),
        }
    }
    pub fn dict(&self) -> &RwLock<DictGeneric> {
        &self.props
    }
    pub fn get_env<'a>(rwl: &'a parking_lot::RwLockReadGuard<'a, DictGeneric>) -> &'a DictGeneric {
        match rwl.get(Self::KEY_ENV).unwrap() {
            DictEntryGeneric::Data(_) => unreachable!(),
            DictEntryGeneric::Map(m) => m,
        }
    }
    pub fn get_env_mut<'a>(
        rwl: &'a mut parking_lot::RwLockWriteGuard<'a, DictGeneric>,
    ) -> &'a mut DictGeneric {
        match rwl.get_mut(Self::KEY_ENV).unwrap() {
            DictEntryGeneric::Data(_) => unreachable!(),
            DictEntryGeneric::Map(m) => m,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// Procedure for `create space`
struct ProcedureCreate {
    space_name: Box<str>,
    space: Space,
}

impl ProcedureCreate {
    #[inline(always)]
    /// Define the procedure
    fn new(space_name: Box<str>, space: Space) -> Self {
        Self { space_name, space }
    }
}

impl Space {
    pub fn _create_model(&self, name: &str, model: Model) -> DatabaseResult<()> {
        if self
            .mns
            .write()
            .st_insert(name.to_string().into_boxed_str(), model)
        {
            Ok(())
        } else {
            Err(DatabaseError::DdlModelAlreadyExists)
        }
    }
    pub fn get_uuid(&self) -> Uuid {
        self.uuid
    }
    pub fn models(&self) -> &RWLIdx<Box<str>, Model> {
        &self.mns
    }
    pub fn metadata(&self) -> &SpaceMeta {
        &self.meta
    }
    pub fn with_model<T>(
        &self,
        model: &str,
        f: impl FnOnce(&Model) -> DatabaseResult<T>,
    ) -> DatabaseResult<T> {
        let mread = self.mns.read();
        let Some(model) = mread.st_get(model) else {
            return Err(DatabaseError::DdlModelNotFound);
        };
        f(model)
    }
    pub(crate) fn new_restore_empty(meta: SpaceMeta, uuid: Uuid) -> Space {
        Self::new_with_uuid(Default::default(), meta, uuid)
    }
}

impl Space {
    pub fn empty() -> Self {
        Space::new_auto(Default::default(), SpaceMeta::with_env(into_dict! {}))
    }
    pub fn empty_with_uuid(uuid: Uuid) -> Self {
        Space::new_with_uuid(Default::default(), SpaceMeta::with_env(into_dict!()), uuid)
    }
    #[inline(always)]
    pub fn new_auto(mns: IndexST<Box<str>, Model>, meta: SpaceMeta) -> Self {
        Self {
            uuid: Uuid::new(),
            mns: RWLIdx::new(mns),
            meta,
        }
    }
    pub fn new_with_uuid(mns: IndexST<Box<str>, Model>, meta: SpaceMeta, uuid: Uuid) -> Self {
        Self {
            uuid,
            meta,
            mns: RwLock::new(mns),
        }
    }
    #[inline]
    /// Validate a `create` stmt
    fn process_create(
        CreateSpace {
            space_name,
            mut props,
        }: CreateSpace,
    ) -> DatabaseResult<ProcedureCreate> {
        let space_name = space_name.to_string().into_boxed_str();
        // check env
        let env = match props.remove(SpaceMeta::KEY_ENV) {
            Some(DictEntryGeneric::Map(m)) if props.is_empty() => m,
            Some(DictEntryGeneric::Data(l)) if l.is_null() => IndexST::default(),
            None if props.is_empty() => IndexST::default(),
            _ => {
                return Err(DatabaseError::DdlSpaceBadProperty);
            }
        };
        Ok(ProcedureCreate {
            space_name,
            space: Self::new_auto(
                IndexST::default(),
                SpaceMeta::with_env(
                    // FIXME(@ohsayan): see this is bad. attempt to do it at AST build time
                    dict::rflatten_metadata(env),
                ),
            ),
        })
    }
}

impl Space {
    pub fn transactional_exec_create<TI: gnstxn::GNSTransactionDriverLLInterface>(
        gns: &super::GlobalNS,
        txn_driver: &mut gnstxn::GNSTransactionDriverAnyFS<TI>,
        space: CreateSpace,
    ) -> DatabaseResult<()> {
        // process create
        let ProcedureCreate { space_name, space } = Self::process_create(space)?;
        // acquire access
        let mut wl = gns.spaces().write();
        if wl.st_contains(&space_name) {
            return Err(DatabaseError::DdlSpaceAlreadyExists);
        }
        // commit txn
        if TI::NONNULL {
            // prepare and commit txn
            let s_read = space.metadata().dict().read();
            txn_driver.try_commit(gnstxn::CreateSpaceTxn::new(&s_read, &space_name, &space))?;
        }
        // update global state
        let _ = wl.st_insert(space_name, space);
        Ok(())
    }
    /// Execute a `create` stmt
    #[cfg(test)]
    pub fn nontransactional_exec_create(
        gns: &super::GlobalNS,
        space: CreateSpace,
    ) -> DatabaseResult<()> {
        gnstxn::GNSTransactionDriverNullZero::nullzero_create_exec(gns, move |driver| {
            Self::transactional_exec_create(gns, driver, space)
        })
    }
    pub fn transactional_exec_alter<TI: gnstxn::GNSTransactionDriverLLInterface>(
        gns: &super::GlobalNS,
        txn_driver: &mut gnstxn::GNSTransactionDriverAnyFS<TI>,
        AlterSpace {
            space_name,
            updated_props,
        }: AlterSpace,
    ) -> DatabaseResult<()> {
        gns.with_space(&space_name, |space| {
            match updated_props.get(SpaceMeta::KEY_ENV) {
                Some(DictEntryGeneric::Map(_)) if updated_props.len() == 1 => {}
                Some(DictEntryGeneric::Data(l)) if updated_props.len() == 1 && l.is_null() => {}
                None if updated_props.is_empty() => return Ok(()),
                _ => return Err(DatabaseError::DdlSpaceBadProperty),
            }
            let mut space_props = space.meta.dict().write();
            // create patch
            let patch = match dict::rprepare_metadata_patch(&space_props, updated_props) {
                Some(patch) => patch,
                None => return Err(DatabaseError::DdlSpaceBadProperty),
            };
            if TI::NONNULL {
                // prepare txn
                let txn =
                    gnstxn::AlterSpaceTxn::new(gnstxn::SpaceIDRef::new(&space_name, space), &patch);
                // commit
                txn_driver.try_commit(txn)?;
            }
            // merge
            dict::rmerge_data_with_patch(&mut space_props, patch);
            // the `env` key may have been popped, so put it back (setting `env: null` removes the env key and we don't want to waste time enforcing this in the
            // merge algorithm)
            let _ = space_props.st_insert(
                SpaceMeta::KEY_ENV.into(),
                DictEntryGeneric::Map(into_dict!()),
            );
            Ok(())
        })
    }
    #[cfg(test)]
    /// Execute a `alter` stmt
    pub fn nontransactional_exec_alter(
        gns: &super::GlobalNS,
        alter: AlterSpace,
    ) -> DatabaseResult<()> {
        gnstxn::GNSTransactionDriverNullZero::nullzero_create_exec(gns, move |driver| {
            Self::transactional_exec_alter(gns, driver, alter)
        })
    }
    pub fn transactional_exec_drop<TI: gnstxn::GNSTransactionDriverLLInterface>(
        gns: &super::GlobalNS,
        txn_driver: &mut gnstxn::GNSTransactionDriverAnyFS<TI>,
        DropSpace { space, force: _ }: DropSpace,
    ) -> DatabaseResult<()> {
        // TODO(@ohsayan): force remove option
        // TODO(@ohsayan): should a drop space block the entire global table?
        let space_name = space;
        let mut wgns = gns.spaces().write();
        let space = match wgns.get(space_name.as_str()) {
            Some(space) => space,
            None => return Err(DatabaseError::DdlSpaceNotFound),
        };
        let space_w = space.mns.write();
        if space_w.st_len() != 0 {
            return Err(DatabaseError::DdlSpaceRemoveNonEmpty);
        }
        // we can remove this
        if TI::NONNULL {
            // prepare txn
            let txn = gnstxn::DropSpaceTxn::new(gnstxn::SpaceIDRef::new(&space_name, space));
            txn_driver.try_commit(txn)?;
        }
        drop(space_w);
        let _ = wgns.st_delete(space_name.as_str());
        Ok(())
    }
    #[cfg(test)]
    pub fn nontransactional_exec_drop(
        gns: &super::GlobalNS,
        drop_space: DropSpace,
    ) -> DatabaseResult<()> {
        gnstxn::GNSTransactionDriverNullZero::nullzero_create_exec(gns, move |driver| {
            Self::transactional_exec_drop(gns, driver, drop_space)
        })
    }
}

#[cfg(test)]
impl PartialEq for SpaceMeta {
    fn eq(&self, other: &Self) -> bool {
        let x = self.props.read();
        let y = other.props.read();
        *x == *y
    }
}

#[cfg(test)]
impl PartialEq for Space {
    fn eq(&self, other: &Self) -> bool {
        let self_mns = self.mns.read();
        let other_mns = other.mns.read();
        self.meta == other.meta && *self_mns == *other_mns && self.uuid == other.uuid
    }
}
