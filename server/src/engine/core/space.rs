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
        core::{model::ModelView, ItemID, RWLIdx},
        data::{md_dict, DictEntryGeneric, MetaDict},
        error::{DatabaseError, DatabaseResult},
        idx::{IndexST, STIndex},
        ql::ddl::{alt::AlterSpace, crt::CreateSpace, drop::DropSpace},
    },
    parking_lot::RwLock,
};

#[derive(Debug)]
/// A space with the model namespace
pub struct Space {
    mns: RWLIdx<ItemID, ModelView>,
    pub(super) meta: SpaceMeta,
}

#[derive(Debug, Default)]
/// Space metadata
pub struct SpaceMeta {
    pub(super) env: RwLock<MetaDict>,
}

impl SpaceMeta {
    pub const KEY_ENV: &str = "env";
    pub fn with_env(env: MetaDict) -> Self {
        Self {
            env: RWLIdx::new(env),
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// Procedure for `create space`
struct ProcedureCreate {
    space_name: ItemID,
    space: Space,
}

impl ProcedureCreate {
    #[inline(always)]
    /// Define the procedure
    fn new(space_name: ItemID, space: Space) -> Self {
        Self { space_name, space }
    }
}

impl Space {
    pub fn _create_model(&self, name: ItemID, model: ModelView) -> DatabaseResult<()> {
        if self.mns.write().st_insert(name, model) {
            Ok(())
        } else {
            Err(DatabaseError::DdlModelAlreadyExists)
        }
    }
    pub(super) fn models(&self) -> &RWLIdx<ItemID, ModelView> {
        &self.mns
    }
}

impl Space {
    pub fn empty() -> Self {
        Space::new(Default::default(), SpaceMeta::with_env(into_dict! {}))
    }
    #[inline(always)]
    pub fn new(mns: IndexST<ItemID, ModelView>, meta: SpaceMeta) -> Self {
        Self {
            mns: RWLIdx::new(mns),
            meta,
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
        let space_name = ItemID::try_new(&space_name).ok_or(DatabaseError::SysBadItemID)?;
        // check env
        let env = match props.remove(SpaceMeta::KEY_ENV) {
            Some(Some(DictEntryGeneric::Map(m))) if props.is_empty() => m,
            Some(None) | None if props.is_empty() => IndexST::default(),
            _ => {
                return Err(DatabaseError::DdlSpaceBadProperty);
            }
        };
        Ok(ProcedureCreate {
            space_name,
            space: Self::new(
                IndexST::default(),
                SpaceMeta::with_env(
                    // FIXME(@ohsayan): see this is bad. attempt to do it at AST build time
                    md_dict::rflatten_metadata(env),
                ),
            ),
        })
    }
}

impl Space {
    /// Execute a `create` stmt
    pub fn exec_create(gns: &super::GlobalNS, space: CreateSpace) -> DatabaseResult<()> {
        let ProcedureCreate { space_name, space } = Self::process_create(space)?;
        let mut wl = gns.spaces().write();
        if wl.st_insert(space_name, space) {
            Ok(())
        } else {
            Err(DatabaseError::DdlSpaceAlreadyExists)
        }
    }
    /// Execute a `alter` stmt
    pub fn exec_alter(
        gns: &super::GlobalNS,
        AlterSpace {
            space_name,
            mut updated_props,
        }: AlterSpace,
    ) -> DatabaseResult<()> {
        match gns.spaces().read().st_get(space_name.as_bytes()) {
            Some(space) => {
                let mut space_env = space.meta.env.write();
                match updated_props.remove(SpaceMeta::KEY_ENV) {
                    Some(Some(DictEntryGeneric::Map(env))) if updated_props.is_empty() => {
                        if !md_dict::rmerge_metadata(&mut space_env, env) {
                            return Err(DatabaseError::DdlSpaceBadProperty);
                        }
                    }
                    Some(None) if updated_props.is_empty() => space_env.clear(),
                    None => {}
                    _ => return Err(DatabaseError::DdlSpaceBadProperty),
                }
                Ok(())
            }
            None => Err(DatabaseError::DdlSpaceNotFound),
        }
    }
    pub fn exec_drop(
        gns: &super::GlobalNS,
        DropSpace { space, force: _ }: DropSpace,
    ) -> DatabaseResult<()> {
        // TODO(@ohsayan): force remove option
        // TODO(@ohsayan): should a drop space block the entire global table?
        match gns
            .spaces()
            .write()
            .st_delete_if(space.as_bytes(), |space| space.mns.read().len() == 0)
        {
            Some(true) => Ok(()),
            Some(false) => Err(DatabaseError::DdlSpaceRemoveNonEmpty),
            None => Err(DatabaseError::DdlSpaceNotFound),
        }
    }
}

#[cfg(test)]
impl PartialEq for SpaceMeta {
    fn eq(&self, other: &Self) -> bool {
        let x = self.env.read();
        let y = other.env.read();
        *x == *y
    }
}

#[cfg(test)]
impl PartialEq for Space {
    fn eq(&self, other: &Self) -> bool {
        let self_mns = self.mns.read();
        let other_mns = other.mns.read();
        self.meta == other.meta && *self_mns == *other_mns
    }
}
