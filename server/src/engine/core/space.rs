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
        core::{
            data::{md_dict, DictEntryGeneric, MetaDict},
            model::ModelView,
            ItemID, RWLIdx,
        },
        error::{DatabaseError, DatabaseResult},
        idx::{IndexST, STIndex},
        ql::ddl::crt::CreateSpace,
    },
    parking_lot::RwLock,
    std::sync::Arc,
};

#[derive(Debug)]
pub struct Space {
    mns: RWLIdx<ItemID, Arc<ModelView>>,
    meta: SpaceMeta,
}

#[derive(Debug, Default)]
pub struct SpaceMeta {
    env: RwLock<MetaDict>,
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
struct Procedure {
    space_name: ItemID,
    space: Space,
}

impl Procedure {
    #[inline(always)]
    pub(super) fn new(space_name: ItemID, space: Space) -> Self {
        Self { space_name, space }
    }
}

impl Space {
    #[inline(always)]
    pub fn new(mns: IndexST<ItemID, Arc<ModelView>>, meta: SpaceMeta) -> Self {
        Self {
            mns: RWLIdx::new(mns),
            meta,
        }
    }
    #[inline]
    fn validate(
        CreateSpace {
            space_name,
            mut props,
        }: CreateSpace,
    ) -> DatabaseResult<Procedure> {
        let space_name = ItemID::try_new(&space_name).ok_or(DatabaseError::SysBadItemID)?;
        // check env
        let env;
        match props.remove(SpaceMeta::KEY_ENV) {
            Some(Some(DictEntryGeneric::Map(m))) if props.is_empty() => env = m,
            None | Some(None) if props.is_empty() => env = IndexST::default(),
            _ => {
                return Err(DatabaseError::DdlCreateSpaceBadProperty);
            }
        }
        Ok(Procedure {
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
    pub fn validate_apply(gns: &super::GlobalNS, space: CreateSpace) -> DatabaseResult<()> {
        let Procedure { space_name, space } = Self::validate(space)?;
        let mut wl = gns._spaces().write();
        if wl.st_insert(space_name, Arc::new(space)) {
            Ok(())
        } else {
            Err(DatabaseError::DdlCreateSpaceAlreadyExists)
        }
    }
}

#[cfg(test)]
impl PartialEq for SpaceMeta {
    fn eq(&self, other: &Self) -> bool {
        let x = self.env.read();
        let y = other.env.read();
        &*x == &*y
    }
}

#[cfg(test)]
impl PartialEq for Space {
    fn eq(&self, other: &Self) -> bool {
        let self_mns = self.mns.read();
        let other_mns = other.mns.read();
        self.meta == other.meta && &*self_mns == &*other_mns
    }
}
