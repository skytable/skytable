/*
 * Created on Sat Feb 10 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use crate::engine::{core::space::Space, data::DictGeneric, txn::SpaceIDRef};

impl_gns_event!(CreateSpaceTxn<'_> = CreateSpace, AlterSpaceTxn<'_> = AlterSpace, DropSpaceTxn<'_> = DropSpace);

#[derive(Clone, Copy)]
/// Transaction commit payload for a `create space ...` query
pub struct CreateSpaceTxn<'a> {
    space_meta: &'a DictGeneric,
    space_name: &'a str,
    space: &'a Space,
}

impl<'a> CreateSpaceTxn<'a> {
    pub const fn new(space_meta: &'a DictGeneric, space_name: &'a str, space: &'a Space) -> Self {
        Self {
            space_meta,
            space_name,
            space,
        }
    }
    pub fn space_meta(&self) -> &DictGeneric {
        self.space_meta
    }
    pub fn space_name(&self) -> &str {
        self.space_name
    }
    pub fn space(&self) -> &Space {
        self.space
    }
}

#[derive(Clone, Copy)]
/// Transaction payload for an `alter space ...` query
pub struct AlterSpaceTxn<'a> {
    space_id: SpaceIDRef<'a>,
    updated_props: &'a DictGeneric,
}

impl<'a> AlterSpaceTxn<'a> {
    pub const fn new(space_id: SpaceIDRef<'a>, updated_props: &'a DictGeneric) -> Self {
        Self {
            space_id,
            updated_props,
        }
    }
    pub fn space_id(&self) -> SpaceIDRef<'_> {
        self.space_id
    }
    pub fn updated_props(&self) -> &DictGeneric {
        self.updated_props
    }
}

#[derive(Clone, Copy)]
/// Transaction commit payload for a `drop space ...` query
pub struct DropSpaceTxn<'a> {
    space_id: SpaceIDRef<'a>,
}

impl<'a> DropSpaceTxn<'a> {
    pub const fn new(space_id: SpaceIDRef<'a>) -> Self {
        Self { space_id }
    }
    pub fn space_id(&self) -> SpaceIDRef<'_> {
        self.space_id
    }
}
