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

use crate::engine::{
    core::{model::ModelData, space::Space},
    data::uuid::Uuid,
};

#[derive(Debug, Clone, Copy)]
pub struct SpaceIDRef<'a> {
    uuid: Uuid,
    name: &'a str,
}

impl<'a> SpaceIDRef<'a> {
    pub fn with_uuid(name: &'a str, uuid: Uuid) -> Self {
        Self { uuid, name }
    }
    pub fn new(name: &'a str, space: &Space) -> Self {
        Self::with_uuid(name, space.get_uuid())
    }
    pub fn name(&self) -> &str {
        self.name
    }
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ModelIDRef<'a> {
    space_id: SpaceIDRef<'a>,
    model_name: &'a str,
    model_uuid: Uuid,
    model_version: u64,
}

impl<'a> ModelIDRef<'a> {
    pub fn new_ref(
        space_name: &'a str,
        space: &'a Space,
        model_name: &'a str,
        model: &'a ModelData,
    ) -> ModelIDRef<'a> {
        ModelIDRef::new(
            SpaceIDRef::new(space_name, space),
            model_name,
            model.get_uuid(),
            model.delta_state().schema_current_version().value_u64(),
        )
    }
    pub fn new(
        space_id: SpaceIDRef<'a>,
        model_name: &'a str,
        model_uuid: Uuid,
        model_version: u64,
    ) -> Self {
        Self {
            space_id,
            model_name,
            model_uuid,
            model_version,
        }
    }
    pub fn space_id(&self) -> SpaceIDRef {
        self.space_id
    }
    pub fn model_name(&self) -> &str {
        self.model_name
    }
    pub fn model_uuid(&self) -> Uuid {
        self.model_uuid
    }
    pub fn model_version(&self) -> u64 {
        self.model_version
    }
}
