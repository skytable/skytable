/*
 * Created on Tue Jan 09 2024
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

pub mod checksum;
pub mod interface;
pub mod sdss;
pub mod static_meta;
pub mod versions;

pub mod paths_v1 {
    use crate::engine::data::uuid::Uuid;
    pub fn model_path(
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> String {
        format!(
            "{}/data.db-btlog",
            self::model_dir(space_name, space_uuid, model_name, model_uuid)
        )
    }
    pub fn model_dir(
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> String {
        format!("data/{space_name}-{space_uuid}/mdl_{model_name}-{model_uuid}")
    }
    pub fn space_dir(space_name: &str, space_uuid: Uuid) -> String {
        format!("data/{space_name}-{space_uuid}")
    }
}
