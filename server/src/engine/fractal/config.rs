/*
 * Created on Sun Sep 10 2023
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

use crate::engine::storage::v1::header_meta::HostRunMode;

pub struct ServerConfig {
    host_settings_version: u32,
    host_run_mode: HostRunMode,
    host_startup_counter: u64,
}

impl ServerConfig {
    pub fn new(
        host_settings_version: u32,
        host_run_mode: HostRunMode,
        host_startup_counter: u64,
    ) -> Self {
        Self {
            host_settings_version,
            host_run_mode,
            host_startup_counter,
        }
    }
}
