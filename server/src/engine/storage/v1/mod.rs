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

//! SDSS based storage engine driver v1 ([`versions::v1`])
//!
//! Target tags: `0.8.0-beta`, `0.8.0-beta.2`, `0.8.0-beta.3`

mod loader;
pub mod raw;

use {
    self::raw::sysdb::RestoredSystemDatabase,
    super::common::interface::fs::FileSystem,
    crate::{
        engine::{core::GNSData, RuntimeResult},
        util,
    },
};

pub const GNS_PATH: &str = "gns.db-tlog";
pub const SYSDB_PATH: &str = "sys.db";
pub const DATA_DIR: &str = "data";

pub fn load_gns_prepare_migration() -> RuntimeResult<GNSData> {
    // load gns
    let gns = loader::load_gns()?;
    // load sysdb
    let RestoredSystemDatabase { users, .. } =
        raw::sysdb::RestoredSystemDatabase::restore(SYSDB_PATH)?;
    for (user, phash) in users {
        gns.sys_db().__raw_create_user(user, phash);
    }
    // now move all our files into a backup directory
    let backup_dir_path = format!(
        "backups/{}",
        util::time_now_with_postfix("before_upgrade_to_v2")
    );
    // move data folder
    FileSystem::create_dir_all(&backup_dir_path)?;
    util::os::move_files_recursively("data", &format!("{backup_dir_path}/data"))?;
    // move GNS
    FileSystem::rename(GNS_PATH, &format!("{backup_dir_path}/{GNS_PATH}"))?;
    // move sysdb
    FileSystem::rename(SYSDB_PATH, &format!("{backup_dir_path}/{SYSDB_PATH}"))?;
    Ok(gns)
}
