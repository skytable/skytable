/*
 * Created on Mon May 15 2023
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

//! Implementations of the Skytable Disk Storage Subsystem (SDSS)

use {
    self::safe_interfaces::LocalFS,
    super::{
        config::Configuration, core::GlobalNS, fractal::context, fractal::ModelDrivers,
        RuntimeResult,
    },
    std::path::Path,
};

mod common;
mod common_encoding;
// driver versions
pub mod v1;
pub mod v2;

pub mod safe_interfaces {
    #[cfg(test)]
    pub use super::common::interface::fs_test::{NullFS, VirtualFS};
    pub use super::{
        common::{
            interface::{fs_imp::LocalFS, fs_traits::FSInterface},
            paths_v1,
        },
        v2::impls::mdl_journal::StdModelBatch,
    };
}

/*
    loader impl
*/

pub use v2::impls::{gns_log::GNSDriver, mdl_journal::ModelDriver};

pub struct SELoaded {
    pub gns: GlobalNS,
    pub gns_driver: v2::impls::gns_log::GNSDriver<LocalFS>,
    pub model_drivers: ModelDrivers<LocalFS>,
}

pub fn load(cfg: &Configuration) -> RuntimeResult<SELoaded> {
    info!("loading databases");
    // first determine if this is a new install, an existing install or if it uses the old driver
    if Path::new(v1::SYSDB_PATH).is_file() {
        warn!("older storage format detected");
        // this is an old install
        info!("loading data");
        context::set_dmsg("loading storage-v1 in compatibility mode");
        let gns = v1::load_gns_prepare_migration()?;
        info!("loaded data. now upgrading to new storage format");
        context::set_dmsg("upgrading storage-v1 to storage-v2 format");
        return v2::recreate(gns);
    }
    if !Path::new(v2::GNS_PATH).is_file() {
        info!("initializing databases");
        context::set_dmsg("creating databases");
        // this is a new install
        v2::initialize_new(cfg)
    } else {
        info!("reinitializing databases");
        context::set_dmsg("loading databases");
        v2::restore()
    }
}
