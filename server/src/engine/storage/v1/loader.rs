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

#[cfg(test)]
use crate::engine::storage::{
    common::interface::fs_traits::{FSInterface, FileOpen},
    v1::raw::journal::JournalWriter,
};
use crate::engine::{
    core::{EntityIDRef, GlobalNS},
    data::uuid::Uuid,
    error::RuntimeResult,
    fractal::error::ErrorContext,
    fractal::{FractalModelDriver, ModelDrivers, ModelUniqueID},
    storage::{
        common::interface::fs_imp::LocalFS,
        v1::{
            impls::gns::{GNSAdapter, GNSTransactionDriverAnyFS},
            raw::{batch_jrnl, journal, spec},
        },
    },
};

const GNS_FILE_PATH: &str = "gns.db-tlog";
const DATA_DIR: &str = "data";

pub struct SEInitState {
    pub txn_driver: GNSTransactionDriverAnyFS<LocalFS>,
    pub model_drivers: ModelDrivers<LocalFS>,
    pub gns: GlobalNS,
}

impl SEInitState {
    pub fn new(
        txn_driver: GNSTransactionDriverAnyFS<LocalFS>,
        model_drivers: ModelDrivers<LocalFS>,
        gns: GlobalNS,
    ) -> Self {
        Self {
            txn_driver,
            model_drivers,
            gns,
        }
    }
    pub fn try_init(is_new: bool) -> RuntimeResult<Self> {
        let gns = GlobalNS::empty();
        let gns_txn_driver = if is_new {
            journal::create_journal::<GNSAdapter, LocalFS, spec::GNSTransactionLogV1>(GNS_FILE_PATH)
        } else {
            journal::load_journal::<GNSAdapter, LocalFS, spec::GNSTransactionLogV1>(
                GNS_FILE_PATH,
                &gns,
            )
        }?;
        let mut model_drivers = ModelDrivers::new();
        let mut driver_guard = || {
            if is_new {
                std::fs::create_dir(DATA_DIR).inherit_set_dmsg("creating data directory")?;
            }
            if !is_new {
                let mut models = gns.idx_models().write();
                // this is an existing instance, so read in all data
                for (space_name, space) in gns.idx().read().iter() {
                    let space_uuid = space.get_uuid();
                    for model_name in space.models().iter() {
                        let model = models
                            .get_mut(&EntityIDRef::new(&space_name, &model_name))
                            .unwrap();
                        let path =
                            Self::model_path(space_name, space_uuid, model_name, model.get_uuid());
                        let persist_driver = batch_jrnl::reinit(&path, model).inherit_set_dmsg(
                            format!("failed to restore model data from journal in `{path}`"),
                        )?;
                        unsafe {
                            // UNSAFE(@ohsayan): all pieces of data are upgraded by now, so vacuum
                            model.model_mutator().vacuum_stashed();
                        }
                        let _ = model_drivers.insert(
                            ModelUniqueID::new(space_name, model_name, model.get_uuid()),
                            FractalModelDriver::init(persist_driver),
                        );
                    }
                }
            }
            RuntimeResult::Ok(())
        };
        if let Err(e) = driver_guard() {
            gns_txn_driver.close().unwrap();
            for (_, driver) in model_drivers {
                driver.close().unwrap();
            }
            return Err(e);
        }
        Ok(SEInitState::new(
            GNSTransactionDriverAnyFS::new(gns_txn_driver),
            model_drivers,
            gns,
        ))
    }
    pub fn model_path(
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> String {
        format!(
            "{}/data.db-btlog",
            Self::model_dir(space_name, space_uuid, model_name, model_uuid)
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

#[cfg(test)]
pub fn open_gns_driver<Fs: FSInterface>(
    path: &str,
    gns: &GlobalNS,
) -> RuntimeResult<FileOpen<JournalWriter<Fs, GNSAdapter>>> {
    journal::open_or_create_journal::<GNSAdapter, Fs, spec::GNSTransactionLogV1>(path, gns)
}
