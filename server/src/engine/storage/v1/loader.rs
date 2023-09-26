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

use crate::engine::{
    core::GlobalNS,
    data::uuid::Uuid,
    fractal::{FractalModelDriver, ModelDrivers, ModelUniqueID},
    storage::v1::{
        batch_jrnl,
        journal::{self, JournalWriter},
        rw::{FileOpen, RawFSInterface},
        spec, LocalFS, SDSSErrorContext, SDSSResult,
    },
    txn::gns::{GNSAdapter, GNSTransactionDriverAnyFS},
};

const GNS_FILE_PATH: &str = "gns.db-tlog";
const GNS_LOG_VERSION_CODE: u32 = 0;

pub struct SEInitState {
    pub new_instance: bool,
    pub txn_driver: GNSTransactionDriverAnyFS<super::LocalFS>,
    pub model_drivers: ModelDrivers<LocalFS>,
    pub gns: GlobalNS,
}

impl SEInitState {
    pub fn new(
        new_instance: bool,
        txn_driver: GNSTransactionDriverAnyFS<super::LocalFS>,
        model_drivers: ModelDrivers<LocalFS>,
        gns: GlobalNS,
    ) -> Self {
        Self {
            new_instance,
            txn_driver,
            model_drivers,
            gns,
        }
    }
    pub fn try_init() -> SDSSResult<Self> {
        let gns = GlobalNS::empty();
        let gns_txn_driver = open_gns_driver(GNS_FILE_PATH, &gns)?;
        let new_instance = gns_txn_driver.is_created();
        let mut model_drivers = ModelDrivers::new();
        if !new_instance {
            // this is an existing instance, so read in all data
            for (space_name, space) in gns.spaces().read().iter() {
                let space_uuid = space.get_uuid();
                for (model_name, model) in space.models().read().iter() {
                    let path =
                        Self::model_path(space_name, space_uuid, model_name, model.get_uuid());
                    let persist_driver = match batch_jrnl::reinit(&path, model) {
                        Ok(j) => j,
                        Err(e) => {
                            return Err(e.with_extra(format!(
                                "failed to restore model data from journal in `{path}`"
                            )))
                        }
                    };
                    let _ = model_drivers.insert(
                        ModelUniqueID::new(space_name, model_name, model.get_uuid()),
                        FractalModelDriver::init(persist_driver),
                    );
                }
            }
        }
        Ok(SEInitState::new(
            new_instance,
            GNSTransactionDriverAnyFS::new(gns_txn_driver.into_inner()),
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

pub fn open_gns_driver<Fs: RawFSInterface>(
    path: &str,
    gns: &GlobalNS,
) -> SDSSResult<FileOpen<JournalWriter<Fs, GNSAdapter>>> {
    journal::open_journal::<GNSAdapter, Fs, spec::GNSTransactionLogV1>(path, gns)
}
