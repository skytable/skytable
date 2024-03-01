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

use {
    crate::engine::{
        core::{EntityIDRef, GNSData},
        error::RuntimeResult,
        fractal::{error::ErrorContext, ModelUniqueID},
        storage::{
            common::paths_v1,
            v1::raw::{
                batch_jrnl,
                journal::{raw as raw_journal, GNSAdapter},
                spec,
            },
        },
    },
    std::collections::HashMap,
};

pub fn load_gns() -> RuntimeResult<GNSData> {
    let gns = GNSData::empty();
    let gns_txn_driver =
        raw_journal::load_journal::<GNSAdapter, spec::GNSTransactionLogV1>(super::GNS_PATH, &gns)?;
    let mut model_drivers = HashMap::new();
    let mut driver_guard = || {
        let mut models = gns.idx_models().write();
        // this is an existing instance, so read in all data
        for (space_name, space) in gns.idx().read().iter() {
            let space_uuid = space.get_uuid();
            for model_name in space.models().iter() {
                let model = models
                    .get_mut(&EntityIDRef::new(&space_name, &model_name))
                    .unwrap();
                let path = paths_v1::model_path(
                    space_name,
                    space_uuid,
                    model_name,
                    model.data().get_uuid(),
                );
                let persist_driver = batch_jrnl::reinit(&path, model.data()).inherit_set_dmsg(
                    format!("failed to restore model data from journal in `{path}`"),
                )?;
                unsafe {
                    // UNSAFE(@ohsayan): all pieces of data are upgraded by now, so vacuum
                    model.data_mut().model_mutator().vacuum_stashed();
                }
                let _ = model_drivers.insert(
                    ModelUniqueID::new(space_name, model_name, model.data().get_uuid()),
                    persist_driver,
                );
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
    // close all drivers
    gns_txn_driver.close().unwrap();
    for (_, driver) in model_drivers {
        driver.close().unwrap();
    }
    Ok(gns)
}
