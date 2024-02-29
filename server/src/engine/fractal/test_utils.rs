/*
 * Created on Wed Sep 13 2023
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
    super::{
        drivers::FractalGNSDriver, CriticalTask, GenericTask, GlobalInstanceLike, ModelUniqueID,
        Task,
    },
    crate::engine::{
        core::{EntityIDRef, GlobalNS},
        data::uuid::Uuid,
        error::ErrorKind,
        fractal::drivers::FractalModelDriver,
        storage::{
            safe_interfaces::{paths_v1, FileSystem, StdModelBatch},
            BatchStats, GNSDriver, ModelDriver,
        },
        RuntimeResult,
    },
    parking_lot::{Mutex, RwLock},
    std::collections::HashMap,
};

/// A `test` mode global implementation
pub struct TestGlobal {
    gns: GlobalNS,
    lp_queue: RwLock<Vec<Task<GenericTask>>>,
    #[allow(unused)]
    max_delta_size: usize,
    txn_driver: Mutex<FractalGNSDriver>,
    model_drivers: RwLock<HashMap<ModelUniqueID, super::drivers::FractalModelDriver>>,
    max_data_pressure: usize,
}

impl TestGlobal {
    fn new(gns: GlobalNS, max_delta_size: usize, txn_driver: GNSDriver) -> Self {
        Self {
            gns,
            lp_queue: RwLock::default(),
            max_delta_size,
            txn_driver: Mutex::new(FractalGNSDriver::new(txn_driver)),
            model_drivers: RwLock::default(),
            max_data_pressure: usize::MAX,
        }
    }
    pub fn set_max_data_pressure(&mut self, max_data_pressure: usize) {
        self.max_data_pressure = max_data_pressure;
    }
    /// Normally, model drivers are not loaded on startup because of shared global state. Calling this will attempt to load
    /// all model drivers
    pub fn load_model_drivers(&self) -> RuntimeResult<()> {
        let mut mdl_drivers = self.model_drivers.write();
        let space_idx = self.gns.idx().read();
        for (model_name, model) in self.gns.idx_models().read().iter() {
            let space_uuid = space_idx.get(model_name.space()).unwrap().get_uuid();
            let driver = ModelDriver::open_model_driver(
                model,
                &paths_v1::model_path(
                    model_name.space(),
                    space_uuid,
                    model_name.entity(),
                    model.get_uuid(),
                ),
            )?;
            assert!(mdl_drivers
                .insert(
                    ModelUniqueID::new(model_name.space(), model_name.entity(), model.get_uuid()),
                    FractalModelDriver::init(driver)
                )
                .is_none());
        }
        Ok(())
    }
}

impl TestGlobal {
    pub fn new_with_driver_id_instant_update(log_name: &str) -> Self {
        let mut me = Self::new_with_driver_id(log_name);
        me.set_max_data_pressure(1);
        me
    }
    pub fn new_with_driver_id(log_name: &str) -> Self {
        let gns = GlobalNS::empty();
        let driver = match GNSDriver::create_gns_with_name(log_name) {
            Ok(drv) => Ok(drv),
            Err(e) => match e.kind() {
                ErrorKind::IoError(e_) => match e_.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        GNSDriver::open_gns_with_name(log_name, &gns)
                    }
                    _ => Err(e),
                },
                _ => Err(e),
            },
        }
        .unwrap();
        Self::new(gns, 0, driver)
    }
}

impl GlobalInstanceLike for TestGlobal {
    fn state(&self) -> &GlobalNS {
        &self.gns
    }
    fn gns_driver(&self) -> &Mutex<FractalGNSDriver> {
        &self.txn_driver
    }
    fn taskmgr_post_high_priority(&self, task: Task<CriticalTask>) {
        match task.into_task() {
            CriticalTask::WriteBatch(mdl_id, count) => {
                let models = self.gns.idx_models().read();
                let mdl = models
                    .get(&EntityIDRef::new(mdl_id.space(), mdl_id.model()))
                    .unwrap();
                self.model_drivers
                    .read()
                    .get(&mdl_id)
                    .unwrap()
                    .batch_driver()
                    .lock()
                    .commit_with_ctx(StdModelBatch::new(mdl, count), BatchStats::new())
                    .unwrap();
            }
        }
    }
    fn taskmgr_post_standard_priority(&self, task: Task<GenericTask>) {
        self.lp_queue.write().push(task)
    }
    fn get_max_delta_size(&self) -> usize {
        self.max_data_pressure
    }
    fn purge_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
        skip_delete: bool,
    ) {
        let id = ModelUniqueID::new(space_name, model_name, model_uuid);
        self.model_drivers
            .write()
            .remove(&id)
            .expect("tried to remove non-existent model");
        if !skip_delete {
            self.taskmgr_post_standard_priority(Task::new(GenericTask::delete_model_dir(
                space_name, space_uuid, model_name, model_uuid,
            )));
        }
    }
    fn initialize_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> crate::engine::error::RuntimeResult<()> {
        // create model dir
        FileSystem::create_dir_all(&paths_v1::model_dir(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        let driver = ModelDriver::create_model_driver(&paths_v1::model_path(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        self.model_drivers.write().insert(
            ModelUniqueID::new(space_name, model_name, model_uuid),
            super::drivers::FractalModelDriver::init(driver),
        );
        Ok(())
    }
}

impl Drop for TestGlobal {
    fn drop(&mut self) {
        let mut txn_driver = self.txn_driver.lock();
        GNSDriver::close_driver(&mut txn_driver.txn_driver).unwrap();
        for (_, model_driver) in self.model_drivers.write().drain() {
            model_driver.close().unwrap();
        }
    }
}
