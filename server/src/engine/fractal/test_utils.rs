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
        core::GlobalNS,
        data::uuid::Uuid,
        storage::{
            safe_interfaces::{paths_v1, FSInterface, NullFS, VirtualFS},
            GNSDriver, ModelDriver,
        },
    },
    parking_lot::{Mutex, RwLock},
    std::collections::HashMap,
};

/// A `test` mode global implementation
pub struct TestGlobal<Fs: FSInterface = VirtualFS> {
    gns: GlobalNS,
    hp_queue: RwLock<Vec<Task<CriticalTask>>>,
    lp_queue: RwLock<Vec<Task<GenericTask>>>,
    #[allow(unused)]
    max_delta_size: usize,
    txn_driver: Mutex<FractalGNSDriver<Fs>>,
    model_drivers: RwLock<HashMap<ModelUniqueID, super::drivers::FractalModelDriver<Fs>>>,
}

impl<Fs: FSInterface> TestGlobal<Fs> {
    fn new(gns: GlobalNS, max_delta_size: usize, txn_driver: GNSDriver<Fs>) -> Self {
        Self {
            gns,
            hp_queue: RwLock::default(),
            lp_queue: RwLock::default(),
            max_delta_size,
            txn_driver: Mutex::new(FractalGNSDriver::new(txn_driver)),
            model_drivers: RwLock::default(),
        }
    }
}

impl<Fs: FSInterface> TestGlobal<Fs> {
    pub fn new_with_driver_id(log_name: &str) -> Self {
        let gns = GlobalNS::empty();
        let driver = GNSDriver::open_gns_with_name(log_name, &gns).unwrap();
        Self::new(gns, 0, driver)
    }
}

impl TestGlobal<VirtualFS> {
    pub fn new_with_vfs_driver(log_name: &str) -> Self {
        Self::new_with_driver_id(log_name)
    }
}

impl TestGlobal<NullFS> {
    pub fn new_with_nullfs_driver(log_name: &str) -> Self {
        Self::new_with_driver_id(log_name)
    }
    pub fn new_with_tmp_nullfs_driver() -> Self {
        Self::new_with_nullfs_driver("")
    }
}

impl<Fs: FSInterface> GlobalInstanceLike for TestGlobal<Fs> {
    type FileSystem = Fs;
    fn state(&self) -> &GlobalNS {
        &self.gns
    }
    fn gns_driver(&self) -> &Mutex<FractalGNSDriver<Self::FileSystem>> {
        &self.txn_driver
    }
    fn taskmgr_post_high_priority(&self, task: Task<CriticalTask>) {
        self.hp_queue.write().push(task)
    }
    fn taskmgr_post_standard_priority(&self, task: Task<GenericTask>) {
        self.lp_queue.write().push(task)
    }
    fn get_max_delta_size(&self) -> usize {
        100
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
        Fs::fs_create_dir(&paths_v1::model_dir(
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

impl<Fs: FSInterface> Drop for TestGlobal<Fs> {
    fn drop(&mut self) {
        let mut txn_driver = self.txn_driver.lock();
        GNSDriver::close_driver(&mut txn_driver.txn_driver).unwrap()
    }
}
