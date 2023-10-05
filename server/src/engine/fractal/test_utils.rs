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
        config::SysConfig, CriticalTask, FractalModelDriver, GenericTask, GlobalInstanceLike,
        ModelUniqueID, Task,
    },
    crate::engine::{
        core::GlobalNS,
        data::uuid::Uuid,
        storage::{
            self,
            v1::{
                memfs::{NullFS, VirtualFS},
                RawFSInterface,
            },
        },
        txn::gns::GNSTransactionDriverAnyFS,
    },
    parking_lot::{Mutex, RwLock},
    std::collections::HashMap,
};

/// A `test` mode global implementation
pub struct TestGlobal<Fs: RawFSInterface = VirtualFS> {
    gns: GlobalNS,
    hp_queue: RwLock<Vec<Task<CriticalTask>>>,
    lp_queue: RwLock<Vec<Task<GenericTask>>>,
    #[allow(unused)]
    max_delta_size: usize,
    txn_driver: Mutex<GNSTransactionDriverAnyFS<Fs>>,
    model_drivers: RwLock<HashMap<ModelUniqueID, FractalModelDriver<Fs>>>,
    sys_cfg: SysConfig,
}

impl<Fs: RawFSInterface> TestGlobal<Fs> {
    fn new(
        gns: GlobalNS,
        max_delta_size: usize,
        txn_driver: GNSTransactionDriverAnyFS<Fs>,
    ) -> Self {
        Self {
            gns,
            hp_queue: RwLock::default(),
            lp_queue: RwLock::default(),
            max_delta_size,
            txn_driver: Mutex::new(txn_driver),
            model_drivers: RwLock::default(),
            sys_cfg: SysConfig::test_default(),
        }
    }
}

impl<Fs: RawFSInterface> TestGlobal<Fs> {
    pub fn new_with_driver_id(log_name: &str) -> Self {
        let gns = GlobalNS::empty();
        let driver = storage::v1::loader::open_gns_driver(log_name, &gns)
            .unwrap()
            .into_inner();
        Self::new(gns, 0, GNSTransactionDriverAnyFS::new(driver))
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

impl<Fs: RawFSInterface> GlobalInstanceLike for TestGlobal<Fs> {
    type FileSystem = Fs;
    fn namespace(&self) -> &GlobalNS {
        &self.gns
    }
    fn namespace_txn_driver(&self) -> &Mutex<GNSTransactionDriverAnyFS<Self::FileSystem>> {
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
    fn sys_cfg(&self) -> &super::config::SysConfig {
        &self.sys_cfg
    }
    fn initialize_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> crate::engine::error::RuntimeResult<()> {
        // create model dir
        Fs::fs_create_dir(&storage::v1::loader::SEInitState::model_dir(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        let driver =
            storage::v1::data_batch::create(&storage::v1::loader::SEInitState::model_path(
                space_name, space_uuid, model_name, model_uuid,
            ))?;
        self.model_drivers.write().insert(
            ModelUniqueID::new(space_name, model_name, model_uuid),
            FractalModelDriver::init(driver),
        );
        Ok(())
    }
}

impl<Fs: RawFSInterface> Drop for TestGlobal<Fs> {
    fn drop(&mut self) {
        let mut txn_driver = self.txn_driver.lock();
        txn_driver
            .__journal_mut()
            .__append_journal_close_and_close()
            .unwrap();
    }
}
