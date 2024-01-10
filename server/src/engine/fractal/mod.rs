/*
 * Created on Sat Sep 09 2023
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
    self::sys_store::SystemStore,
    super::{
        core::{dml::QueryExecMeta, model::Model, GlobalNS},
        data::uuid::Uuid,
        storage::{
            self,
            safe_interfaces::{FSInterface, LocalFS},
        },
        txn::gns::GNSTransactionDriverAnyFS,
    },
    crate::engine::error::RuntimeResult,
    parking_lot::{Mutex, RwLock},
    std::{collections::HashMap, fmt, mem::MaybeUninit},
    tokio::sync::mpsc::unbounded_channel,
};

pub mod context;
mod drivers;
pub mod error;
mod mgr;
pub mod sys_store;
#[cfg(test)]
pub mod test_utils;
mod util;
pub use {
    drivers::FractalModelDriver,
    mgr::{CriticalTask, GenericTask, Task, GENERAL_EXECUTOR_WINDOW},
    util::FractalToken,
};

pub type ModelDrivers<Fs> = HashMap<ModelUniqueID, drivers::FractalModelDriver<Fs>>;

/*
    global state init
*/

/// Returned by [`enable_and_start_all`]. This contains a [`Global`] handle that can be used to easily access global
/// data
pub struct GlobalStateStart {
    pub global: Global,
    pub boot: mgr::FractalBoot,
}

/// Enable all drivers and start all engines (or others that you must start)
///
/// ## Safety
///
/// Must be called iff this is the only thread calling it
pub unsafe fn load_and_enable_all(
    gns: GlobalNS,
    config: SystemStore<LocalFS>,
    gns_driver: GNSTransactionDriverAnyFS<LocalFS>,
    model_drivers: ModelDrivers<LocalFS>,
) -> GlobalStateStart {
    let model_cnt_on_boot = model_drivers.len();
    let gns_driver = drivers::FractalGNSDriver::new(gns_driver);
    let mdl_driver = RwLock::new(model_drivers);
    let (hp_sender, hp_recv) = unbounded_channel();
    let (lp_sender, lp_recv) = unbounded_channel();
    let global_state = GlobalState::new(
        gns,
        gns_driver,
        mdl_driver,
        mgr::FractalMgr::new(hp_sender, lp_sender, model_cnt_on_boot),
        config,
    );
    *Global::__gref_raw() = MaybeUninit::new(global_state);
    let token = Global::new();
    GlobalStateStart {
        global: token.clone(),
        boot: mgr::FractalBoot::prepare(token.clone(), lp_recv, hp_recv),
    }
}

/*
    global access
*/

/// Something that represents the global state
pub trait GlobalInstanceLike {
    type FileSystem: FSInterface;
    const FS_IS_NON_NULL: bool = Self::FileSystem::NOT_NULL;
    // stat
    fn get_max_delta_size(&self) -> usize;
    // global namespace
    fn namespace(&self) -> &GlobalNS;
    fn namespace_txn_driver(&self) -> &Mutex<GNSTransactionDriverAnyFS<Self::FileSystem>>;
    // model drivers
    fn initialize_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> RuntimeResult<()>;
    fn purge_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
        skip_delete: bool,
    );
    // taskmgr
    fn taskmgr_post_high_priority(&self, task: Task<CriticalTask>);
    fn taskmgr_post_standard_priority(&self, task: Task<GenericTask>);
    // default impls
    fn request_batch_resolve_if_cache_full(
        &self,
        space_name: &str,
        model_name: &str,
        model: &Model,
        hint: QueryExecMeta,
    ) {
        let current_delta_size = hint.delta_hint();
        let index_size = model.primary_index().count();
        let five = (index_size as f64 * 0.05) as usize;
        let max_delta = five.max(self.get_max_delta_size());
        if current_delta_size >= max_delta {
            let obtained_delta_size = model
                .delta_state()
                .__fractal_take_full_from_data_delta(FractalToken::new());
            self.taskmgr_post_high_priority(Task::new(CriticalTask::WriteBatch(
                ModelUniqueID::new(space_name, model_name, model.get_uuid()),
                obtained_delta_size,
            )));
        }
    }
    // config handle
    fn sys_store(&self) -> &SystemStore<Self::FileSystem>;
}

impl GlobalInstanceLike for Global {
    type FileSystem = LocalFS;
    // ns
    fn namespace(&self) -> &GlobalNS {
        self._namespace()
    }
    fn namespace_txn_driver(&self) -> &Mutex<GNSTransactionDriverAnyFS<Self::FileSystem>> {
        self.get_state().gns_driver.txn_driver()
    }
    // taskmgr
    fn taskmgr_post_high_priority(&self, task: Task<CriticalTask>) {
        self._post_high_priority_task(task)
    }
    fn taskmgr_post_standard_priority(&self, task: Task<GenericTask>) {
        self._post_standard_priority_task(task)
    }
    // stat
    fn get_max_delta_size(&self) -> usize {
        self._get_max_delta_size()
    }
    // sys
    fn sys_store(&self) -> &SystemStore<Self::FileSystem> {
        &self.get_state().config
    }
    // model
    fn purge_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
        skip_delete: bool,
    ) {
        let id = ModelUniqueID::new(space_name, model_name, model_uuid);
        self.get_state()
            .mdl_driver
            .write()
            .remove(&id)
            .expect("tried to remove non existent driver");
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
    ) -> RuntimeResult<()> {
        // create dir
        LocalFS::fs_create_dir(&storage::v1::loader::SEInitState::model_dir(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        // init driver
        let driver =
            storage::v1::data_batch::create(&storage::v1::loader::SEInitState::model_path(
                space_name, space_uuid, model_name, model_uuid,
            ))?;
        self.get_state().mdl_driver.write().insert(
            ModelUniqueID::new(space_name, model_name, model_uuid),
            drivers::FractalModelDriver::init(driver),
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// A handle to the global state
pub struct Global(());

impl Global {
    unsafe fn new() -> Self {
        Self(())
    }
    fn get_state(&self) -> &'static GlobalState {
        unsafe { self.__gref() }
    }
    /// Returns a handle to the [`GlobalNS`]
    fn _namespace(&self) -> &'static GlobalNS {
        &unsafe { self.__gref() }.gns
    }
    /// Post an urgent task
    fn _post_high_priority_task(&self, task: Task<CriticalTask>) {
        self.get_state().fractal_mgr().post_high_priority(task)
    }
    /// Post a task with normal priority
    ///
    /// NB: It is not guaranteed that the task will remain as a low priority task because the scheduler can choose
    /// to promote the task to a high priority task, if it deems necessary.
    fn _post_standard_priority_task(&self, task: Task<GenericTask>) {
        self.get_state().fractal_mgr().post_low_priority(task)
    }
    /// Returns the maximum size a model's delta size can hit before it should immediately issue a batch write request
    /// to avoid memory pressure
    fn _get_max_delta_size(&self) -> usize {
        self.get_state()
            .fractal_mgr()
            .get_rt_stat()
            .per_mdl_delta_max_size()
    }
    unsafe fn __gref_raw() -> &'static mut MaybeUninit<GlobalState> {
        static mut G: MaybeUninit<GlobalState> = MaybeUninit::uninit();
        &mut G
    }
    unsafe fn __gref(&self) -> &'static GlobalState {
        Self::__gref_raw().assume_init_ref()
    }
    pub unsafe fn unload_all(self) {
        // TODO(@ohsayan): handle errors
        let GlobalState {
            gns_driver,
            mdl_driver,
            ..
        } = Self::__gref_raw().assume_init_read();
        let gns_driver = gns_driver.txn_driver.into_inner().into_inner();
        let mdl_drivers = mdl_driver.into_inner();
        gns_driver.close().unwrap();
        for (_, driver) in mdl_drivers {
            driver.close().unwrap();
        }
    }
}

/*
    global state
*/

/// The global state
struct GlobalState {
    gns: GlobalNS,
    gns_driver: drivers::FractalGNSDriver<LocalFS>,
    mdl_driver: RwLock<ModelDrivers<LocalFS>>,
    task_mgr: mgr::FractalMgr,
    config: SystemStore<LocalFS>,
}

impl GlobalState {
    fn new(
        gns: GlobalNS,
        gns_driver: drivers::FractalGNSDriver<LocalFS>,
        mdl_driver: RwLock<ModelDrivers<LocalFS>>,
        task_mgr: mgr::FractalMgr,
        config: SystemStore<LocalFS>,
    ) -> Self {
        Self {
            gns,
            gns_driver,
            mdl_driver,
            task_mgr,
            config,
        }
    }
    pub(self) fn get_mdl_drivers(&self) -> &RwLock<ModelDrivers<LocalFS>> {
        &self.mdl_driver
    }
    pub(self) fn fractal_mgr(&self) -> &mgr::FractalMgr {
        &self.task_mgr
    }
}

// these impls are completely fine
unsafe impl Send for GlobalState {}
unsafe impl Sync for GlobalState {}

/// An unique signature that identifies a model, and only that model (guaranteed by the OS's random source)
// NB(@ohsayan): if there are collisions, which I absolutely do not expect any instances of, pool in the space's UUID
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ModelUniqueID {
    space: Box<str>,
    model: Box<str>,
    uuid: Uuid,
}

impl fmt::Display for ModelUniqueID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "model-{}@{}", self.model(), self.space())
    }
}

impl ModelUniqueID {
    /// Create a new unique model ID
    pub fn new(space: &str, model: &str, uuid: Uuid) -> Self {
        Self {
            space: space.into(),
            model: model.into(),
            uuid,
        }
    }
    /// Returns the space name
    pub fn space(&self) -> &str {
        self.space.as_ref()
    }
    /// Returns the model name
    pub fn model(&self) -> &str {
        self.model.as_ref()
    }
    /// Returns the uuid
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}
