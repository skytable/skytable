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
    super::{
        core::{dml::QueryExecMeta, model::ModelData, GlobalNS},
        data::uuid::Uuid,
        storage::{
            safe_interfaces::{paths_v1, FileSystem},
            GNSDriver, ModelDriver,
        },
    },
    crate::{engine::error::RuntimeResult, util::compiler},
    std::{
        fmt,
        mem::MaybeUninit,
        ptr::addr_of_mut,
        sync::atomic::{AtomicUsize, Ordering},
    },
    tokio::sync::mpsc::unbounded_channel,
};

pub mod context;
mod drivers;
pub mod error;
mod mgr;
#[cfg(test)]
pub mod test_utils;
mod util;
pub use {
    drivers::{FractalGNSDriver, FractalModelDriver},
    mgr::{CriticalTask, GenericTask, Task, GENERAL_EXECUTOR_WINDOW},
    util::FractalToken,
};

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
pub unsafe fn load_and_enable_all(gns: GlobalNS) -> GlobalStateStart {
    let model_cnt_on_boot = gns.namespace().idx_models().read().len();
    let (hp_sender, hp_recv) = unbounded_channel();
    let (lp_sender, lp_recv) = unbounded_channel();
    let global_state = GlobalState::new(
        gns,
        mgr::FractalMgr::new(hp_sender, lp_sender, model_cnt_on_boot),
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

pub struct GlobalHealth {
    faults: AtomicUsize,
}

impl GlobalHealth {
    pub fn status_okay(&self) -> bool {
        self.faults.load(Ordering::Acquire) == 0
    }
    const fn new() -> Self {
        Self {
            faults: AtomicUsize::new(0),
        }
    }
    fn report_fault(&self) {
        self.faults.fetch_add(1, Ordering::Release);
    }
    fn report_recovery(&self) {
        self.faults.fetch_sub(1, Ordering::Release);
    }
    pub fn report_removal_of_faulty_source(&self) {
        self.report_recovery()
    }
}

/// Something that represents the global state
pub trait GlobalInstanceLike {
    // stat
    fn health(&self) -> &GlobalHealth;
    fn get_max_delta_size(&self) -> usize;
    // global namespace
    fn state(&self) -> &GlobalNS;
    fn initialize_space(&self, space_name: &str, space_uuid: Uuid) -> RuntimeResult<()> {
        e!(FileSystem::create_dir_all(&paths_v1::space_dir(
            space_name, space_uuid
        )))
    }
    // model drivers
    fn initialize_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> RuntimeResult<FractalModelDriver>;
    fn purge_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    );
    // taskmgr
    fn taskmgr_post_high_priority(&self, task: Task<CriticalTask>);
    fn taskmgr_post_standard_priority(&self, task: Task<GenericTask>);
    // default impls
    #[inline(always)]
    fn request_batch_resolve_if_cache_full(
        &self,
        space_name: &str,
        model_name: &str,
        model: &ModelData,
        hint: QueryExecMeta,
    ) {
        // check if we need to sync
        let r_tolerated_change = hint.delta_hint() >= self.get_max_delta_size();
        let r_percent_change = (hint.delta_hint() >= ((model.primary_index().count() / 100) * 5))
            & (r_tolerated_change);
        if compiler::unlikely(r_tolerated_change | r_percent_change) {
            // do not inline this path as we expect sufficient memory to be present and/or the background service
            // to pick this up
            compiler::cold_call(|| {
                let obtained_delta_size = model
                    .delta_state()
                    .__fractal_take_full_from_data_delta(FractalToken::new());
                self.taskmgr_post_high_priority(Task::new(CriticalTask::WriteBatch(
                    ModelUniqueID::new(space_name, model_name, model.get_uuid()),
                    obtained_delta_size,
                )));
            })
        }
    }
}

impl GlobalInstanceLike for Global {
    // ns
    fn state(&self) -> &GlobalNS {
        self._namespace()
    }
    fn health(&self) -> &GlobalHealth {
        &unsafe {
            // UNSAFE(@ohsayan): we expect the system to be initialized
            self.__gref()
        }
        .health
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
    // model
    fn purge_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) {
        self.taskmgr_post_standard_priority(Task::new(GenericTask::delete_model_dir(
            space_name, space_uuid, model_name, model_uuid,
        )));
    }
    fn initialize_model_driver(
        &self,
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> RuntimeResult<FractalModelDriver> {
        // create dir
        FileSystem::create_dir(&paths_v1::model_dir(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        // init driver
        let driver = ModelDriver::create_model_driver(&paths_v1::model_path(
            space_name, space_uuid, model_name, model_uuid,
        ))?;
        Ok(FractalModelDriver::init(driver))
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
    unsafe fn __gref_raw() -> *mut MaybeUninit<GlobalState> {
        static mut G: MaybeUninit<GlobalState> = MaybeUninit::uninit();
        addr_of_mut!(G)
    }
    unsafe fn __gref(&self) -> &'static GlobalState {
        (&*Self::__gref_raw()).assume_init_ref()
    }
    pub unsafe fn unload_all(self) {
        // TODO(@ohsayan): handle errors
        let GlobalState { gns, .. } = Self::__gref_raw().read().assume_init();
        let mut gns_driver = gns.gns_driver().txn_driver.lock();
        GNSDriver::close_driver(&mut gns_driver).unwrap();
        for mdl in gns
            .namespace()
            .idx_models()
            .write()
            .drain()
            .map(|(_, mdl)| mdl)
        {
            mdl.into_driver().close().unwrap();
        }
    }
}

/*
    global state
*/

/// The global state
struct GlobalState {
    gns: GlobalNS,
    task_mgr: mgr::FractalMgr,
    health: GlobalHealth,
}

impl GlobalState {
    fn new(gns: GlobalNS, task_mgr: mgr::FractalMgr) -> Self {
        Self {
            gns,
            task_mgr,
            health: GlobalHealth::new(),
        }
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

pub struct ModelUniqueIDRef<'a> {
    space: &'a str,
    model: &'a str,
    uuid: Uuid,
}

impl<'a> ModelUniqueIDRef<'a> {
    pub fn new(space: &'a str, model: &'a str, uuid: Uuid) -> Self {
        Self { space, model, uuid }
    }
}

impl fmt::Display for ModelUniqueID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "model-{}@{}", self.model(), self.space())
    }
}

impl<'a> From<ModelUniqueIDRef<'a>> for ModelUniqueID {
    fn from(uid: ModelUniqueIDRef<'a>) -> Self {
        Self::new(uid.space, uid.model, uid.uuid)
    }
}

impl<'a> From<&'a ModelUniqueID> for ModelUniqueIDRef<'a> {
    fn from(uid: &'a ModelUniqueID) -> Self {
        Self::new(uid.space(), uid.model(), uid.uuid())
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
