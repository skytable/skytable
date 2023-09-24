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

use crate::engine::storage::v1::LocalFS;

use {
    super::ModelUniqueID,
    crate::{
        engine::{
            core::model::{delta::DataDelta, Model},
            data::uuid::Uuid,
        },
        util::os,
    },
    std::path::PathBuf,
    tokio::{
        fs,
        sync::mpsc::{UnboundedReceiver, UnboundedSender},
        task::JoinHandle,
    },
};

/// A task for the [`FractalMgr`] to perform
pub struct Task<T> {
    threshold: usize,
    task: T,
}

impl<T> Task<T> {
    const THRESHOLD: usize = 10;
    /// Create a new task with the default threshold
    pub fn new(task: T) -> Self {
        Self::with_threshold(task, Self::THRESHOLD)
    }
    /// Create a task with the given threshold
    fn with_threshold(task: T, threshold: usize) -> Self {
        Self { threshold, task }
    }
}

/// A general task
pub enum GenericTask {
    /// Delete a single file
    DeleteFile(PathBuf),
    /// Delete a directory (and all its children)
    DeleteDirAll(PathBuf),
}

impl GenericTask {
    pub fn delete_model_dir(
        space_name: &str,
        space_uuid: Uuid,
        model_name: &str,
        model_uuid: Uuid,
    ) -> Self {
        Self::DeleteDirAll(
            crate::engine::storage::v1::loader::SEInitState::model_dir(
                space_name, space_uuid, model_name, model_uuid,
            )
            .into(),
        )
    }
    pub fn delete_space_dir(space_name: &str, space_uuid: Uuid) -> Self {
        Self::DeleteDirAll(
            crate::engine::storage::v1::loader::SEInitState::space_dir(space_name, space_uuid)
                .into(),
        )
    }
}

/// A critical task
pub enum CriticalTask {
    /// Write a new data batch
    WriteBatch(ModelUniqueID, usize),
}

/// The task manager
pub(super) struct FractalMgr {
    hp_dispatcher: UnboundedSender<Task<CriticalTask>>,
    general_dispatcher: UnboundedSender<Task<GenericTask>>,
    runtime_stats: FractalRTStat,
}

pub(super) struct FractalRTStat {
    mem_free_bytes: u64,
    per_mdl_delta_max_size: usize,
}

impl FractalRTStat {
    fn init(model_cnt: usize) -> Self {
        let mem_free_bytes = os::free_memory_in_bytes();
        let allowed_delta_limit = mem_free_bytes as f64 * 0.02;
        let per_model_limit = allowed_delta_limit / model_cnt.max(1) as f64;
        Self {
            mem_free_bytes,
            per_mdl_delta_max_size: per_model_limit as usize / sizeof!(DataDelta),
        }
    }
    pub(super) fn mem_free_bytes(&self) -> u64 {
        self.mem_free_bytes
    }
    pub(super) fn per_mdl_delta_max_size(&self) -> usize {
        self.per_mdl_delta_max_size
    }
}

impl FractalMgr {
    pub(super) fn new(
        hp_dispatcher: UnboundedSender<Task<CriticalTask>>,
        general_dispatcher: UnboundedSender<Task<GenericTask>>,
        model_count: usize,
    ) -> Self {
        Self {
            hp_dispatcher,
            general_dispatcher,
            runtime_stats: FractalRTStat::init(model_count),
        }
    }
    pub fn get_rt_stat(&self) -> &FractalRTStat {
        &self.runtime_stats
    }
    /// Add a high priority task to the queue
    ///
    /// ## Panics
    ///
    /// This will panic if the high priority executor has crashed or exited
    pub fn post_high_priority(&self, task: Task<CriticalTask>) {
        self.hp_dispatcher.send(task).unwrap()
    }
    /// Add a low priority task to the queue
    ///
    /// ## Panics
    ///
    /// This will panic if the low priority executor has crashed or exited
    pub fn post_low_priority(&self, task: Task<GenericTask>) {
        self.general_dispatcher.send(task).unwrap()
    }
}

/// Handles to all the services that fractal needs. These are spawned on the default runtime
pub struct FractalServiceHandles {
    pub hp_handle: JoinHandle<()>,
    pub lp_handle: JoinHandle<()>,
}

impl FractalMgr {
    /// Start all background services, and return their handles
    pub(super) fn start_all(
        global: super::Global,
        lp_receiver: UnboundedReceiver<Task<GenericTask>>,
        hp_receiver: UnboundedReceiver<Task<CriticalTask>>,
    ) -> FractalServiceHandles {
        let fractal_mgr = global.get_state().fractal_mgr();
        let global_1 = global.__global_clone();
        let global_2 = global.__global_clone();
        let hp_handle = tokio::spawn(async move {
            FractalMgr::hp_executor_svc(fractal_mgr, global_1, hp_receiver).await
        });
        let lp_handle = tokio::spawn(async move {
            FractalMgr::general_executor_svc(fractal_mgr, global_2, lp_receiver).await
        });
        FractalServiceHandles {
            hp_handle,
            lp_handle,
        }
    }
}

// services
impl FractalMgr {
    const GENERAL_EXECUTOR_WINDOW: u64 = 5 * 60;
    /// The high priority executor service runs in the background to take care of high priority tasks and take any
    /// appropriate action. It will exclusively own the high priority queue since it is the only broker that is
    /// allowed to perform HP tasks
    pub async fn hp_executor_svc(
        &'static self,
        global: super::Global,
        mut receiver: UnboundedReceiver<Task<CriticalTask>>,
    ) {
        loop {
            let Some(Task { threshold, task }) = receiver.recv().await else {
                return; // all handles closed; nothing left to do
            };
            // TODO(@ohsayan): check threshold and update hooks
            match task {
                CriticalTask::WriteBatch(model_id, observed_size) => {
                    let mdl_drivers = global.get_state().get_mdl_drivers().read();
                    let Some(mdl_driver) = mdl_drivers.get(&model_id) else {
                        // because we maximize throughput, the model driver may have been already removed but this task
                        // was way behind in the queue
                        continue;
                    };
                    let res = global._namespace().with_model(
                        (model_id.space(), model_id.model()),
                        |model| {
                            if model.get_uuid() != model_id.uuid() {
                                // once again, throughput maximization will lead to, in extremely rare cases, this
                                // branch returning. but it is okay
                                return Ok(());
                            }
                            // mark that we're taking these deltas
                            model.delta_state().__fractal_take_from_data_delta(
                                observed_size,
                                super::FractalToken::new(),
                            );
                            Self::try_write_model_data_batch(model, observed_size, mdl_driver)
                        },
                    );
                    match res {
                        Ok(()) => {}
                        Err(_) => {
                            log::error!(
                                "Error writing data batch for model {}. Retrying...",
                                model_id.uuid()
                            );
                            // enqueue again for retrying
                            self.hp_dispatcher
                                .send(Task::with_threshold(
                                    CriticalTask::WriteBatch(model_id, observed_size),
                                    threshold - 1,
                                ))
                                .unwrap();
                        }
                    }
                }
            }
        }
    }
    /// The general priority task or simply the general queue takes of care of low priority and other standard priority
    /// tasks (such as those running on a schedule). A low priority task can be promoted to a high priority task, and the
    /// discretion of the GP executor. Similarly, the executor owns the general purpose task queue since it is the sole broker
    /// for such tasks
    pub async fn general_executor_svc(
        &'static self,
        global: super::Global,
        mut lpq: UnboundedReceiver<Task<GenericTask>>,
    ) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(Self::GENERAL_EXECUTOR_WINDOW)) => {
                    let mdl_drivers = global.get_state().get_mdl_drivers().read();
                    for (model_id, driver) in mdl_drivers.iter() {
                        let mut observed_len = 0;
                        let res = global._namespace().with_model((model_id.space(), model_id.model()), |model| {
                            if model.get_uuid() != model_id.uuid() {
                                // once again, throughput maximization will lead to, in extremely rare cases, this
                                // branch returning. but it is okay
                                return Ok(());
                            }
                            // mark that we're taking these deltas
                            observed_len = model.delta_state().__fractal_take_full_from_data_delta(super::FractalToken::new());
                            Self::try_write_model_data_batch(model, observed_len, driver)
                        });
                        match res {
                            Ok(()) => {}
                            Err(_) => {
                                // this failure is *not* good, so we want to promote this to a critical task
                                self.hp_dispatcher.send(Task::new(CriticalTask::WriteBatch(model_id.clone(), observed_len))).unwrap()
                            }
                        }
                    }
                }
                task = lpq.recv() => {
                    let Some(Task { threshold, task }) = task else {
                        return;
                    };
                    // TODO(@ohsayan): threshold
                    match task {
                        GenericTask::DeleteFile(f) => {
                            if let Err(_) = fs::remove_file(&f).await {
                                self.general_dispatcher.send(
                                    Task::with_threshold(GenericTask::DeleteFile(f), threshold - 1)
                                ).unwrap();
                            }
                        }
                        GenericTask::DeleteDirAll(dir) => {
                            if let Err(_) = fs::remove_dir_all(&dir).await {
                                self.general_dispatcher.send(
                                    Task::with_threshold(GenericTask::DeleteDirAll(dir), threshold - 1)
                                ).unwrap();
                            }
                        }
                    }
                }
            }
        }
    }
}

// util
impl FractalMgr {
    /// Attempt to write a model data batch with the observed size.
    ///
    /// The zero check is essential
    fn try_write_model_data_batch(
        model: &Model,
        observed_size: usize,
        mdl_driver: &super::FractalModelDriver<LocalFS>,
    ) -> crate::engine::error::QueryResult<()> {
        if observed_size == 0 {
            // no changes, all good
            return Ok(());
        }
        // try flushing the batch
        let mut batch_driver = mdl_driver.batch_driver().lock();
        batch_driver.write_new_batch(model, observed_size)?;
        Ok(())
    }
}
