/*
 * Created on Tue Nov 21 2023
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
    super::{RuntimeStats, WorkerLocalStats, WorkerTask},
    crossbeam_channel::{unbounded, Receiver, Sender},
    std::{
        fmt::{self, Display},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            RwLock, RwLockReadGuard, RwLockWriteGuard,
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
};

pub type BombardResult<T, Bt> = Result<T, BombardError<Bt>>;

/*
    state mgmt
*/

#[derive(Debug)]
/// The pool state. Be warned **ONLY ONE POOL AT A TIME!**
struct GPState {
    current: AtomicU64,
    state: AtomicBool,
    occupied: AtomicBool,
    start_sig: RwLock<()>,
}

impl GPState {
    #[inline(always)]
    fn get() -> &'static Self {
        static STATE: GPState = GPState::zero();
        &STATE
    }
    const fn zero() -> Self {
        Self {
            current: AtomicU64::new(0),
            state: AtomicBool::new(true),
            occupied: AtomicBool::new(false),
            start_sig: RwLock::new(()),
        }
    }
    fn wait_for_global_begin(&self) -> RwLockReadGuard<'_, ()> {
        self.start_sig.read().unwrap()
    }
    fn occupy(&self) {
        assert!(!self.occupied.swap(true, Ordering::Release));
    }
    fn vacate(&self) {
        assert!(self.occupied.swap(false, Ordering::Release));
    }
    fn guard<T>(f: impl FnOnce(RwLockWriteGuard<'static, ()>) -> T) -> T {
        let slf = Self::get();
        slf.occupy();
        let ret = f(slf.start_sig.write().unwrap());
        slf.vacate();
        ret
    }
    fn post_failure(&self) {
        self.state.store(false, Ordering::Release)
    }
    fn post_target(&self, target: u64) {
        self.current.store(target, Ordering::Release)
    }
    /// WARNING: this is not atomic! only sensible to run a quiescent state
    fn post_reset(&self) {
        self.current.store(0, Ordering::Release);
        self.state.store(true, Ordering::Release);
    }
    fn update_target(&self) -> u64 {
        let mut current = self.current.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return 0;
            }
            match self.current.compare_exchange(
                current,
                current - 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(last) => {
                    return last;
                }
                Err(new) => {
                    current = new;
                }
            }
        }
    }
    fn load_okay(&self) -> bool {
        self.state.load(Ordering::Acquire)
    }
}

/*
    task spec
*/

/// A threaded bombard task specification which drives a global pool of threads towards a common goal
pub trait ThreadedBombardTask: Send + Sync + 'static {
    /// The per-task worker that is initialized once in every thread (not to be confused with the actual thread worker!)
    type Worker: Send + Sync;
    /// The task that the [`ThreadedBombardTask::TaskWorker`] performs
    type WorkerTask: Send + Sync;
    type WorkerTaskSpec: Clone + Send + Sync + 'static;
    /// Errors while running a task
    type WorkerTaskError: Send + Sync;
    /// Errors while initializing a task worker
    type WorkerInitError: Send + Sync;
    /// Initialize a task worker
    fn worker_init(&self) -> Result<Self::Worker, Self::WorkerInitError>;
    fn generate_task(spec: &Self::WorkerTaskSpec, current: u64) -> Self::WorkerTask;
    /// Drive a single subtask
    fn worker_drive_timed(
        worker: &mut Self::Worker,
        task: Self::WorkerTask,
    ) -> Result<u128, Self::WorkerTaskError>;
}

/*
    worker
*/

#[derive(Debug)]
enum WorkerResult<Bt: ThreadedBombardTask> {
    Completed(WorkerLocalStats),
    Errored(Bt::WorkerTaskError),
}

#[derive(Debug)]
struct Worker {
    handle: JoinHandle<()>,
}

impl Worker {
    fn start<Bt: ThreadedBombardTask>(
        id: usize,
        driver: Bt::Worker,
        rx_work: Receiver<WorkerTask<Bt::WorkerTaskSpec>>,
        tx_res: Sender<WorkerResult<Bt>>,
    ) -> Self {
        Self {
            handle: thread::Builder::new()
                .name(format!("worker-{id}"))
                .spawn(move || {
                    let mut worker_driver = driver;
                    'blocking_wait: loop {
                        let task = match rx_work.recv().unwrap() {
                            WorkerTask::Exit => return,
                            WorkerTask::Task(spec) => spec,
                        };
                        let guard = GPState::get().wait_for_global_begin();
                        // check global state
                        let mut global_okay = GPState::get().load_okay();
                        let mut global_position = GPState::get().update_target();
                        // init local state
                        let mut local_start = None;
                        let mut local_elapsed = 0u128;
                        let mut local_head = u128::MAX;
                        let mut local_tail = 0;
                        // bombard
                        while (global_position != 0) & global_okay {
                            let task = Bt::generate_task(&task, global_position);
                            if local_start.is_none() {
                                local_start = Some(Instant::now());
                            }
                            let this_elapsed =
                                match Bt::worker_drive_timed(&mut worker_driver, task) {
                                    Ok(elapsed) => elapsed,
                                    Err(e) => {
                                        GPState::get().post_failure();
                                        tx_res.send(WorkerResult::Errored(e)).unwrap();
                                        continue 'blocking_wait;
                                    }
                                };
                            local_elapsed += this_elapsed;
                            if this_elapsed < local_head {
                                local_head = this_elapsed;
                            }
                            if this_elapsed > local_tail {
                                local_tail = this_elapsed;
                            }
                            global_position = GPState::get().update_target();
                            global_okay = GPState::get().load_okay();
                        }
                        if global_okay {
                            // we're done
                            tx_res
                                .send(WorkerResult::Completed(WorkerLocalStats::new(
                                    local_start.unwrap(),
                                    local_elapsed,
                                    local_head,
                                    local_tail,
                                )))
                                .unwrap();
                        }
                        drop(guard);
                    }
                })
                .expect("failed to start thread"),
        }
    }
}

/*
    pool
*/

#[derive(Debug)]
pub enum BombardError<Bt: ThreadedBombardTask> {
    InitError(Bt::WorkerInitError),
    WorkerTaskError(Bt::WorkerTaskError),
    AllWorkersOffline,
}

impl<Bt: ThreadedBombardTask> fmt::Display for BombardError<Bt>
where
    Bt::WorkerInitError: fmt::Display,
    Bt::WorkerTaskError: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllWorkersOffline => write!(
                f,
                "bombard failed because all workers went offline indicating catastrophic failure"
            ),
            Self::WorkerTaskError(e) => write!(f, "worker task failed. {e}"),
            Self::InitError(e) => write!(f, "worker init failed. {e}"),
        }
    }
}

#[derive(Debug)]
pub struct BombardPool<Bt: ThreadedBombardTask> {
    workers: Vec<(Worker, Sender<WorkerTask<Bt::WorkerTaskSpec>>)>,
    rx_res: Receiver<WorkerResult<Bt>>,
    _config: Bt,
}

impl<Bt: ThreadedBombardTask> BombardPool<Bt> {
    pub fn new(size: usize, config: Bt) -> BombardResult<Self, Bt> {
        assert_ne!(size, 0, "pool can't be empty");
        let mut workers = Vec::with_capacity(size);
        let (tx_res, rx_res) = unbounded();
        for id in 0..size {
            let (tx_work, rx_work) = unbounded();
            let driver = config.worker_init().map_err(BombardError::InitError)?;
            workers.push((Worker::start(id, driver, rx_work, tx_res.clone()), tx_work));
        }
        Ok(Self {
            workers,
            rx_res,
            _config: config,
        })
    }
    /// Bombard queries to the workers
    pub fn blocking_bombard(
        &mut self,
        task_description: Bt::WorkerTaskSpec,
        count: usize,
    ) -> BombardResult<RuntimeStats, Bt> {
        GPState::guard(|paused| {
            GPState::get().post_target(count as _);
            let mut global_start = None;
            let mut global_stop = None;
            let mut global_head = u128::MAX;
            let mut global_tail = 0u128;
            for (_, sender) in self.workers.iter() {
                sender
                    .send(WorkerTask::Task(task_description.clone()))
                    .unwrap();
            }
            // now let them begin!
            drop(paused);
            // wait for all workers to complete
            let mut received = 0;
            while received != self.workers.len() {
                let results = match self.rx_res.recv() {
                    Err(_) => return Err(BombardError::AllWorkersOffline),
                    Ok(r) => r,
                };
                let WorkerLocalStats {
                    start: this_start,
                    elapsed,
                    head,
                    tail,
                } = match results {
                    WorkerResult::Completed(r) => r,
                    WorkerResult::Errored(e) => return Err(BombardError::WorkerTaskError(e)),
                };
                // update start if required
                match global_start.as_mut() {
                    None => {
                        global_start = Some(this_start);
                    }
                    Some(start) => {
                        if this_start < *start {
                            *start = this_start;
                        }
                    }
                }
                let this_task_stopped_at =
                    this_start + Duration::from_nanos(elapsed.try_into().unwrap());
                match global_stop.as_mut() {
                    None => {
                        global_stop = Some(this_task_stopped_at);
                    }
                    Some(stop) => {
                        if this_task_stopped_at > *stop {
                            // this task stopped later than the previous one
                            *stop = this_task_stopped_at;
                        }
                    }
                }
                if head < global_head {
                    global_head = head;
                }
                if tail > global_tail {
                    global_tail = tail;
                }
                received += 1;
            }
            // reset global pool state
            GPState::get().post_reset();
            // compute results
            let global_elapsed = global_stop
                .unwrap()
                .duration_since(global_start.unwrap())
                .as_nanos();
            Ok(RuntimeStats {
                qps: super::qps(count, global_elapsed),
                head: global_head,
                tail: global_tail,
            })
        })
    }
}

impl<Bt: ThreadedBombardTask> Drop for BombardPool<Bt> {
    fn drop(&mut self) {
        info!("taking all workers offline");
        for (_, sender) in self.workers.iter() {
            sender.send(WorkerTask::Exit).unwrap();
        }
        for (worker, _) in self.workers.drain(..) {
            worker.handle.join().unwrap();
        }
        info!("all workers now offline");
    }
}
