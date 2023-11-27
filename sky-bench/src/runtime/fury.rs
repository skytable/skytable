/*
 * Created on Wed Nov 22 2023
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
    crate::bench::{BenchmarkTask, BENCHMARK_SPACE_ID},
    skytable::Config,
    std::{
        fmt,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::{Duration, Instant},
    },
    tokio::sync::{broadcast, mpsc, RwLock},
};

/*
    state
*/

static GLOBAL_START: RwLock<()> = RwLock::const_new(());
static GLOBAL_TARGET: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_EXIT: AtomicBool = AtomicBool::new(false);

fn gset_target(target: usize) {
    GLOBAL_TARGET.store(target, Ordering::Release)
}
fn gset_exit() {
    GLOBAL_EXIT.store(true, Ordering::Release)
}
fn grefresh_target() -> usize {
    let mut current = GLOBAL_TARGET.load(Ordering::Acquire);
    loop {
        if current == 0 {
            return 0;
        }
        match GLOBAL_TARGET.compare_exchange(
            current,
            current - 1,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(prev) => return prev,
            Err(new) => current = new,
        }
    }
}
fn grefresh_early_exit() -> bool {
    GLOBAL_EXIT.load(Ordering::Acquire)
}

/*
    errors
*/

pub type FuryResult<T> = Result<T, FuryError>;

#[derive(Debug)]
pub enum FuryError {
    Init(skytable::error::Error),
    Worker(FuryWorkerError),
    Dead,
}

impl fmt::Display for FuryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init(e) => write!(f, "fury init failed. {e}"),
            Self::Worker(e) => write!(f, "worker failed. {e}"),
            Self::Dead => write!(f, "all workers offline"),
        }
    }
}

impl From<FuryWorkerError> for FuryError {
    fn from(e: FuryWorkerError) -> Self {
        Self::Worker(e)
    }
}

#[derive(Debug)]
pub enum FuryWorkerError {
    DbError(skytable::error::Error),
    Mismatch,
}

impl fmt::Display for FuryWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DbError(e) => write!(f, "client errored. {e}"),
            Self::Mismatch => write!(f, "server response did not match expected response"),
        }
    }
}

/*
    impl
*/

#[derive(Debug)]
pub struct Fury {
    tx_task: broadcast::Sender<WorkerTask<BenchmarkTask>>,
    rx_task_result: mpsc::Receiver<FuryResult<WorkerLocalStats>>,
    client_count: usize,
}

impl Fury {
    pub async fn new(client_count: usize, config: Config) -> FuryResult<Self> {
        let (tx_task, rx_task) = broadcast::channel(1);
        let (tx_task_result, rx_task_result) = mpsc::channel(client_count);
        let (tx_ack, mut rx_ack) = mpsc::channel(1);
        for id in 0..client_count {
            let rx_task = tx_task.subscribe();
            let tx_task_result = tx_task_result.clone();
            let tx_ack = tx_ack.clone();
            let config = config.clone();
            tokio::spawn(
                async move { worker_svc(id, rx_task, tx_task_result, tx_ack, config).await },
            );
        }
        drop((tx_ack, rx_task));
        match rx_ack.recv().await {
            None => {}
            Some(e) => return Err(FuryError::Init(e)),
        }
        info!("all workers online. ready for event loop");
        Ok(Self {
            tx_task,
            rx_task_result,
            client_count,
        })
    }
    pub async fn bombard(&mut self, count: usize, task: BenchmarkTask) -> FuryResult<RuntimeStats> {
        // pause workers and set target
        let start_guard = GLOBAL_START.write().await;
        gset_target(count);
        // send tasks
        if self.tx_task.send(WorkerTask::Task(task)).is_err() {
            return Err(FuryError::Dead);
        }
        // begin work
        drop(start_guard);
        // init stats
        let mut global_start = None;
        let mut global_stop = None;
        let mut global_head = u128::MAX;
        let mut global_tail = 0u128;
        let mut remaining = self.client_count;
        while remaining != 0 {
            let WorkerLocalStats {
                start: this_start,
                elapsed: this_elapsed,
                head: this_head,
                tail: this_tail,
            } = match self.rx_task_result.recv().await {
                None => {
                    return Err(FuryError::Dead);
                }
                Some(res) => res,
            }?;
            match global_start.as_mut() {
                None => global_start = Some(this_start),
                Some(current_start) => {
                    if this_start < *current_start {
                        *current_start = this_start;
                    }
                }
            }
            let this_stop = this_start + Duration::from_nanos(this_elapsed.try_into().unwrap());
            match global_stop.as_mut() {
                None => global_stop = Some(this_stop),
                Some(current_gstop) => {
                    if this_stop > *current_gstop {
                        *current_gstop = this_stop;
                    }
                }
            }
            if this_head < global_head {
                global_head = this_head;
            }
            if this_tail > global_tail {
                global_tail = this_tail;
            }
            remaining -= 1;
        }
        Ok(RuntimeStats {
            qps: super::qps(
                count,
                global_stop
                    .unwrap()
                    .duration_since(global_start.unwrap())
                    .as_nanos(),
            ),
            head: global_head,
            tail: global_tail,
        })
    }
}

async fn worker_svc(
    id: usize,
    mut rx_task: broadcast::Receiver<WorkerTask<BenchmarkTask>>,
    tx_task_result: mpsc::Sender<FuryResult<WorkerLocalStats>>,
    tx_ack: mpsc::Sender<skytable::error::Error>,
    connection_cfg: Config,
) {
    let mut db = match connection_cfg.connect_async().await {
        Ok(c) => c,
        Err(e) => {
            if tx_ack.send(e).await.is_err() {
                error!("worker-{id} failed to ack because main thread exited");
            }
            return;
        }
    };
    // set DB in connections
    match db
        .query_parse::<()>(&skytable::query!(format!("use {BENCHMARK_SPACE_ID}")))
        .await
    {
        Ok(()) => {}
        Err(e) => {
            if tx_ack.send(e).await.is_err() {
                error!("worker-{id} failed to report error because main thread exited");
            }
            return;
        }
    }
    // we're connected and ready to server
    drop(tx_ack);
    'wait: loop {
        let task = match rx_task.recv().await {
            Err(_) => {
                error!("worked-{id} is exiting because main thread exited");
                return;
            }
            Ok(WorkerTask::Exit) => return,
            Ok(WorkerTask::Task(t)) => t,
        };
        // received a task; ready to roll; wait for begin signal
        let permit = GLOBAL_START.read().await;
        // off to the races
        let mut current = grefresh_target();
        let mut exit_now = grefresh_early_exit();
        // init local stats
        let mut local_start = None;
        let mut local_elapsed = 0u128;
        let mut local_head = u128::MAX;
        let mut local_tail = 0u128;
        while (current != 0) && !exit_now {
            // prepare query
            let query = task.generate_query(current as _);
            // execute timed
            let start = Instant::now();
            let ret = db.query(&query).await;
            let stop = Instant::now();
            // check response
            let resp = match ret {
                Ok(resp) => resp,
                Err(e) => {
                    gset_exit();
                    if tx_task_result
                        .send(Err(FuryError::Worker(FuryWorkerError::DbError(e))))
                        .await
                        .is_err()
                    {
                        error!(
                            "worker-{id} failed to report worker error because main thread exited"
                        );
                        return;
                    }
                    continue 'wait;
                }
            };
            if !task.verify_response(current as _, resp.clone()) {
                gset_exit();
                if tx_task_result
                    .send(Err(FuryError::Worker(FuryWorkerError::Mismatch)))
                    .await
                    .is_err()
                {
                    error!(
                        "worker-{id} failed to report mismatch error because main thread exited"
                    );
                    return;
                }
                continue 'wait;
            }
            // update stats
            if local_start.is_none() {
                local_start = Some(start);
            }
            let elapsed = stop.duration_since(start).as_nanos();
            local_elapsed += elapsed;
            if elapsed > local_tail {
                local_tail = elapsed;
            }
            if elapsed < local_head {
                local_head = elapsed;
            }
            current = grefresh_target();
            exit_now = grefresh_early_exit();
        }
        if exit_now {
            continue 'wait;
        }
        // good! send these results
        if tx_task_result
            .send(Ok(WorkerLocalStats::new(
                local_start.unwrap(),
                local_elapsed,
                local_head,
                local_tail,
            )))
            .await
            .is_err()
        {
            error!("worker-{id} failed to send results because main thread exited");
            return;
        }
        drop(permit);
    }
}

impl Drop for Fury {
    fn drop(&mut self) {
        let _ = self.tx_task.send(WorkerTask::Exit);
    }
}
