/*
 * Created on Fri Nov 17 2023
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
    crossbeam_channel::{unbounded, Receiver, Sender},
    std::{
        fmt,
        marker::PhantomData,
        thread::{self, JoinHandle},
        time::Instant,
    },
};

pub type TaskPoolResult<T, Th> = Result<T, TaskpoolError<Th>>;

#[derive(Debug)]
pub enum TaskpoolError<Th: ThreadedTask> {
    InitError(Th::TaskWorkerInitError),
    BombardError(&'static str),
    WorkerError(Th::TaskWorkerWorkError),
}

impl<Th: ThreadedTask> fmt::Display for TaskpoolError<Th>
where
    Th::TaskWorkerInitError: fmt::Display,
    Th::TaskWorkerWorkError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitError(e) => write!(f, "failed to init worker pool. {e}"),
            Self::BombardError(e) => write!(f, "failed to post work to pool. {e}"),
            Self::WorkerError(e) => write!(f, "failed running worker task. {e}"),
        }
    }
}

pub trait ThreadedTask: Send + Sync + 'static {
    /// the per-thread item that does the actual work
    ///
    /// NB: this is not to be confused with the actual underlying thread pool worker
    type TaskWorker: Send + Sync;
    /// when attempting initialization of the per-thread task worker, if an error is thrown, this is the type
    /// you're looking for
    type TaskWorkerInitError: Send + Sync;
    /// when attempting to run a single unit of work, if any error occurs this is the error type that is to be returned
    type TaskWorkerWorkError: Send + Sync;
    /// when attempting to close a worker, if an error occurs this is the error type that is returned
    type TaskWorkerTerminateError: Send + Sync;
    /// the task that is sent to each worker
    type TaskInput: Send + Sync;
    // fn
    /// initialize the worker
    fn initialize_worker(&self) -> Result<Self::TaskWorker, Self::TaskWorkerInitError>;
    /// drive the worker to complete a task and return the time
    fn drive_worker_timed(
        worker: &mut Self::TaskWorker,
        task: Self::TaskInput,
    ) -> Result<(Instant, Instant), Self::TaskWorkerWorkError>;
    fn terminate_worker(
        &self,
        worker: &mut Self::TaskWorker,
    ) -> Result<(), Self::TaskWorkerTerminateError>;
}

#[derive(Debug)]
struct ThreadWorker<Th> {
    handle: JoinHandle<()>,
    _m: PhantomData<Th>,
}

#[derive(Debug)]
enum WorkerTask<Th: ThreadedTask> {
    Task(Th::TaskInput),
    Exit,
}

impl<Th: ThreadedTask> ThreadWorker<Th> {
    fn new(
        hl_worker: Th::TaskWorker,
        task_rx: Receiver<WorkerTask<Th>>,
        res_tx: Sender<Result<(Instant, Instant), Th::TaskWorkerWorkError>>,
    ) -> Self {
        Self {
            handle: thread::spawn(move || {
                let mut worker = hl_worker;
                loop {
                    let task = match task_rx.recv().unwrap() {
                        WorkerTask::Exit => {
                            drop(task_rx);
                            return;
                        }
                        WorkerTask::Task(t) => t,
                    };
                    res_tx
                        .send(Th::drive_worker_timed(&mut worker, task))
                        .unwrap();
                }
            }),
            _m: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct Taskpool<Th: ThreadedTask> {
    workers: Vec<ThreadWorker<Th>>,
    _config: Th,
    task_tx: Sender<WorkerTask<Th>>,
    res_rx: Receiver<Result<(Instant, Instant), Th::TaskWorkerWorkError>>,
    record_real_start: Instant,
    record_real_stop: Instant,
    stat_run_avg_ns: f64,
    stat_run_tail_ns: u128,
    stat_run_head_ns: u128,
}

// TODO(@ohsayan): prepare histogram for report; for now there's no use of the head and tail latencies
#[derive(Default, Debug)]
pub struct RuntimeStats {
    pub qps: f64,
    pub avg_per_query_ns: f64,
    pub head_ns: u128,
    pub tail_ns: u128,
}

impl<Th: ThreadedTask> Taskpool<Th> {
    pub fn stat_avg(&self) -> f64 {
        self.stat_run_avg_ns
    }
    pub fn stat_tail(&self) -> u128 {
        self.stat_run_tail_ns
    }
    pub fn stat_head(&self) -> u128 {
        self.stat_run_head_ns
    }
    pub fn stat_elapsed(&self) -> u128 {
        self.record_real_stop
            .duration_since(self.record_real_start)
            .as_nanos()
    }
}

fn qps(query_count: usize, time_taken_in_nanos: u128) -> f64 {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let time_taken_in_nanos_f64 = time_taken_in_nanos as f64;
    let query_count_f64 = query_count as f64;
    (query_count_f64 / time_taken_in_nanos_f64) * NANOS_PER_SECOND as f64
}

impl<Th: ThreadedTask> Taskpool<Th> {
    pub fn new(size: usize, config: Th) -> TaskPoolResult<Self, Th> {
        let (task_tx, task_rx) = unbounded();
        let (res_tx, res_rx) = unbounded();
        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            let con = config
                .initialize_worker()
                .map_err(TaskpoolError::InitError)?;
            workers.push(ThreadWorker::new(con, task_rx.clone(), res_tx.clone()));
        }
        Ok(Self {
            workers,
            _config: config,
            task_tx,
            res_rx,
            stat_run_avg_ns: 0.0,
            record_real_start: Instant::now(),
            record_real_stop: Instant::now(),
            stat_run_head_ns: u128::MAX,
            stat_run_tail_ns: u128::MIN,
        })
    }
    pub fn blocking_bombard(
        &mut self,
        vec: Vec<Th::TaskInput>,
    ) -> TaskPoolResult<RuntimeStats, Th> {
        let expected = vec.len();
        let mut received = 0usize;
        for task in vec {
            match self.task_tx.send(WorkerTask::Task(task)) {
                Ok(()) => {}
                Err(_) => {
                    // stop bombarding, we hit an error
                    return Err(TaskpoolError::BombardError(
                        "all worker threads exited. this indicates a catastrophic failure",
                    ));
                }
            }
        }
        while received != expected {
            match self.res_rx.recv() {
                Err(_) => {
                    // all workers exited. that is catastrophic
                    return Err(TaskpoolError::BombardError(
                        "detected all worker threads crashed during run check",
                    ));
                }
                Ok(r) => self.recompute_stats(&mut received, r)?,
            };
        }
        // compute stats
        let ret = Ok(RuntimeStats {
            qps: qps(received, self.stat_elapsed()),
            avg_per_query_ns: self.stat_avg(),
            head_ns: self.stat_head(),
            tail_ns: self.stat_tail(),
        });
        // reset stats
        self.stat_run_avg_ns = 0.0;
        self.record_real_start = Instant::now();
        self.record_real_stop = Instant::now();
        self.stat_run_head_ns = u128::MAX;
        self.stat_run_tail_ns = u128::MIN;
        // return
        ret
    }
    fn recompute_stats(
        &mut self,
        received: &mut usize,
        result: Result<(Instant, Instant), <Th as ThreadedTask>::TaskWorkerWorkError>,
    ) -> Result<(), TaskpoolError<Th>> {
        *received += 1;
        let (start, stop) = match result {
            Ok(time) => time,
            Err(e) => return Err(TaskpoolError::WorkerError(e)),
        };
        // adjust real start
        if start < self.record_real_start {
            self.record_real_start = start;
        }
        if stop > self.record_real_stop {
            self.record_real_stop = stop;
        }
        let current_time = stop.duration_since(start).as_nanos();
        self.stat_run_avg_ns = self.stat_run_avg_ns
            + ((current_time as f64 - self.stat_run_avg_ns) / *received as f64);
        if current_time > self.stat_run_tail_ns {
            self.stat_run_tail_ns = current_time;
        }
        if current_time < self.stat_run_head_ns {
            self.stat_run_head_ns = current_time;
        }
        Ok(())
    }
}

impl<Th: ThreadedTask> Drop for Taskpool<Th> {
    fn drop(&mut self) {
        for _ in 0..self.workers.len() {
            self.task_tx.send(WorkerTask::Exit).unwrap();
        }
        for worker in self.workers.drain(..) {
            worker.handle.join().unwrap()
        }
    }
}
