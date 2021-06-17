/*
 * Created on Wed Jun 16 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]

use core::marker::PhantomData;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver as CReceiver;
use crossbeam_channel::Sender as CSender;
use rayon::prelude::*;
use std::thread;

/// A Job. The UIn type parameter is the type that will be used to execute the action
/// Nothing is a variant used by the drop implementation to terminate all the workers
/// and call the exit_loop function
enum JobType<UIn> {
    Task(UIn),
    Nothing,
}

/// A worker
///
/// The only reason we use option is to reduce the effort needed to implement [`Drop`] for the
/// [`Workpool`]
struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    /// Initialize a new worker
    fn new<Inp: 'static, UIn>(
        job_receiver: CReceiver<JobType<UIn>>,
        init_pre_loop_var: impl Fn() -> Inp + 'static + Send,
        on_exit: impl Fn(&mut Inp) + Send + 'static,
        on_loop: impl Fn(&mut Inp, UIn) + Send + Sync + 'static,
    ) -> Self
    where
        UIn: Send + Sync + 'static,
    {
        let thread = thread::spawn(move || {
            let on_loop = on_loop;
            let mut pre_loop_var = init_pre_loop_var();
            loop {
                let action = job_receiver.recv().unwrap();
                match action {
                    JobType::Task(tsk) => on_loop(&mut pre_loop_var, tsk),
                    JobType::Nothing => {
                        on_exit(&mut pre_loop_var);
                        break;
                    }
                }
            }
        });
        Self {
            thread: Some(thread),
        }
    }
}

impl<Inp: 'static, UIn, Lp, Lv, Ex> Clone for Workpool<Inp, UIn, Lv, Lp, Ex>
where
    UIn: Send + Sync + 'static,
    Inp: Sync,
    Ex: Fn(&mut Inp) + Send + Sync + 'static + Clone,
    Lv: Fn() -> Inp + Send + Sync + 'static + Clone,
    Lp: Fn(&mut Inp, UIn) + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Workpool::new(
            self.workers.len(),
            self.init_pre_loop_var.clone(),
            self.on_loop.clone(),
            self.on_exit.clone(),
            self.needs_iterator_pool,
        )
    }
}

/// # Workpool
///
/// A Workpool is a generic synchronous thread pool that can be used to perform, well, anything.
/// A workpool has to be initialized with the number of workers, the pre_loop_variable (set this
/// to None if there isn't any). what to do on loop and what to do on exit of each worker. The
/// closures are kept as `Clone`able types just to reduce complexity with copy (we were lazy).
///
/// ## Clones
///
/// Workpool clones simply create a new workpool with the same on_exit, on_loop and init_pre_loop_var
/// configurations. This provides a very convenient interface if one desires to use multiple workpools
/// to do the _same kind of thing_
///
/// ## Actual thread count
///
/// The actual thread count will depend on whether the caller requests the initialization of an
/// iterator pool or not. If the caller does request for an iterator pool, then the number of threads
/// spawned will be twice the number of the set workers. Else, the number of spawned threads is equal
/// to the number of workers.
pub struct Workpool<Inp, UIn, Lv, Lp, Ex> {
    /// the workers
    workers: Vec<Worker>,
    /// the sender that sends jobs
    job_distributor: CSender<JobType<UIn>>,
    /// the function that sets the pre-loop variable
    init_pre_loop_var: Lv,
    /// the function to be executed on worker termination
    on_exit: Ex,
    /// the function to be executed on loop
    on_loop: Lp,
    /// a marker for `Inp` since no parameters use it directly
    _marker: PhantomData<Inp>,
    /// check if self needs a pool for parallel iterators
    needs_iterator_pool: bool,
}

impl<Inp: 'static, UIn, Lv, Ex, Lp> Workpool<Inp, UIn, Lv, Lp, Ex>
where
    UIn: Send + Sync + 'static,
    Ex: Fn(&mut Inp) + Send + Sync + 'static + Clone,
    Lv: Fn() -> Inp + Send + Sync + 'static + Clone,
    Lp: Fn(&mut Inp, UIn) + Send + Sync + 'static + Clone,
    Inp: Sync,
{
    /// Create a new workpool
    pub fn new(
        count: usize,
        init_pre_loop_var: Lv,
        on_loop: Lp,
        on_exit: Ex,
        needs_iterator_pool: bool,
    ) -> Self {
        if needs_iterator_pool {
            // initialize a global threadpool for parallel iterators
            let _ = rayon::ThreadPoolBuilder::new()
                .num_threads(count)
                .build_global();
        }
        if count == 0 {
            panic!("Runtime panic: Bad value `0` for thread count");
        }
        let (sender, receiver) = unbounded();
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            workers.push(Worker::new(
                receiver.clone(),
                init_pre_loop_var.clone(),
                on_exit.clone(),
                on_loop.clone(),
            ));
        }
        Self {
            workers,
            job_distributor: sender,
            init_pre_loop_var,
            on_exit,
            on_loop,
            _marker: PhantomData,
            needs_iterator_pool,
        }
    }
    /// Execute something
    pub fn execute(&self, inp: UIn) {
        self.job_distributor.send(JobType::Task(inp)).unwrap();
    }
    pub fn execute_iter(&self, iter: impl IntoParallelIterator<Item = UIn>) {
        iter.into_par_iter().for_each(|inp| self.execute(inp));
    }
    pub fn new_default_threads(
        init_pre_loop_var: Lv,
        on_loop: Lp,
        on_exit: Ex,
        needs_iterator_pool: bool,
    ) -> Self {
        // we'll naively use the number of CPUs present on the system times 2 to determine
        // the number of workers (sure the scheduler does tricks all the time)
        let worker_count = num_cpus::get() * 2;
        Self::new(
            worker_count,
            init_pre_loop_var,
            on_loop,
            on_exit,
            needs_iterator_pool,
        )
    }
}

impl<Inp, UIn, Lv, Lp, Ex> Drop for Workpool<Inp, UIn, Lp, Lv, Ex> {
    fn drop(&mut self) {
        for _ in &mut self.workers {
            self.job_distributor.send(JobType::Nothing).unwrap();
        }
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap()
            }
        }
    }
}
