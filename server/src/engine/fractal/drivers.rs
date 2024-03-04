/*
 * Created on Sun Sep 10 2023
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
    super::{util, GlobalInstanceLike},
    crate::{
        engine::{
            error::{QueryError, QueryResult, RuntimeResult},
            fractal::{CriticalTask, Task},
            storage::{GNSDriver, ModelDriver},
        },
        util::compiler,
    },
    parking_lot::Mutex,
};

/// GNS driver
#[derive(Debug)]
pub struct FractalGNSDriver {
    status: util::Status,
    pub(super) txn_driver: Mutex<GNSDriver>,
}

impl FractalGNSDriver {
    pub fn new(txn_driver: GNSDriver) -> Self {
        Self {
            status: util::Status::new_okay(),
            txn_driver: Mutex::new(txn_driver),
        }
    }
    pub(super) fn status(&self) -> &util::Status {
        &self.status
    }
    pub fn driver_context<T>(
        &self,
        g: &impl GlobalInstanceLike,
        f: impl Fn(&mut GNSDriver) -> RuntimeResult<T>,
        on_failure: impl Fn(),
    ) -> QueryResult<T> {
        if self.status.is_iffy() {
            return Err(QueryError::SysServerError);
        }
        let mut txn_driver = self.txn_driver.lock();
        match f(&mut txn_driver) {
            Ok(v) => Ok(v),
            Err(e) => compiler::cold_call(|| {
                self.status.set_iffy();
                g.health().report_fault();
                on_failure();
                g.taskmgr_post_high_priority(Task::new(CriticalTask::CheckGNSDriver));
                error!("GNS driver failed with: {e}");
                Err(QueryError::SysServerError)
            }),
        }
    }
}

/// Model driver
#[derive(Debug)]
#[must_use]
pub struct FractalModelDriver {
    status: util::Status,
    batch_driver: Mutex<Option<ModelDriver>>,
}

impl FractalModelDriver {
    pub const fn uninitialized() -> Self {
        Self {
            status: util::Status::new_okay(),
            batch_driver: Mutex::new(None),
        }
    }
    pub fn initialize_model_driver(&self, driver: ModelDriver) {
        let mut drv = self.batch_driver.lock();
        if drv.is_none() {
            *drv = Some(driver);
        } else {
            panic!("driver already initialized")
        }
    }
    pub(in crate::engine::fractal) fn init(batch_driver: ModelDriver) -> Self {
        Self {
            status: util::Status::new_okay(),
            batch_driver: Mutex::new(Some(batch_driver)),
        }
    }
    pub fn status(&self) -> &util::Status {
        &self.status
    }
    /// Returns a reference to the batch persist driver
    pub fn batch_driver(&self) -> &Mutex<Option<ModelDriver>> {
        &self.batch_driver
    }
    pub fn close(self) -> RuntimeResult<()> {
        ModelDriver::close_driver(&mut self.batch_driver.into_inner().unwrap())
    }
}
