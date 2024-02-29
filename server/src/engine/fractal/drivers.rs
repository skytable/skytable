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
    super::{util, ModelUniqueID},
    crate::{
        engine::{
            error::{QueryError, QueryResult, RuntimeResult},
            storage::{GNSDriver, ModelDriver},
        },
        util::compiler,
    },
    parking_lot::{Mutex, RwLock},
    std::{collections::HashMap, sync::Arc},
};

/// GNS driver
pub struct FractalGNSDriver {
    status: util::Status,
    pub(super) txn_driver: GNSDriver,
}

impl FractalGNSDriver {
    pub(super) fn new(txn_driver: GNSDriver) -> Self {
        Self {
            status: util::Status::new_okay(),
            txn_driver: txn_driver,
        }
    }
    pub fn driver_context<T>(
        &mut self,
        f: impl Fn(&mut GNSDriver) -> RuntimeResult<T>,
        on_failure: impl Fn(),
    ) -> QueryResult<T> {
        if self.status.is_iffy() {
            return Err(QueryError::SysServerError);
        }
        match f(&mut self.txn_driver) {
            Ok(v) => Ok(v),
            Err(e) => compiler::cold_call(|| {
                error!("GNS driver failed with: {e}");
                self.status.set_iffy();
                on_failure();
                Err(QueryError::SysServerError)
            }),
        }
    }
}

pub struct ModelDrivers {
    drivers: RwLock<HashMap<ModelUniqueID, FractalModelDriver>>,
}

impl ModelDrivers {
    pub fn empty() -> Self {
        Self {
            drivers: RwLock::new(HashMap::new()),
        }
    }
    pub fn drivers(&self) -> &RwLock<HashMap<ModelUniqueID, FractalModelDriver>> {
        &self.drivers
    }
    pub fn count(&self) -> usize {
        self.drivers.read().len()
    }
    pub fn add_driver(&self, id: ModelUniqueID, batch_driver: ModelDriver) {
        assert!(self
            .drivers
            .write()
            .insert(id, FractalModelDriver::init(batch_driver))
            .is_none());
    }
    pub fn remove_driver(&self, id: ModelUniqueID) {
        assert!(self.drivers.write().remove(&id).is_some())
    }
    pub fn into_inner(self) -> HashMap<ModelUniqueID, FractalModelDriver> {
        self.drivers.into_inner()
    }
}

/// Model driver
pub struct FractalModelDriver {
    status: Arc<util::Status>,
    batch_driver: Mutex<ModelDriver>,
}

impl FractalModelDriver {
    pub(in crate::engine::fractal) fn init(batch_driver: ModelDriver) -> Self {
        Self {
            status: Arc::new(util::Status::new_okay()),
            batch_driver: Mutex::new(batch_driver),
        }
    }
    pub fn status(&self) -> &util::Status {
        &self.status
    }
    /// Returns a reference to the batch persist driver
    pub fn batch_driver(&self) -> &Mutex<ModelDriver> {
        &self.batch_driver
    }
    pub fn close(self) -> RuntimeResult<()> {
        ModelDriver::close_driver(&mut self.batch_driver.into_inner())
    }
}
