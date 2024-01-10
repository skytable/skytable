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
    super::util,
    crate::engine::{
        error::RuntimeResult,
        storage::{safe_interfaces::FSInterface, v1::data_batch::DataBatchPersistDriver},
        txn::gns::GNSTransactionDriverAnyFS,
    },
    parking_lot::Mutex,
    std::sync::Arc,
};

/// GNS driver
pub(super) struct FractalGNSDriver<Fs: FSInterface> {
    #[allow(unused)]
    status: util::Status,
    pub(super) txn_driver: Mutex<GNSTransactionDriverAnyFS<Fs>>,
}

impl<Fs: FSInterface> FractalGNSDriver<Fs> {
    pub(super) fn new(txn_driver: GNSTransactionDriverAnyFS<Fs>) -> Self {
        Self {
            status: util::Status::new_okay(),
            txn_driver: Mutex::new(txn_driver),
        }
    }
    pub fn txn_driver(&self) -> &Mutex<GNSTransactionDriverAnyFS<Fs>> {
        &self.txn_driver
    }
}

/// Model driver
pub struct FractalModelDriver<Fs: FSInterface> {
    #[allow(unused)]
    hooks: Arc<FractalModelHooks>,
    batch_driver: Mutex<DataBatchPersistDriver<Fs>>,
}

impl<Fs: FSInterface> FractalModelDriver<Fs> {
    /// Initialize a model driver with default settings
    pub fn init(batch_driver: DataBatchPersistDriver<Fs>) -> Self {
        Self {
            hooks: Arc::new(FractalModelHooks::new()),
            batch_driver: Mutex::new(batch_driver),
        }
    }
    /// Returns a reference to the batch persist driver
    pub fn batch_driver(&self) -> &Mutex<DataBatchPersistDriver<Fs>> {
        &self.batch_driver
    }
    pub fn close(self) -> RuntimeResult<()> {
        self.batch_driver.into_inner().close()
    }
}

/// Model hooks
#[derive(Debug)]
pub struct FractalModelHooks;

impl FractalModelHooks {
    fn new() -> Self {
        Self
    }
}
