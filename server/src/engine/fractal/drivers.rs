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
        storage::v1::{data_batch::DataBatchPersistDriver, LocalFS},
        txn::gns::GNSTransactionDriverAnyFS,
    },
    parking_lot::Mutex,
    std::sync::Arc,
};

/// GNS driver
pub(super) struct FractalGNSDriver {
    status: util::Status,
    txn_driver: Mutex<GNSTransactionDriverAnyFS<LocalFS>>,
}

impl FractalGNSDriver {
    pub(super) fn new(txn_driver: GNSTransactionDriverAnyFS<LocalFS>) -> Self {
        Self {
            status: util::Status::new_okay(),
            txn_driver: Mutex::new(txn_driver),
        }
    }
}

/// Model driver
pub struct FractalModelDriver {
    hooks: Arc<FractalModelHooks>,
    batch_driver: Mutex<DataBatchPersistDriver<LocalFS>>,
}

impl FractalModelDriver {
    /// Initialize a model driver with default settings
    pub fn init(batch_driver: DataBatchPersistDriver<LocalFS>) -> Self {
        Self {
            hooks: Arc::new(FractalModelHooks::new()),
            batch_driver: Mutex::new(batch_driver),
        }
    }
    /// Returns a reference to the batch persist driver
    pub fn batch_driver(&self) -> &Mutex<DataBatchPersistDriver<LocalFS>> {
        &self.batch_driver
    }
}

/// Model hooks
#[derive(Debug)]
pub struct FractalModelHooks {
    status: util::Status,
}

impl FractalModelHooks {
    #[cfg(test)]
    pub fn test() -> Self {
        Self::new()
    }
    fn new() -> Self {
        Self {
            status: util::Status::new_okay(),
        }
    }
}
