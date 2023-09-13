/*
 * Created on Wed Sep 13 2023
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
    super::{CriticalTask, GenericTask, GlobalInstanceLike, Task},
    crate::engine::core::GlobalNS,
    parking_lot::RwLock,
};

/// A `test` mode global implementation
pub struct TestGlobal {
    gns: GlobalNS,
    hp_queue: RwLock<Vec<Task<CriticalTask>>>,
    lp_queue: RwLock<Vec<Task<GenericTask>>>,
    max_delta_size: usize,
}

impl TestGlobal {
    pub fn empty() -> Self {
        Self::with_max_delta_size(0)
    }
    pub fn with_max_delta_size(max_delta_size: usize) -> Self {
        Self {
            gns: GlobalNS::empty(),
            hp_queue: RwLock::default(),
            lp_queue: RwLock::default(),
            max_delta_size,
        }
    }
}

impl GlobalInstanceLike for TestGlobal {
    fn namespace(&self) -> &GlobalNS {
        &self.gns
    }
    fn post_high_priority_task(&self, task: Task<CriticalTask>) {
        self.hp_queue.write().push(task)
    }
    fn post_standard_priority_task(&self, task: Task<GenericTask>) {
        self.lp_queue.write().push(task)
    }
    fn get_max_delta_size(&self) -> usize {
        100
    }
}
