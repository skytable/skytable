/*
 * Created on Mon Aug 28 2023
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

use crate::{
    engine::core::{model::delta::DataDelta, GlobalNS},
    util::os,
};

type Buf = Vec<u8>;

/*
    memory adjustments
*/

/// free memory in bytes
static mut FREEMEM_BYTES: u64 = 0;
/// capacity in bytes, per linked list
static mut CAP_PER_LL_BYTES: u64 = 0;
/// maximum number of nodes in linked list
static mut MAX_NODES_IN_LL_CNT: usize = 0;

/// Set the free memory and cap for deltas so that we don't bust through memory
///
/// ## Safety
/// - All models must have been loaded
/// - This must be called **before** the arbiter spawns threads for connections
pub unsafe fn set_limits(gns: &GlobalNS) {
    let model_cnt: usize = gns
        .spaces()
        .read()
        .values()
        .map(|space| space.models().read().len())
        .sum();
    let available_mem = os::free_memory_in_bytes();
    FREEMEM_BYTES = available_mem;
    CAP_PER_LL_BYTES =
        ((available_mem / core::cmp::max(1, model_cnt) as u64) as f64 * 0.002) as u64;
    MAX_NODES_IN_LL_CNT = CAP_PER_LL_BYTES as usize / (sizeof!(DataDelta) + sizeof!(u64));
}

/// Returns the maximum number of nodes that can be stored inside a delta queue for a model
///
/// Currently hardcoded to 0.2% of free memory after all datasets have been loaded
pub unsafe fn get_max_delta_queue_size() -> usize {
    // TODO(@ohsayan): dynamically approximate this limit
    MAX_NODES_IN_LL_CNT
}
