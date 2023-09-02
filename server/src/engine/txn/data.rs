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
    engine::{
        core::{index::PrimaryIndexKey, GlobalNS},
        data::cell::Datacell,
        storage::v1::inf::obj,
    },
    util::{os, EndianQW},
};

type Buf = Vec<u8>;

static mut CAP_PER_LL: usize = 0;
static mut FREEMEM: u64 = 0;

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
    FREEMEM = available_mem;
    CAP_PER_LL = ((available_mem as usize / core::cmp::max(1, model_cnt)) as f64 * 0.01) as usize;
}

/*
    misc. methods
*/

fn encode_primary_key(buf: &mut Buf, pk: &PrimaryIndexKey) {
    buf.push(pk.tag().d());
    static EXEC: [unsafe fn(&mut Buf, &PrimaryIndexKey); 2] = [
        |buf, pk| unsafe { buf.extend(pk.read_uint().to_le_bytes()) },
        |buf, pk| unsafe {
            let bin = pk.read_bin();
            buf.extend(bin.len().u64_bytes_le());
            buf.extend(bin);
        },
    ];
    unsafe {
        // UNSAFE(@ohsayan): tag map
        assert!((pk.tag().d() / 2) < 2);
        EXEC[(pk.tag().d() / 2) as usize](buf, pk);
    }
}

fn encode_dc(buf: &mut Buf, dc: &Datacell) {
    obj::encode_element(buf, dc)
}
