/*
 * Created on Fri Jun 18 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/
use log::trace;
use skytable::Query;
use sysinfo::{System, SystemExt};

pub fn calculate_max_keylen(expected_queries: usize, sys: &mut System) -> usize {
    let total_mem_in_bytes = (sys.total_memory() * 1024) as usize;
    trace!(
        "This host has a total memory of: {} Bytes",
        total_mem_in_bytes
    );
    // av_mem gives us 90% of the memory size
    let ninety_percent_of_memory = (0.90_f32 * total_mem_in_bytes as f32) as usize;
    let mut highest_len = 1usize;
    loop {
        let set_pack_len = Query::array_packet_size_hint(vec![3, highest_len, highest_len]);
        let get_pack_len = Query::array_packet_size_hint(vec![3, highest_len]);
        let resulting_size = expected_queries
            * (
                // for the set packets
                set_pack_len +
                // for the get packets
                get_pack_len +
                // for the keys themselves
                highest_len
            );
        if resulting_size >= ninety_percent_of_memory as usize {
            break;
        }
        // increase the length by 5% every time to get the maximum possible length
        // now this 5% increment is a tradeoff, but it's worth it to not wait for
        // so long
        highest_len = (highest_len as f32 * 1.05_f32).ceil() as usize;
    }
    highest_len
}
