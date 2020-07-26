/*
 * Created on Sun Jul 26 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

//! This module implements consistent hashing
//! with the [Maglev Hasing algorithm](https://research.google.com/pubs/archive/44824.pdf)

// TODO(@ohsayan): Use the Lucas method instead
fn easy_prime(num: u64) -> bool {
    for val in 2..num - 1 {
        if num % val == 1 {
            return false;
        };
    }
    true
}

const BIGM: u64 = 65537;

pub struct Maglev {
    n: u64,
    m: u64,
    perm: Vec<Vec<u64>>,
    lookup: Vec<usize>,
    nodes: Vec<String>,
}
