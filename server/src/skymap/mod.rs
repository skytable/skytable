/*
 * Created on Wed Jun 02 2021
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

//! A hashtable with SIMD lookup, quadratic probing and thread friendliness.
//! TODO(@ohsayan): Update this notice!
//! 
//! ## Acknowledgements
//! 
//! This implementation is inspired by:
//! - The Rust Standard Library's hashtable implementation since 1.36, released under the 
//! [Apache-2.0 License](https://github.com/rust-lang/hashbrown/blob/master/LICENSE-APACHE) OR
//! the [MIT License](https://github.com/rust-lang/hashbrown/blob/master/LICENSE-MIT) at your option
//! - Google for the [original Swisstable implementation](https://github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h)
//! that is distributed under the [Apache-2.0 License](https://github.com/abseil/abseil-cpp/blob/master/LICENSE)

mod raw;
