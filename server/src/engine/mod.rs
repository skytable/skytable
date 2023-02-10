/*
 * Created on Mon Sep 12 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

#![allow(dead_code)]

#[macro_use]
mod macros;
mod core;
mod data;
mod error;
mod idx;
mod mem;
mod ql;
mod sync;

/*

    A word on tests:

    "Nature is not equal. That's the whole problem." - Freeman Dyson

    Well, that applies to us for atleast all test cases since most of them are based on a quiescent
    state than a chaotic state as in runtime. We do emulate such cases, but remember most assertions
    that you'll make on most structures are just illusionary, and are only atomically correct at point
    in time.
*/
