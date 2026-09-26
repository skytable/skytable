/*
 * Created on Mon Jul 20 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]

//! The core library for Skytable
//!
//! This contains modules which are shared by both the `cli` and the `server` modules

pub mod build_scripts;
pub mod cli_utils;
pub mod utils;
pub mod variables;

/// Returns a formatted version message `{binary} vx.y.z`
pub fn version_msg(binary: &str) -> String {
    format!("{binary} v{}", variables::VERSION)
}

#[macro_export]
macro_rules! take_many_options {
    ($from:expr => $($name:expr),* $(,)?) => {
        ($($crate::cli_utils::CliCommandData::take_option(&mut $from, $name)),*)
    }
}
