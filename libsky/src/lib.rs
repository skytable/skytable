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

use std::error::Error;
/// A generic result
pub type TResult<T> = Result<T, Box<dyn Error>>;
/// The size of the read buffer in bytes
pub const BUF_CAP: usize = 8 * 1024; // 8 KB per-connection
/// The current version
pub static VERSION: &str = env!("CARGO_PKG_VERSION");
/// The URL
pub static URL: &str = "https://github.com/skytable/skytable";

#[macro_export]
/// Don't use unwrap_or but use this macro as the optimizer fails to optimize away usages
/// of unwrap_or and creates a lot of LLVM IR bloat. use
// FIXME(@ohsayan): Fix this when https://github.com/rust-lang/rust/issues/68667 is addressed
macro_rules! option_unwrap_or {
    ($try:expr, $fallback:expr) => {
        match $try {
            Some(t) => t,
            None => $fallback,
        }
    };
}
