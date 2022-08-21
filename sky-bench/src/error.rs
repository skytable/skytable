/*
 * Created on Mon Aug 08 2022
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

use {
    libstress::WorkpoolError,
    skytable::error::Error as SkyError,
    std::{collections::TryReserveError, fmt::Display},
};

pub type BResult<T> = Result<T, Error>;

/// Benchmark tool errors
pub enum Error {
    /// An error originating from the Skytable client
    Client(SkyError),
    /// An error originating from the benchmark/server configuration
    Config(String),
    /// A runtime error
    Runtime(String),
}

impl From<SkyError> for Error {
    fn from(e: SkyError) -> Self {
        Self::Client(e)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Client(e) => write!(f, "client error: {}", e),
            Error::Config(e) => write!(f, "config error: {}", e),
            Error::Runtime(e) => write!(f, "runtime error: {}", e),
        }
    }
}

impl From<TryReserveError> for Error {
    fn from(e: TryReserveError) -> Self {
        Error::Runtime(format!("memory reserve error: {}", e))
    }
}

impl From<WorkpoolError> for Error {
    fn from(e: WorkpoolError) -> Self {
        Error::Runtime(format!("threadpool error: {}", e))
    }
}
