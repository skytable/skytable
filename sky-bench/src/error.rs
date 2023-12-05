/*
 * Created on Sat Nov 18 2023
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
    crate::{
        bench::BombardTask,
        runtime::{fury, rookie::BombardError},
    },
    core::fmt,
    skytable::error::Error,
};

pub type BenchResult<T> = Result<T, BenchError>;

#[derive(Debug)]
pub enum BenchError {
    ArgsErr(String),
    RookieEngineError(BombardError<BombardTask>),
    FuryEngineError(fury::FuryError),
    DirectDbError(Error),
}

impl From<fury::FuryError> for BenchError {
    fn from(e: fury::FuryError) -> Self {
        Self::FuryEngineError(e)
    }
}

impl From<libsky::ArgParseError> for BenchError {
    fn from(e: libsky::ArgParseError) -> Self {
        match e {
            libsky::ArgParseError::Duplicate(d) => {
                Self::ArgsErr(format!("duplicate value for `{d}`"))
            }
            libsky::ArgParseError::MissingValue(m) => {
                Self::ArgsErr(format!("missing value for `{m}`"))
            }
        }
    }
}

impl From<Error> for BenchError {
    fn from(e: Error) -> Self {
        Self::DirectDbError(e)
    }
}

impl From<BombardError<BombardTask>> for BenchError {
    fn from(e: BombardError<BombardTask>) -> Self {
        Self::RookieEngineError(e)
    }
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArgsErr(e) => write!(f, "args error: {e}"),
            Self::DirectDbError(e) => write!(f, "direct operation on db failed. {e}"),
            Self::RookieEngineError(e) => write!(f, "benchmark failed (rookie engine): {e}"),
            Self::FuryEngineError(e) => write!(f, "benchmark failed (fury engine): {e}"),
        }
    }
}

impl std::error::Error for BenchError {}

#[derive(Debug)]
pub enum BenchmarkTaskWorkerError {
    DbError(Error),
}

impl From<Error> for BenchmarkTaskWorkerError {
    fn from(e: Error) -> Self {
        Self::DbError(e)
    }
}

impl fmt::Display for BenchmarkTaskWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DbError(e) => write!(f, "worker failed due to DB error. {e}"),
        }
    }
}
