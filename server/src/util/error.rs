/*
 * Created on Sat Mar 26 2022
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

use crate::storage::v1::{error::StorageEngineError, sengine::SnapshotEngineError};
use openssl::{error::ErrorStack as SslErrorStack, ssl::Error as SslError};
use std::{fmt, io::Error as IoError};

pub type SkyResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Storage(StorageEngineError),
    IoError(IoError),
    IoErrorExtra(IoError, String),
    OtherError(String),
    TlsError(SslError),
    SnapshotEngineError(SnapshotEngineError),
}

impl Error {
    pub fn ioerror_extra(ioe: IoError, extra: impl ToString) -> Self {
        Self::IoErrorExtra(ioe, extra.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(serr) => write!(f, "Storage engine error: {}", serr),
            Self::IoError(nerr) => write!(f, "I/O error: {}", nerr),
            Self::IoErrorExtra(ioe, extra) => write!(f, "I/O error while {extra}: {ioe}"),
            Self::OtherError(oerr) => write!(f, "Error: {}", oerr),
            Self::TlsError(terr) => write!(f, "TLS error: {}", terr),
            Self::SnapshotEngineError(snaperr) => write!(f, "Snapshot engine error: {snaperr}"),
        }
    }
}

impl From<IoError> for Error {
    fn from(ioe: IoError) -> Self {
        Self::IoError(ioe)
    }
}

impl From<StorageEngineError> for Error {
    fn from(see: StorageEngineError) -> Self {
        Self::Storage(see)
    }
}

impl From<SslError> for Error {
    fn from(sslerr: SslError) -> Self {
        Self::TlsError(sslerr)
    }
}

impl From<SslErrorStack> for Error {
    fn from(estack: SslErrorStack) -> Self {
        Self::TlsError(estack.into())
    }
}

impl From<SnapshotEngineError> for Error {
    fn from(snaperr: SnapshotEngineError) -> Self {
        Self::SnapshotEngineError(snaperr)
    }
}
