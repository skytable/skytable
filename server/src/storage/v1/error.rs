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

use crate::corestore::memstore::ObjectID;
use core::fmt;
use std::io::Error as IoError;

pub type StorageEngineResult<T> = Result<T, StorageEngineError>;

pub trait ErrorContext<T> {
    /// Provide some context to an error
    fn map_err_context(self, extra: impl ToString) -> StorageEngineResult<T>;
}

impl<T> ErrorContext<T> for Result<T, IoError> {
    fn map_err_context(self, extra: impl ToString) -> StorageEngineResult<T> {
        self.map_err(|e| StorageEngineError::ioerror_extra(e, extra.to_string()))
    }
}

#[derive(Debug)]
pub enum StorageEngineError {
    /// An I/O Error
    IoError(IoError),
    /// An I/O Error with extra context
    IoErrorExtra(IoError, String),
    /// A corrupted file
    CorruptedFile(String),
    /// The file contains bad metadata
    BadMetadata(String),
}

impl StorageEngineError {
    pub fn corrupted_partmap(ksid: &ObjectID) -> Self {
        Self::CorruptedFile(format!("{ksid}/PARTMAP", ksid = unsafe { ksid.as_str() }))
    }
    pub fn bad_metadata_in_table(ksid: &ObjectID, table: &ObjectID) -> Self {
        unsafe {
            Self::CorruptedFile(format!(
                "{ksid}/{table}",
                ksid = ksid.as_str(),
                table = table.as_str()
            ))
        }
    }
    pub fn corrupted_preload() -> Self {
        Self::CorruptedFile("PRELOAD".into())
    }
    pub fn ioerror_extra(ioe: IoError, extra: impl ToString) -> Self {
        Self::IoErrorExtra(ioe, extra.to_string())
    }
}

impl From<IoError> for StorageEngineError {
    fn from(ioe: IoError) -> Self {
        Self::IoError(ioe)
    }
}

impl fmt::Display for StorageEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(ioe) => write!(f, "I/O error: {}", ioe),
            Self::IoErrorExtra(ioe, extra) => write!(f, "I/O error while {extra}: {ioe}"),
            Self::CorruptedFile(cfile) => write!(f, "file `{cfile}` is corrupted"),
            Self::BadMetadata(file) => write!(f, "bad metadata in file `{file}`"),
        }
    }
}
