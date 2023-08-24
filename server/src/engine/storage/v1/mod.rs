/*
 * Created on Mon May 15 2023
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

// raw
mod header_impl;
// impls
mod journal;
mod rw;
// hl
pub mod inf;
mod start_stop;
// test
#[cfg(test)]
pub mod test_util;
#[cfg(test)]
mod tests;

// re-exports
pub use {
    journal::{open_journal, JournalAdapter, JournalWriter},
    rw::BufferedScanner,
};

use crate::util::os::SysIOError as IoError;

pub type SDSSResult<T> = Result<T, SDSSError>;

pub trait SDSSErrorContext {
    type ExtraData;
    fn with_extra(self, extra: Self::ExtraData) -> SDSSError;
}

impl SDSSErrorContext for IoError {
    type ExtraData = &'static str;
    fn with_extra(self, extra: Self::ExtraData) -> SDSSError {
        SDSSError::IoErrorExtra(self, extra)
    }
}

impl SDSSErrorContext for std::io::Error {
    type ExtraData = &'static str;
    fn with_extra(self, extra: Self::ExtraData) -> SDSSError {
        SDSSError::IoErrorExtra(self.into(), extra)
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum SDSSError {
    // IO errors
    /// An IO err
    IoError(IoError),
    /// An IO err with extra ctx
    IoErrorExtra(IoError, &'static str),
    /// A corrupted file
    CorruptedFile(&'static str),
    // process errors
    StartupError(&'static str),
    // header
    /// The entire header is corrupted
    HeaderDecodeCorruptedHeader,
    /// The header versions don't match
    HeaderDecodeHeaderVersionMismatch,
    /// The driver versions don't match
    HeaderDecodeDriverVersionMismatch,
    /// The server versions don't match
    HeaderDecodeServerVersionMismatch,
    /// Expected header values were not matched with the current header
    HeaderDecodeDataMismatch,
    /// The time in the [header/dynrec/rtsig] is in the future
    HeaderTimeConflict,
    // journal
    /// While attempting to handle a basic failure (such as adding a journal entry), the recovery engine ran into an exceptional
    /// situation where it failed to make a necessary repair the log
    JournalWRecoveryStageOneFailCritical,
    /// An entry in the journal is corrupted
    JournalLogEntryCorrupted,
    /// The structure of the journal is corrupted
    JournalCorrupted,
    // internal file structures
    /// While attempting to decode a structure in an internal segment of a file, the storage engine ran into a possibly irrecoverable error
    InternalDecodeStructureCorrupted,
    /// the payload (non-static) part of a structure in an internal segment of a file is corrupted
    InternalDecodeStructureCorruptedPayload,
    /// the data for an internal structure was decoded but is logically invalid
    InternalDecodeStructureIllegalData,
}

impl SDSSError {
    pub const fn corrupted_file(fname: &'static str) -> Self {
        Self::CorruptedFile(fname)
    }
    pub const fn ioerror_extra(error: IoError, extra: &'static str) -> Self {
        Self::IoErrorExtra(error, extra)
    }
    pub fn with_ioerror_extra(self, extra: &'static str) -> Self {
        match self {
            Self::IoError(ioe) => Self::IoErrorExtra(ioe, extra),
            x => x,
        }
    }
}

impl From<IoError> for SDSSError {
    fn from(e: IoError) -> Self {
        Self::IoError(e)
    }
}

impl From<std::io::Error> for SDSSError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.into())
    }
}
