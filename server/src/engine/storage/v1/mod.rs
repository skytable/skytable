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

mod header_impl;
mod rw;
mod start_stop;

use std::io::Error as IoError;

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

#[derive(Debug)]
pub enum SDSSError {
    IoError(IoError),
    IoErrorExtra(IoError, &'static str),
    CorruptedFile(&'static str),
    StartupError(&'static str),
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
