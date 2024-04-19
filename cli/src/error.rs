/*
 * Created on Wed Nov 15 2023
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

use core::fmt;

pub type CliResult<T> = Result<T, CliError>;

#[derive(Debug)]
pub enum CliError {
    QueryError(String),
    ArgsErr(String),
    ClientError(skytable::error::Error),
    IoError(std::io::Error),
    OtherError(&'static str),
}

impl From<libsky::ArgParseError> for CliError {
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

impl From<skytable::error::Error> for CliError {
    fn from(cle: skytable::error::Error) -> Self {
        Self::ClientError(cle)
    }
}

impl From<std::io::Error> for CliError {
    fn from(ioe: std::io::Error) -> Self {
        Self::IoError(ioe)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArgsErr(e) => write!(f, "incorrect arguments. {e}"),
            Self::ClientError(e) => write!(f, "client error. {e}"),
            Self::IoError(e) => write!(f, "i/o error. {e}"),
            Self::QueryError(e) => write!(f, "invalid query. {e}"),
            Self::OtherError(e) => write!(f, "{e}"),
        }
    }
}
