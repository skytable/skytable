/*
 * Created on Thu Mar 17 2022
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

use {crate::util::ExitCode, std::fmt};

pub type HarnessResult<T> = Result<T, HarnessError>;
#[derive(Debug)]
pub enum HarnessError {
    /// Unknown command
    UnknownCommand(String),
    /// Bad arguments
    BadArguments(String),
    /// Child process failure
    ChildError(String, ExitCode),
    /// Other error
    Other(String),
}

impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HarnessError::BadArguments(arg) => write!(f, "Bad arguments: `{}`", arg),
            HarnessError::UnknownCommand(cmd) => write!(f, "Unknown command: `{}`", cmd),
            HarnessError::ChildError(desc, code) => match code {
                Some(code) => write!(f, "The child (`{desc}`) exited with code {code}"),
                None => write!(f, "The child (`{desc}`) exited with a non-zero code"),
            },
            HarnessError::Other(other) => write!(f, "{other}"),
        }
    }
}
