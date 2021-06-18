/*
 * Created on Fri Jun 18 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use std::fmt;

/// A trait for aggresive erroring
pub trait ExitError<T> {
    /// Abort the process if the type errors with an error code or
    /// return the type
    fn exit_error<Ms>(self, msg: Ms) -> T
    where
        Ms: ToString;
}

impl<T, E> ExitError<T> for Result<T, E>
where
    E: fmt::Display,
{
    fn exit_error<Ms>(self, msg: Ms) -> T
    where
        Ms: ToString,
    {
        match self {
            Self::Err(e) => {
                log::error!("{} : '{}'", msg.to_string(), e);
                std::process::exit(0x100);
            }
            Self::Ok(v) => v,
        }
    }
}

impl<T> ExitError<T> for Option<T> {
    fn exit_error<Ms>(self, msg: Ms) -> T
    where
        Ms: ToString,
    {
        match self {
            Self::None => {
                log::error!("{}", msg.to_string());
                std::process::exit(0x100);
            }
            Self::Some(v) => v,
        }
    }
}
