/*
 * Created on Sat Oct 02 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

pub(super) const ERR_CONFLICT: &str =
    "Either use command line arguments or use a configuration file";

use std::fmt;
#[derive(Debug)]
/// Type of configuration error:
/// - The config file was not found (`OSError`)
/// - The config file was invalid (`SyntaxError`)
/// - The config file has an invalid value, which is syntatically correct
/// but logically incorrect (`CfgError`)
/// - The command line arguments have an invalid value/invalid values (`CliArgError`)
pub enum ConfigError {
    OSError(std::io::Error),
    SyntaxError(toml::de::Error),
    CfgError(&'static str),
    CliArgErr(&'static str),
}

impl PartialEq for ConfigError {
    fn eq(&self, oth: &Self) -> bool {
        use ConfigError::*;
        match (self, oth) {
            (OSError(a), OSError(b)) => a.kind() == b.kind(),
            (SyntaxError(a), SyntaxError(b)) => a == b,
            (CfgError(a), CfgError(b)) => a == b,
            (CliArgErr(a), CliArgErr(b)) => a == b,
            _ => false,
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::OSError(e) => write!(f, "error: {}", e),
            ConfigError::SyntaxError(e) => {
                write!(f, "syntax error in configuration file: {}", e)
            }
            ConfigError::CfgError(e) => write!(f, "Configuration error: {}", e),
            ConfigError::CliArgErr(e) => write!(f, "Argument error: {}", e),
        }
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(derr: toml::de::Error) -> Self {
        Self::SyntaxError(derr)
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(derr: std::io::Error) -> Self {
        Self::OSError(derr)
    }
}
