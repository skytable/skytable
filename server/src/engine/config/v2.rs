/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2025, Sayan Nandan <nandansayan@outlook.com>
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

#![allow(dead_code)]

use std::{
    collections::HashMap,
    env::{self, VarError},
    ops,
};

type ConfigResult<T> = Result<T, ConfigError>;

/*
    config item
*/

/// a configuration group
pub trait ConfigGroup: Sized {
    /// parse this configuration group using the provided CLI args
    fn from_cli(args: &mut HashMap<String, String>) -> ConfigResult<ConfigReturn<Self>>;
    /// parse this configuration group using env vars
    fn from_env() -> ConfigResult<ConfigReturn<Self>>;
    /// parse this configuration group using the provided vars
    fn from_env_test(args: &mut HashMap<String, String>) -> ConfigResult<ConfigReturn<Self>>;
}

/// similar to [`ConfigGroup`], this trait is to be used when a config group is more complex to decode
/// and needs to access, for example, multiple variables
pub trait ConfigGroupOverride: Sized {
    /// parse from cli args
    fn from_cli(
        args: &mut HashMap<String, String>,
        cpath: &str,
    ) -> ConfigResult<ConfigReturn<Self>>;
    /// parse from env vars
    fn from_env(cpath: &str) -> ConfigResult<ConfigReturn<Self>>;
    /// parse from provided vars
    fn from_env_test(
        args: &mut HashMap<String, String>,
        cpath: &str,
    ) -> ConfigResult<ConfigReturn<Self>>;
}

/*
    errors
*/

#[derive(Debug, PartialEq)]
/// errors resulting from parsing and evaluating configuration options
pub enum ConfigError {
    /// the configuration item was required but is missing
    Required(String),
    /// failed to parse the configuration item
    ParseError(String),
}

/*
    utils
*/

fn get_var(v: &str) -> ConfigResult<Option<String>> {
    Ok(match env::var(v) {
        Ok(v) => Some(v),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(e)) => {
            return Err(ConfigError::ParseError(format!(
                "failed to parse value for env var `{v}` - {e}",
                e = e.to_string_lossy()
            )))
        }
    })
}

#[derive(Debug, PartialEq)]
/// Modified or unmodified [`ConfigItem`]
pub enum ConfigReturn<T> {
    Modified(T),
    Unmodified(T),
}

impl<T> ops::Deref for ConfigReturn<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Modified(m) | Self::Unmodified(m) => m,
        }
    }
}
