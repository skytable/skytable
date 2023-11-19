/*
 * Created on Mon Jul 20 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]

//! The core library for Skytable
//!
//! This contains modules which are shared by both the `cli` and the `server` modules

/// The size of the read buffer in bytes
pub const BUF_CAP: usize = 8 * 1024; // 8 KB per-connection
/// The current version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The URL
pub const URL: &str = "https://github.com/skytable/skytable";

use std::{
    collections::{hash_map::Entry, HashMap},
    env,
};

/// Returns a formatted version message `{binary} vx.y.z`
pub fn version_msg(binary: &str) -> String {
    format!("{binary} v{VERSION}")
}

#[derive(Debug, PartialEq)]
/// The CLI action that is expected to be performed
pub enum CliAction<A> {
    /// Display the `--help` message
    Help,
    /// Dipslay the `--version`
    Version,
    /// Perform an action using the given args
    Action(A),
}

pub type CliActionMulti = CliAction<HashMap<String, Vec<String>>>;
pub type CliActionSingle = CliAction<HashMap<String, String>>;

/*
    generic cli arg parser
*/

#[derive(Debug, PartialEq)]
/// Argument parse error
pub enum AnyArgsParseError {
    /// The value for the given argument was either incorrectly formatted or missing
    MissingValue(String),
}
/// Parse CLI args, allowing duplicates (bucketing them)
pub fn parse_cli_args_allow_duplicate() -> Result<CliActionMulti, AnyArgsParseError> {
    parse_args(env::args())
}
/// Parse args allowing and bucketing any duplicates
pub fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<CliActionMulti, AnyArgsParseError> {
    let mut ret: HashMap<String, Vec<String>> = HashMap::new();
    let mut args = args.into_iter().skip(1).peekable();
    while let Some(arg) = args.next() {
        if arg == "--help" {
            return Ok(CliAction::Help);
        }
        if arg == "--version" {
            return Ok(CliAction::Version);
        }
        let (arg, value) = extract_arg(arg, &mut args).map_err(AnyArgsParseError::MissingValue)?;
        match ret.get_mut(&arg) {
            Some(values) => {
                values.push(value);
            }
            None => {
                ret.insert(arg, vec![value]);
            }
        }
    }
    Ok(CliAction::Action(ret))
}

/*
    no duplicate arg parser
*/

#[derive(Debug, PartialEq)]
/// Argument parse error
pub enum ArgParseError {
    /// The given argument had a duplicate value
    Duplicate(String),
    /// The given argument did not have an appropriate value
    MissingValue(String),
}
/// Parse all non-repeating CLI arguments
pub fn parse_cli_args_disallow_duplicate() -> Result<CliActionSingle, ArgParseError> {
    parse_args_deny_duplicate(env::args())
}
/// Parse all arguments but deny any duplicates
pub fn parse_args_deny_duplicate(
    args: impl IntoIterator<Item = String>,
) -> Result<CliActionSingle, ArgParseError> {
    let mut ret: HashMap<String, String> = HashMap::new();
    let mut args = args.into_iter().skip(1).peekable();
    while let Some(arg) = args.next() {
        if arg == "--help" {
            return Ok(CliAction::Help);
        }
        if arg == "--version" {
            return Ok(CliAction::Version);
        }
        let (arg, value) = extract_arg(arg, &mut args).map_err(ArgParseError::MissingValue)?;
        match ret.entry(arg) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(oe) => return Err(ArgParseError::Duplicate(oe.key().into())),
        }
    }
    Ok(CliAction::Action(ret))
}

/// Extract an argument:
/// - `--arg=value`
/// - `--arg value`
fn extract_arg(
    arg: String,
    args: &mut impl Iterator<Item = String>,
) -> Result<(String, String), String> {
    let this_args: Vec<&str> = arg.split("=").collect();
    let (arg, value) = if this_args.len() == 2 {
        // self contained arg
        (this_args[0].to_owned(), this_args[1].to_owned())
    } else {
        if this_args.len() == 1 {
            match args.next() {
                None => return Err(arg),
                Some(val) => (arg, val),
            }
        } else {
            return Err(arg);
        }
    };
    Ok((arg, value))
}
