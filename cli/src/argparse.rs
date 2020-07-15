/*
 * Created on Wed Jul 01 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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
use std::process;

/// `SET` command line argument
const ARG_SET: &'static str = "set";
/// `GET` command line argument
const ARG_GET: &'static str = "get";
/// `UPDATE` command line argument
const ARG_UPDATE: &'static str = "update";
/// `EXIT` command line argument
const ARG_EXIT: &'static str = "exit";
/// Error message when trying to get multiple keys at the same time (TEMP)
const ERR_GET_MULTIPLE: &'static str = "GET only supports fetching one key at a time";
/// Error message when trying to set multiple keys at the same time (TEMP)
const ERR_SET_MULTIPLE: &'static str = "SET only supports setting one key at a time";
/// Error message when trying to update multiple keys at the same time (TEMP)
const ERR_UPDATE_MULTIPLE: &'static str = "UPDATE only supports updating one key at a time";

/// Representation for a key/value pair
#[derive(Debug, PartialEq)]
pub struct KeyValue(Key, String);

/// `Key` an alias for `String`
pub type Key = String;

/// Directly parsed commands from the command line
#[derive(Debug, PartialEq)]
pub enum Commands {
    /// A `GET` command
    GET,
    /// A `SET` command
    SET,
    /// An `UPDATE` command
    UPDATE,
}

/// Prepared commands that can be executed
#[derive(Debug, PartialEq)]
pub enum FinalCommands {
    /// Parsed `GET` command
    GET(Key),
    /// Parsed `SET` command
    SET(KeyValue),
    /// Parsed `UPDATE` command
    UPDATE(KeyValue),
}

/// Errors that may occur while parsing arguments
#[derive(Debug, PartialEq)]
pub enum ArgsError {
    /// Expected more arguments
    ExpectedMoreArgs,
    /// Failed to fetch an argument
    ArgGetError,
    /// Unexpected argument
    UndefinedArgError(String),
    /// Other error
    Other(&'static str),
}

impl fmt::Display for ArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ArgsError::*;
        match self {
            ExpectedMoreArgs => write!(f, "error: Expected more arguments"),
            ArgGetError => write!(f, "error: Failed to get argument"),
            UndefinedArgError(arg) => write!(f, "error: Undefined argument '{}'", arg),
            Other(e) => write!(f, "error: {}", e),
        }
    }
}

/// Exits the process with an error message
pub const EXIT_ERROR: fn(&'static str) -> ! = |err| {
    eprintln!("error: {}", err);
    process::exit(0x100);
};

/// ### Parse a `String` argument into a corresponding `FinalCommands` variant
/// #### Errors
/// This returns an `Err(ArgsError)` if it fails to parse the arguments and the errors
/// can be displayed directly (i.e the errors implement the `fmt::Display` trait)
pub fn parse_args(args: String) -> Result<FinalCommands, ArgsError> {
    let args: Vec<String> = args
        .split_whitespace()
        .map(|v| v.to_lowercase().to_string())
        .collect();
    // HACK(@ohsayan) This is a temporary workaround we will need a proper parser
    let primary_arg = match args.get(0) {
        Some(arg) => arg,
        None => {
            return Err(ArgsError::ArgGetError);
        }
    };
    let mut actions = Vec::with_capacity(3);
    match primary_arg.as_str() {
        ARG_GET => actions.push(Commands::GET),
        ARG_SET => actions.push(Commands::SET),
        ARG_UPDATE => actions.push(Commands::UPDATE),
        ARG_EXIT => {
            println!("Goodbye!");
            process::exit(0x00);
        }
        _ => {
            return Err(ArgsError::UndefinedArgError(primary_arg.to_owned()));
        }
    }

    match actions[0] {
        Commands::GET => {
            // Now read next command
            if let Some(arg) = args.get(1) {
                if args.get(2).is_none() {
                    return Ok(FinalCommands::GET(arg.to_string()));
                } else {
                    return Err(ArgsError::Other(ERR_GET_MULTIPLE));
                }
            } else {
                return Err(ArgsError::ExpectedMoreArgs);
            }
        }
        Commands::SET => {
            // Now read next command
            if let (Some(key), Some(value)) = (args.get(1), args.get(2)) {
                if args.get(3).is_none() {
                    return Ok(FinalCommands::SET(KeyValue(
                        key.to_string(),
                        value.to_string(),
                    )));
                } else {
                    return Err(ArgsError::Other(ERR_SET_MULTIPLE));
                }
            } else {
                return Err(ArgsError::ExpectedMoreArgs);
            }
        }
        Commands::UPDATE => {
            if let (Some(key), Some(value)) = (args.get(1), args.get(2)) {
                if args.get(3).is_none() {
                    return Ok(FinalCommands::UPDATE(KeyValue(
                        key.to_string(),
                        value.to_string(),
                    )));
                } else {
                    return Err(ArgsError::Other(ERR_UPDATE_MULTIPLE));
                }
            } else {
                return Err(ArgsError::ExpectedMoreArgs);
            }
        }
    }
}

#[cfg(test)]
#[test]
fn test_argparse_valid_cmds() {
    let test_set_arg1 = "set sayan 100".to_owned();
    let test_set_arg2 = "SET sayan 100".to_owned();
    let test_set_arg3 = "SeT sayan 100".to_owned();
    let test_get_arg1 = "get sayan".to_owned();
    let test_get_arg2 = "GET sayan".to_owned();
    let test_get_arg3 = "GeT sayan".to_owned();
    let test_set_result: Result<FinalCommands, ArgsError> = Ok(FinalCommands::SET(KeyValue(
        "sayan".to_owned(),
        "100".to_owned(),
    )));
    let test_get_result: Result<FinalCommands, ArgsError> =
        Ok(FinalCommands::GET("sayan".to_owned()));
    assert_eq!(parse_args(test_get_arg1), test_get_result);
    assert_eq!(parse_args(test_get_arg2), test_get_result);
    assert_eq!(parse_args(test_get_arg3), test_get_result);
    assert_eq!(parse_args(test_set_arg1), test_set_result);
    assert_eq!(parse_args(test_set_arg2), test_set_result);
    assert_eq!(parse_args(test_set_arg3), test_set_result);
}

#[cfg(test)]
#[test]
fn test_argparse_invalid_cmds() {
    let test_multiple_get = "get sayan supersayan".to_owned();
    let test_multiple_set = "set sayan 18 supersayan 118".to_owned();
    let test_multiple_update = "update sayan 19 supersayan 119".to_owned();
    let result_multiple_get: Result<FinalCommands, ArgsError> =
        Err(ArgsError::Other(ERR_GET_MULTIPLE));
    let result_multiple_set: Result<FinalCommands, ArgsError> =
        Err(ArgsError::Other(ERR_SET_MULTIPLE));
    let result_multiple_update: Result<FinalCommands, ArgsError> =
        Err(ArgsError::Other(ERR_UPDATE_MULTIPLE));
    assert_eq!(parse_args(test_multiple_get), result_multiple_get);
    assert_eq!(parse_args(test_multiple_set), result_multiple_set);
    assert_eq!(parse_args(test_multiple_update), result_multiple_update);
}
