/*
 * Created on Tue Aug 04 2020
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

//! # `de`
//! The `de` module provides primitives for deserialization primitives for parsing
//! query and response packets

use crate::terrapipe::RespCodes;
use std::fmt;
use std::ops::Deref;
use std::vec::IntoIter;
/// A wrapper around a `Vec<String>` which represents a data group in the dataframe
#[derive(Debug, PartialEq)]
pub struct DataGroup(pub Vec<String>);

impl fmt::Display for DataGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::util::terminal::*;
        /*
        TODO(@ohsayan): Implement proper formatting for the response. That is,
        for `!` print the respective error code, for `+` print the corresponding
        array or single-value
        */
        if self.len() == 0 {
            // The server returned a zero-sized array
            return write!(f, "[]");
        }
        for token in self.iter() {
            if token.len() == 0 {
                write!(f, "\"\"")?;
                continue;
            }
            let mut byter = token.bytes().peekable();
            match byter.next().unwrap() {
                b'!' => {
                    // This is an error
                    match byter.next() {
                        Some(tok) => {
                            match RespCodes::from_utf8(tok) {
                                Some(code) => {
                                    use RespCodes::*;
                                    match code {
                                        Okay => write_okay("(Okay)")?,
                                        NotFound => {
                                            // `NotFound` is the same as a `Nil` value
                                            write_error("(Nil)")?;
                                        }
                                        OverwriteError => {
                                            write_error(
                                                "ERROR: Existing values cannot be overwritten",
                                            )?;
                                        }
                                        ActionError => {
                                            write_error("ERROR: An invalid request was sent")?;
                                        }
                                        PacketError => write_error(
                                            "ERROR: The action is not in the correct format",
                                        )?,
                                        ServerError => write_error(
                                            "ERROR: An error occurred on the serve-side",
                                        )?,
                                        OtherError(_) => {
                                            let rem = byter.collect::<Vec<u8>>();
                                            if rem.len() == 0 {
                                                write_error("ERROR: An unknown error occurred")?;
                                            } else {
                                                write_error(format!(
                                                    "ERROR: {}",
                                                    String::from_utf8_lossy(&rem)
                                                ))?;
                                            }
                                        }
                                    }
                                }
                                None => {
                                    let rem = byter.collect::<Vec<u8>>();
                                    if rem.len() == 0 {
                                        write_error("ERROR: An unknown error occurred")?;
                                    } else {
                                        write_error(format!(
                                            "ERROR: '{}{}'",
                                            char::from(tok),
                                            String::from_utf8_lossy(&rem)
                                        ))?;
                                    }
                                }
                            }
                        }
                        None => write_error("ERROR: An unknown error occurred")?,
                    }
                }
                b'+' => {
                    // This is a positive response
                    let rem = byter.collect::<Vec<u8>>();
                    write!(f, "\"{}\"", String::from_utf8_lossy(&rem))?;
                }
                x @ _ => {
                    // Unknown response
                    let rem = byter.collect::<Vec<u8>>();
                    write_warning(format!(
                        "Unknown response: \"{}{}\"",
                        x,
                        String::from_utf8_lossy(&rem)
                    ))?;
                }
            }
            write!(f, "\n")?;
        }
        Ok(())
    }
}
impl IntoIterator for DataGroup {
    type Item = String;
    type IntoIter = IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl DataGroup {
    /// Create a new `DataGroup`
    pub fn new(v: Vec<String>) -> Self {
        DataGroup(v)
    }
    /// Drops the `DataGroup` instance returning the `Vec<String>` that it held
    pub fn finish_into_vector(self) -> Vec<String> {
        self.0
    }
}

impl Deref for DataGroup {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
