/*
 * Created on Sat Jul 18 2020
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

//! # The Terrapipe protocol
//! This module implements primitives for the Terrapipe protocol
//!
//! Query and Response packet handling modules can be found in the `de`, `query`
//! and `response` modules from the crate root.

use std::error::Error;
use std::fmt;

pub const ADDR: &'static str = "127.0.0.1:2003";

/// Default query metaline buffer size
pub const DEF_QMETALINE_BUFSIZE: usize = 44;
/// Default query metalayout buffer size
pub const DEF_QMETALAYOUT_BUFSIZE: usize = 576;
/// Default query dataframe buffer size
pub const DEF_QDATAFRAME_BUSIZE: usize = 4096;
pub mod responses {
    //! Empty responses, mostly errors, which are statically compiled
    use lazy_static::lazy_static;
    lazy_static! {
        /// Empty `0`(Okay) response - without any content
        pub static ref RESP_OKAY_EMPTY: Vec<u8> = "*!0!0!0\n".as_bytes().to_owned();
        /// `1` Not found response
        pub static ref RESP_NOT_FOUND: Vec<u8> = "*!1!0!0\n".as_bytes().to_owned();
        /// `2` Overwrite Error response
        pub static ref RESP_OVERWRITE_ERROR: Vec<u8> = "*!2!0!0\n".as_bytes().to_owned();
        /// `3` Invalid Metaframe response
        pub static ref RESP_INVALID_MF: Vec<u8> = "*!3!0!0\n".as_bytes().to_owned();
        /// `4` ArgumentError frame response
        pub static ref RESP_ARG_ERROR: Vec<u8> = "*!4!0!0\n".as_bytes().to_owned();
        /// `5` Internal server error response
        pub static ref RESP_SERVER_ERROR: Vec<u8> = "*!5!0!0\n".as_bytes().to_owned();
    }
}

/// Response codes returned by the server
#[derive(Debug, PartialEq)]
pub enum RespCodes {
    /// `0`: Okay (Empty Response) - use the `ResponseBuilder` for building
    /// responses that contain data
    Okay,
    /// `1`: Not Found
    NotFound,
    /// `2`: Overwrite Error
    OverwriteError,
    /// `3`: Invalid Metaframe
    InvalidMetaframe,
    /// `4`: ArgumentError
    ArgumentError,
    /// `5`: Server Error
    ServerError,
    /// `6`: Some other error - the wrapped `String` will be returned in the response body.
    /// Just a note, this gets quite messy, especially when we're using it for deconding responses
    OtherError(Option<String>),
}

impl fmt::Display for RespCodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use RespCodes::*;
        match self {
            Okay => Ok(()),
            NotFound => writeln!(f, "ERROR: Couldn't find the key"),
            OverwriteError => writeln!(f, "ERROR: Existing values cannot be overwritten"),
            InvalidMetaframe => writeln!(f, "ERROR: Invalid metaframe"),
            ArgumentError => writeln!(f, "ERROR: The command is not in the correct format"),
            ServerError => writeln!(f, "ERROR: The server had an internal error"),
            OtherError(e) => match e {
                None => writeln!(f, "ERROR: Some unknown error occurred"),
                Some(e) => writeln!(f, "ERROR: {}", e),
            },
        }
    }
}

impl Error for RespCodes {}

impl From<RespCodes> for u8 {
    fn from(rcode: RespCodes) -> u8 {
        use RespCodes::*;
        match rcode {
            Okay => 0,
            NotFound => 1,
            OverwriteError => 2,
            InvalidMetaframe => 3,
            ArgumentError => 4,
            ServerError => 5,
            OtherError(_) => 6,
        }
    }
}

impl From<RespCodes> for char {
    fn from(rcode: RespCodes) -> char {
        use RespCodes::*;
        match rcode {
            Okay => '0',
            NotFound => '1',
            OverwriteError => '2',
            InvalidMetaframe => '3',
            ArgumentError => '4',
            ServerError => '5',
            OtherError(_) => '6',
        }
    }
}

impl RespCodes {
    pub fn from_str(val: &str, extra: Option<String>) -> Option<Self> {
        use RespCodes::*;
        let res = match val.parse::<u8>() {
            Ok(val) => match val {
                0 => Okay,
                1 => NotFound,
                2 => OverwriteError,
                3 => InvalidMetaframe,
                4 => ArgumentError,
                5 => ServerError,
                6 => OtherError(extra),
                _ => return None,
            },
            Err(_) => return None,
        };
        Some(res)
    }
    pub fn from_u8(val: u8, extra: Option<String>) -> Option<Self> {
        use RespCodes::*;
        let res = match val {
            0 => Okay,
            1 => NotFound,
            2 => OverwriteError,
            3 => InvalidMetaframe,
            4 => ArgumentError,
            5 => ServerError,
            6 => OtherError(extra),
            _ => return None,
        };
        Some(res)
    }
    pub fn from_utf8(val: u8) -> Option<Self> {
        let result = match val.checked_sub(48) {
            Some(r) => r,
            None => return None,
        };
        if result > 6 {
            return None;
        }
        return RespCodes::from_u8(result, None);
    }
}

/// Representation of the query action type - pipelined or simple
#[derive(Debug, PartialEq)]
pub enum ActionType {
    Simple,
    Pipeline,
}

/// Anything that implements this trait can be written to a `TCPStream`, i.e it can
/// be used to return a response
pub trait RespBytes {
    fn into_response(&self) -> Vec<u8>;
}

impl RespBytes for RespCodes {
    fn into_response(&self) -> Vec<u8> {
        use responses::*;
        use RespCodes::*;
        match self {
            Okay => RESP_OKAY_EMPTY.to_owned(),
            NotFound => RESP_NOT_FOUND.to_owned(),
            OverwriteError => RESP_OVERWRITE_ERROR.to_owned(),
            InvalidMetaframe => RESP_INVALID_MF.to_owned(),
            ArgumentError => RESP_ARG_ERROR.to_owned(),
            ServerError => RESP_SERVER_ERROR.to_owned(),
            OtherError(e) => match e {
                Some(e) => {
                    // The dataframe len includes the LF character
                    let dataframe_len = e.len() + 1;
                    // The metalayout len includes a LF and '#' character
                    let metalayout_len = e.len().to_string().len() + 2;
                    format!(
                        "*!6!{}!{}\n#{}\n{}\n",
                        dataframe_len,
                        metalayout_len,
                        e.len(),
                        e
                    )
                    .as_bytes()
                    .to_owned()
                }
                None => format!("*!6!0!0\n").as_bytes().to_owned(),
            },
        }
    }
}
