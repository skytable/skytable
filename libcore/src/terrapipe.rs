/*
 * Created on Thu Jul 02 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020 Sayan Nandan
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

//! This is the implementation of the terrabasedb/RFC#1

use std::fmt;

/// The 'TP' protocol tag used in the meta frame
const MF_PROTOCOL_TAG: &'static str = "TP";
/// The 'Q' tag used in the meta frame
const MF_QUERY_TAG: &'static str = "Q";
/// The 'R' tag used in the meta frame
const MF_RESULT_TAG: &'static str = "R";
/// 'SET' tag in the meta frame
const MF_QUERY_SET_TAG: &'static str = "SET";
/// 'GET' tag in the meta frame
const MF_QUERY_GET_TAG: &'static str = "GET";
/// 'UPDATE' tag in the meta frame
const MF_QUERY_UPDATE_TAG: &'static str = "UPDATE";
/// 'DEL' tag in the meta frame
const MF_QUERY_DEL_TAG: &'static str = "DEL";

/// A macro to easily create result packets - to be used by servers
macro_rules! result_packet {
    ($version:expr, $respcode:expr, $data:expr) => {{
        let data = $data.to_string();
        format!(
            "TP/{}/R/{}/{}\n{}",
            $version.to_string(),
            $respcode,
            data.len(),
            $data
        )
    }};
}

/// A macro to easily create query packets - to be used by clients
macro_rules! query_packet {
    ($version:expr, $querytype:expr, $data:expr) => {
        format!(
            "TP/{}/Q/{}\n{}",
            $version.to_string(),
            $querytype.to_string(),
            $data
        )
    };
}

/// Anything that implements `ToString` automatically implements `ToTPArgs`
pub trait ToTPArgs: ToString {
    fn to_tp_args(&self) -> String;
}

/// Minimal representation of _semver_
#[derive(Debug)]
pub struct Version(u8, u8, u8);

impl ToString for Version {
    fn to_string(&self) -> String {
        format!("{}.{}.{}", self.0, self.1, self.2)
    }
}

impl Version {
    /// Parse a new semver using a string in the form x.y.z
    pub fn new_from_str<'a>(val: &'a str) -> Option<Self> {
        let vals: Vec<&str> = val.split(".").collect();
        if vals.len() != 3 {
            return None;
        }
        let semver = (
            vals[0].parse::<u8>(),
            vals[1].parse::<u8>(),
            vals[2].parse::<u8>(),
        );
        if let (Ok(major), Ok(minor), Ok(patch)) = semver {
            return Some(Version(major, minor, patch));
        } else {
            return None;
        }
    }
    /// Use semver to check if the versions are compatible with each other
    pub fn is_compatible_with(&self, other: &Version) -> bool {
        if self.0 == other.0 {
            true
        } else {
            false
        }
    }
}

/// `Key` is a type alias for `String`
type Key = String;
/// `Value` is a type alias for `String`
type Value = String;

/// A fully parsed and ready-to-execute Query action
#[derive(Debug, PartialEq)]
pub enum TPQueryMethod {
    GET(Key),
    SET(Key, Value),
    UPDATE(Key, Value),
    DEL(Key),
}

/// Representation of query types
#[derive(Debug, PartialEq)]
pub enum TPQueryType {
    GET,
    SET,
    UPDATE,
    DEL,
}

impl ToString for TPQueryType {
    fn to_string(&self) -> String {
        use TPQueryType::*;
        if self == &GET {
            return MF_QUERY_GET_TAG.to_owned();
        } else if self == &SET {
            return MF_QUERY_SET_TAG.to_owned();
        } else if self == &UPDATE {
            return MF_QUERY_UPDATE_TAG.to_owned();
        } else {
            return MF_QUERY_DEL_TAG.to_owned();
        }
    }
}

/// Errors that may occur while parsing a query/result packet
#[derive(Debug, PartialEq)]
pub enum TPError {
    /// `0: Okay`
    Okay,
    /// `1: Not Found`
    ///
    /// The target resource could not be found
    NotFound,
    /// `2: Overwrite Error`
    ///
    /// This usually occurs when a query tries to alter the value
    /// of an existing key using `SET` instead of `UPDATE`
    OverwriteError,
    /// `3: Method Not Allowed`
    ///
    /// The client is trying to do something illegal like sending a `Result`
    /// packet instead of a `Query` packet
    MethodNotAllowed,
    /// `4: Internal Server Error`
    ///
    /// There is an internal server error
    InternalServerError,
    /// `5: Invalid Metaframe`
    ///
    /// The metaframe of the query packet has some incorrect partitions or
    /// has an incorrect format
    InvalidMetaframe,
    /// `6: Corrupt Dataframe`
    ///
    /// The dataframe may be missing some bytes or more bytes were expected
    CorruptDataframe,
    /// `7: Protocol Version Mismatch`
    ///
    /// The protocol used by the client is not compatible with the protocol
    /// used by the server
    ProtocolVersionMismatch,
    /// `8: Corrupt Packet`
    ///
    /// The packet is either empty or is missing a newline
    CorruptPacket,
}

impl TPError {
    /// Returns a `TPError` variant from an `u8` and returns `None` if it
    /// isn't a valid code
    pub fn from_u8(code: u8) -> Option<Self> {
        use TPError::*;
        let val = match code {
            0 => Okay,
            1 => NotFound,
            2 => OverwriteError,
            3 => MethodNotAllowed,
            4 => InternalServerError,
            5 => InvalidMetaframe,
            6 => CorruptDataframe,
            7 => ProtocolVersionMismatch,
            8 => CorruptPacket,
            _ => return None,
        };
        Some(val)
    }
}

/// Errors that may occur while parsing a query packet
#[derive(Debug, PartialEq)]
pub struct TPQueryError(TPError);

#[cfg(test)]
#[test]
fn test_result_macros() {
    let proto_version = Version(0, 1, 0);
    let query = query_packet!(proto_version, TPQueryType::GET, "sayan");
    let result = result_packet!(proto_version, 0, 17);
    let query_should_be = "TP/0.1.0/Q/GET\nsayan".to_owned();
    let result_should_be = "TP/0.1.0/R/0/2\n17".to_owned();
    assert_eq!(query, query_should_be);
    assert_eq!(result, result_should_be);
}
/// Parse a query packet that is sent by the client
/// ## Returns
/// This returns a `TPQueryMethod` which can be used to execute the action or
/// it returns a `TPQueryError` in the case an error occurs while parsing the packet
pub fn parse_query_packet(
    packet: String,
    self_version: &Version,
) -> Result<TPQueryMethod, TPQueryError> {
    let rlines: Vec<&str> = packet.lines().collect();
    if rlines.len() < 2 {
        return Err(TPQueryError(TPError::CorruptPacket));
    }
    let metaframe: Vec<&str> = rlines[0].split("/").collect();
    if metaframe.len() != 4 {
        return Err(TPQueryError(TPError::InvalidMetaframe));
    }

    if metaframe[0].ne(MF_PROTOCOL_TAG) {
        return Err(TPQueryError(TPError::InvalidMetaframe));
    }
    if let Some(v) = Version::new_from_str(metaframe[1]) {
        if self_version.is_compatible_with(&v) {
            ()
        } else {
            return Err(TPQueryError(TPError::ProtocolVersionMismatch));
        }
    }

    if metaframe[2].ne(MF_QUERY_TAG) {
        return Err(TPQueryError(TPError::InvalidMetaframe));
    }
    /* TODO: This is temporary - the dataframe in the future may be
    multiple lines long
    */
    let dataframe: Vec<&str> = rlines[1].split_whitespace().collect();
    if dataframe.len() == 0 {
        return Err(TPQueryError(TPError::CorruptDataframe));
    }
    match metaframe[3] {
        MF_QUERY_GET_TAG => {
            // This is a GET query
            if let Some(key) = dataframe.get(0) {
                if dataframe.get(1).is_none() {
                    return Ok(TPQueryMethod::GET(key.to_string()));
                }
            }
        }
        MF_QUERY_SET_TAG => {
            // This is a SET query
            if let Some(key) = dataframe.get(0) {
                if let Some(value) = dataframe.get(1) {
                    return Ok(TPQueryMethod::SET(key.to_string(), value.to_string()));
                }
            }
        }
        MF_QUERY_UPDATE_TAG => {
            // This is a SET query
            if let Some(key) = dataframe.get(0) {
                if let Some(value) = dataframe.get(1) {
                    return Ok(TPQueryMethod::UPDATE(key.to_string(), value.to_string()));
                }
            }
        }
        MF_QUERY_DEL_TAG => {
            // This is a DEL query
            if let Some(key) = dataframe.get(0) {
                if dataframe.get(1).is_none() {
                    return Ok(TPQueryMethod::DEL(key.to_string()));
                }
            }
        }
        // Some random illegal command
        _ => return Err(TPQueryError(TPError::MethodNotAllowed)),
    }
    Err(TPQueryError(TPError::CorruptDataframe))
}

/// Errors that may occur while parsing a result packet
#[derive(Debug, PartialEq)]
pub enum TPResultError {
    /// The standard `TPError`s
    StandardError(TPError),
    /// In the event someone tried to parse a result from a _patched_ server
    /// which sent a weird error code, use this variant
    UnrecognizedError(String),
}

/// `TPResult` is type alias for `String`
pub type TPResult = String;

/// Parse a result packet sent by the server
/// ## Returns
/// If there was no error in parsing the packet, then a `TPResult` is returned.
/// Otherwise a `TPResultError` is returned
pub fn parse_result_packet(
    packet: String,
    self_version: &Version,
) -> Result<TPResult, TPResultError> {
    use TPResultError::*;
    let rlines: Vec<&str> = packet.lines().collect();
    if rlines.len() < 2 {
        return Err(StandardError(TPError::CorruptPacket));
    }

    let metaframe: Vec<&str> = rlines[0].split("/").collect();
    if metaframe.len() != 5 {
        return Err(StandardError(TPError::InvalidMetaframe));
    }
    let dataframe: Vec<&str> = rlines[1].split(" ").collect();

    if metaframe[0].ne(MF_PROTOCOL_TAG) || metaframe[2].ne(MF_RESULT_TAG) {
        return Err(StandardError(TPError::InvalidMetaframe));
    }

    // Check version compatibility
    if let Some(version) = Version::new_from_str(metaframe[1]) {
        if !self_version.is_compatible_with(&version) {
            return Err(StandardError(TPError::ProtocolVersionMismatch));
        }
    } else {
        return Err(StandardError(TPError::InvalidMetaframe));
    }

    let respcode = match metaframe[4].parse::<u8>() {
        Ok(v) => v,
        Err(_) => return Err(UnrecognizedError(metaframe[4].to_owned())),
    };

    if let Some(respcode) = TPError::from_u8(respcode) {
        match respcode {
            Okay => {
                // Enter dataframe and check result
                if let Some(value) = dataframe.get(0) {
                    if dataframe.get(1).is_none() {
                        return Ok(value.to_string());
                    } else {
                        return Err(StandardError(TPError::CorruptDataframe));
                    }
                } else {
                    return Err(StandardError(TPError::CorruptDataframe));
                }
            }
            x @ _ => return Err(StandardError(x)),
        }
    } else {
        return Err(UnrecognizedError(respcode.to_string()));
    }
}

#[cfg(test)]
#[test]
fn test_query_packet_parsing() {
    let qpacket = query_packet!(Version(0, 1, 0), TPQueryType::GET, "sayan");
    let query_should_be = TPQueryMethod::GET("sayan".to_owned());
    let parsed_qpacket = parse_query_packet(qpacket, &Version(0, 1, 0)).unwrap();
    assert_eq!(query_should_be, parsed_qpacket);
}

#[cfg(test)]
#[test]
fn test_result_packet_parsing() {
    let v = Version(0, 1, 0);
    let rpacket = result_packet!(v, 0, 18);
    let result_should_be = 18.to_string();
    let parsed_rpacket = parse_result_packet(rpacket, &v).unwrap();
    assert_eq!(result_should_be, parsed_rpacket);
}
