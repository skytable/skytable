/*
 * Created on Thu Jul 02 2020
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
#![allow(unused)]

//! This implements the Terrapipe protocol
use lazy_static::lazy_static;
use std::fmt;
use std::mem;

/// Current version of the terrapipe protocol
pub const SELF_VERSION: Version = Version(0, 1, 0);
/// Metaframe protocol tag
pub const MF_PROTOCOL_TAG: &'static str = "TP";
/// Metaframe query tag
pub const MF_QUERY_TAG: &'static str = "Q";
/// Metaframe response tag
pub const MF_RESPONSE_TAG: &'static str = "R";
/// Metaframe separator ("/")
pub const MF_SEPARATOR: &'static str = "/";
/// Metaframe `GET` tag
pub const MF_METHOD_GET: &'static str = "GET";
/// Metaframe `SET` tag
pub const MF_METHOD_SET: &'static str = "SET";
/// Metaframe `UPDATE` tag
pub const MF_METHOD_UPDATE: &'static str = "UPDATE";
/// Metaframe `DEL` tag
pub const MF_METHOD_DEL: &'static str = "DEL";
/// ## The default buffer size for the query metaframe
/// This currently enables sizes upto 2^64 to be handled. Since
/// `floor(log10(2^64))+1` = 20 digits and the maximum size of the query
/// metaframe __currently__ is 26 - the buffer size, should be 46
pub const DEF_Q_META_BUFSIZE: usize = 46;
/// ## The default buffer size for the response metaframe
/// This currently enables sizes upto 2^64 to be handled. Since 2^64 has 20 digits
/// and the maximum size of the response metaframe is 20 - the buffer size is kept at 40
pub const DEF_R_META_BUFSIZE: usize = 40;

/// Constant function to generate a response packet
const RESPONSE_PACKET: fn(version: Version, respcode: u8, data: &str) -> Vec<u8> =
    |version, respcode, data| {
        let res = format!(
            "TP/{}.{}.{}/R/{}/{}\n{}",
            version.0,
            version.1,
            version.2,
            respcode,
            data.len(),
            data,
        );
        res.as_bytes().to_vec()
    };

/// Constant function to generate a query packet
const QUERY_PACKET: fn(version: Version, method: String, data: String) -> Vec<u8> =
    |version, method, data| {
        let res = format!(
            "TP/{}.{}.{}/Q/{}/{}\n{}",
            version.0,
            version.1,
            version.2,
            method,
            data.len(),
            data
        );
        res.as_bytes().to_vec()
    };

// Evaluate the common error packets at compile-time
lazy_static! {
    /// Success: empty response
    static ref RESP_OKAY_EMPTY: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 0, "");
    /// Error response when the target is not found
    static ref RESP_NOT_FOUND: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 1, "");
    /// Error response when the target key already exists and cannot be overwritten
    static ref RESP_OVERWRITE_ERROR: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 2, "");
    /// Error response when the method in the query is not allowed/deprecated/not supported yet
    static ref RESP_METHOD_NOT_ALLOWED: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 3, "");
    /// Error response when the server has a problem processing the response
    static ref RESP_INTERNAL_SERVER_ERROR: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 4, "");
    /// Error response when the metaframe contains invalid tokens
    static ref RESP_INVALID_MF: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 5, "");
    /// Error response when the dataframe doesn't contain the expected bytes
    static ref RESP_CORRUPT_DF: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 6, "");
    /// Error response when the protocol used by the client is not supported by the server
    static ref RESP_PROTOCOL_VERSION_MISMATCH: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 7, "");
    /// Error response when the query packet is missing the basic information, usually a newline
    static ref RESP_CORRUPT_PACKET: Vec<u8> = RESPONSE_PACKET(SELF_VERSION, 8, "");
}

/// A minimal _Semver_ implementation
pub struct Version(pub u8, pub u8, pub u8);

impl Version {
    /// Create a new `Version` instance from an `&str`
    /// ## Errors
    /// Returns `None` when the string passed isn't in the correct format
    pub fn from_str<'a>(vstr: &'a str) -> Option<Self> {
        let vstr: Vec<&str> = vstr.split(".").collect();
        if vstr.len() != 3 {
            return None;
        }
        if let (Ok(major), Ok(minor), Ok(patch)) = (
            vstr[0].parse::<u8>(),
            vstr[1].parse::<u8>(),
            vstr[2].parse::<u8>(),
        ) {
            Some(Version(major, minor, patch))
        } else {
            None
        }
    }
    /// Check if `other` is compatible with the current version as required
    /// by _Semver_
    pub fn incompatible_with(&self, other: &Version) -> bool {
        if self.0 == other.0 {
            false
        } else {
            true
        }
    }
}

/// Response codes which are returned by the server
pub enum ResponseCodes {
    /// `0` : Okay
    Okay(Option<String>),
    /// `1` : Not Found
    NotFound,
    /// `2` : Overwrite Error
    OverwriteError,
    /// `3` : Method Not Allowed
    MethodNotAllowed,
    /// `4` : Internal Server Error
    InternalServerError,
    /// `5` : Invalid Metaframe
    InvalidMetaframe,
    /// `6` : Corrupt Dataframe
    CorruptDataframe,
    /// `7` : Protocol Version Mismatch
    ProtocolVersionMismatch,
    /// `8` : Corrupt Packet
    CorruptPacket,
}

impl ResponseCodes {
    /// Instantiate a new `ResponseCodes` variant from an `u8` value
    /// ## Errors
    /// Returns `None` when the `u8` doesn't correspond to any of the
    /// response codes
    pub fn from_u8(code: u8) -> Option<Self> {
        use ResponseCodes::*;
        let c = match code {
            0 => Okay(None),
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
        Some(c)
    }
}

/// Any object implementing this trait can be converted into a response
pub trait ResponseBytes {
    /// Return a `Vec<u8>` with the response
    fn response_bytes(&self) -> Vec<u8>;
}

impl ResponseBytes for ResponseCodes {
    fn response_bytes(&self) -> Vec<u8> {
        use ResponseCodes::*;
        match self {
            Okay(val) => {
                if let Some(dat) = val {
                    RESPONSE_PACKET(SELF_VERSION, 0, dat)
                } else {
                    RESP_OKAY_EMPTY.to_vec()
                }
            }
            NotFound => RESP_NOT_FOUND.to_vec(),
            OverwriteError => RESP_OVERWRITE_ERROR.to_vec(),
            MethodNotAllowed => RESP_METHOD_NOT_ALLOWED.to_vec(),
            InternalServerError => RESP_INTERNAL_SERVER_ERROR.to_vec(),
            InvalidMetaframe => RESP_INVALID_MF.to_vec(),
            CorruptDataframe => RESP_CORRUPT_DF.to_vec(),
            ProtocolVersionMismatch => RESP_PROTOCOL_VERSION_MISMATCH.to_vec(),
            CorruptPacket => RESP_CORRUPT_PACKET.to_vec(),
        }
    }
}

/// Query methods that can be used by the client
#[derive(Debug, PartialEq)]
pub enum QueryMethod {
    /// A `GET` query
    GET,
    /// A `SET` query
    SET,
    /// An `UPDATE` query
    UPDATE,
    /// A `DEL` query
    DEL,
}

/// The query metaframe
#[derive(Debug, PartialEq)]
pub struct QueryMetaframe {
    /// The content size that is to be read by the server, from the data packet
    content_size: usize,
    /// The query method that the client has issued to the server
    method: QueryMethod,
}

impl QueryMetaframe {
    /// Create a query metaframe instance from a `String` buffer
    /// ## Errors
    /// Returns `ResponseCodes` which dictate what error has occurred
    pub fn from_buffer(buf: &String) -> Result<QueryMetaframe, ResponseCodes> {
        let mf_parts: Vec<&str> = buf.split(MF_SEPARATOR).collect();
        if mf_parts.len() != 5 {
            return Err(ResponseCodes::InvalidMetaframe);
        }
        if mf_parts[0] != MF_PROTOCOL_TAG || mf_parts[2] != MF_QUERY_TAG {
            return Err(ResponseCodes::InvalidMetaframe);
        }
        let version = match Version::from_str(&mf_parts[1]) {
            None => return Err(ResponseCodes::InvalidMetaframe),
            Some(v) => v,
        };
        if SELF_VERSION.incompatible_with(&version) {
            return Err(ResponseCodes::ProtocolVersionMismatch);
        }
        // The size may have extra code point 0s - remove them
        let cs = mf_parts[4].trim_matches(char::from(0)).trim();
        let content_size = match cs.parse::<usize>() {
            Ok(csize) => csize,
            Err(e) => {
                eprintln!("Errored: {}", e);
                return Err(ResponseCodes::InvalidMetaframe);
            }
        };
        let method = match mf_parts[3] {
            MF_METHOD_GET => QueryMethod::GET,
            MF_METHOD_SET => QueryMethod::SET,
            MF_METHOD_UPDATE => QueryMethod::UPDATE,
            MF_METHOD_DEL => QueryMethod::DEL,
            _ => return Err(ResponseCodes::MethodNotAllowed),
        };

        Ok(QueryMetaframe {
            content_size,
            method,
        })
    }
    /// Get the number of bytes that the server should expect from the dataframe
    pub fn get_content_size(&self) -> usize {
        self.content_size
    }
    /// Get the method to be used
    pub fn get_method(&self) -> &QueryMethod {
        &self.method
    }
}

/// A `Dataframe` is simply treated as a blob which contains bytes in the form
/// of a `String`
#[derive(Debug)]
pub struct Dataframe(String);

impl Dataframe {
    /// Create a new `Dataframe` instance from a `Vec<u8>` buffer
    /// ## Errors
    /// When the `target_size` is not the same as the size of the buffer, this
    /// returns a `CorruptDataframe` response code
    pub fn from_buffer(target_size: usize, buffer: Vec<u8>) -> Result<Dataframe, ResponseCodes> {
        let buffer = String::from_utf8_lossy(&buffer);
        let buffer = buffer.trim();
        if buffer.len() != target_size {
            return Err(ResponseCodes::CorruptDataframe);
        }
        Ok(Dataframe(buffer.to_string()))
    }
    /// Deflatten the dataframe into a `Vec` of actions/identifiers/bytes
    pub fn deflatten(&self) -> Vec<&str> {
        self.0.split_whitespace().collect()
    }
}

#[cfg(test)]
#[test]
fn test_metaframe() {
    let v = Version(0, 1, 0);
    let mut goodframe = String::from("TP/0.1.0/Q/GET/5");
    let res = QueryMetaframe::from_buffer(&goodframe);
    let mf_should_be = QueryMetaframe {
        content_size: 5,
        method: QueryMethod::GET,
    };
    assert_eq!(res.ok().unwrap(), mf_should_be);
}

#[cfg(test)]
#[test]
fn benchmark_metaframe_parsing() {
    let v = Version(0, 1, 0);
    use devtimer::run_benchmark;
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut metaframes: Vec<String> = Vec::with_capacity(50000);
    (0..50000).for_each(|_| {
        let s = rng.gen_range(0, usize::MAX);
        let mut buf = format!("TP/0.1.0/Q/GET/5/{}", s);
        metaframes.push(buf);
    });
    let b = run_benchmark(50000, |n| {
        let _ = QueryMetaframe::from_buffer(&metaframes[n]).ok().unwrap();
    });
    b.print_stats();
}

/// Errors that may occur when parsing a response packet from the server
pub enum ResultError {
    /// A standard response code used by the Terrapipe protocol
    StandardError(ResponseCodes),
    /// Some nonsense response code that may be returned by a buggy or patched server
    UnknownError(String),
}

/// A result metaframe
pub struct ResultMetaframe {
    content_size: usize,
    response: ResponseCodes,
}

impl ResultMetaframe {
    /// Instantiate a new metaframe from a `String` buffer
    /// ## Errors
    /// Returns a `ResultError` in case something happened while parsing the
    /// response packet's metaframe
    pub fn from_buffer(buf: String) -> Result<ResultMetaframe, ResultError> {
        use ResultError::*;
        let mf_parts = buf.trim();
        let mf_parts: Vec<&str> = mf_parts.split("/").collect();
        if mf_parts.len() != 5 {
            return Err(StandardError(ResponseCodes::InvalidMetaframe));
        }

        if mf_parts[0] != MF_PROTOCOL_TAG && mf_parts[2] != MF_RESPONSE_TAG {
            return Err(StandardError(ResponseCodes::InvalidMetaframe));
        }

        let response = match mf_parts[3].parse::<u8>() {
            Ok(r) => match ResponseCodes::from_u8(r) {
                Some(rcode) => rcode,
                None => return Err(UnknownError(mf_parts[3].to_owned())),
            },
            Err(_) => return Err(UnknownError(mf_parts[3].to_owned())),
        };
        // The size may have extra code point 0s - remove them
        let cs = mf_parts[4].trim_matches(char::from(0));
        let content_size = match cs.parse::<usize>() {
            Ok(csize) => csize,
            Err(_) => return Err(StandardError(ResponseCodes::InvalidMetaframe)),
        };

        Ok(ResultMetaframe {
            content_size,
            response,
        })
    }
}
