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
#![allow(unused)]

//! This is an implementation of [Terrabase/RFC1](https://github.com/terrabasedb/rfcs/pull/1)

use std::fmt;
use std::mem;

pub const MF_PROTOCOL_TAG: &'static str = "TP";
pub const MF_QUERY_TAG: &'static str = "Q";
pub const MF_RESPONSE_TAG: &'static str = "R";
pub const MF_SEPARATOR: &'static str = "/";
pub const MF_METHOD_GET: &'static str = "GET";
pub const MF_METHOD_SET: &'static str = "SET";
pub const MF_METHOD_UPDATE: &'static str = "UPDATE";
pub const MF_METHOD_DEL: &'static str = "DEL";
pub const DEF_Q_META_BUFSIZE: usize = 46;
pub const DEF_R_META_BUFSIZE: usize = 40;

macro_rules! response_packet {
    ($version:expr, $respcode:expr, $data:expr) => {{
        let res = format!(
            "TP/{}.{}.{}/R/{}/{}\n\n{}",
            $version.0,
            $version.1,
            $version.2,
            $respcode,
            $data.len(),
            $data,
        );
        res.as_bytes().to_vec()
    }};
}

pub struct Version(pub u8, pub u8, pub u8);

impl Version {
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
    pub fn incompatible_with(&self, other: &Version) -> bool {
        if self.0 == other.0 {
            false
        } else {
            true
        }
    }
}

pub enum ResponseCodes {
    Okay,                    // Code: 0
    NotFound,                // Code: 1
    OverwriteError,          // Code: 2
    MethodNotAllowed,        // Code: 3
    InternalServerError,     // Code: 4
    InvalidMetaframe,        // Code: 5
    CorruptDataframe,        // Code: 6
    ProtocolVersionMismatch, // Code: 7
    CorruptPacket,           // Code: 8
}

impl ResponseCodes {
    pub fn from_u8(code: u8) -> Option<Self> {
        use ResponseCodes::*;
        let c = match code {
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
        Some(c)
    }
}

pub trait ResponseBytes {
    fn response_bytes(&self, v: &Version) -> Vec<u8>;
}

impl ResponseBytes for ResponseCodes {
    fn response_bytes(&self, v: &Version) -> Vec<u8> {
        use ResponseCodes::*;
        match self {
            Okay => {
                // We will never need an implementation for Okay
                unimplemented!()
            }
            NotFound => response_packet!(v, 1, ""),
            OverwriteError => response_packet!(v, 2, ""),
            MethodNotAllowed => response_packet!(v, 3, ""),
            InternalServerError => response_packet!(v, 4, ""),
            InvalidMetaframe => response_packet!(v, 5, ""),
            CorruptDataframe => response_packet!(v, 6, ""),
            ProtocolVersionMismatch => response_packet!(v, 7, ""),
            CorruptPacket => response_packet!(v, 8, ""),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum QueryMethod {
    GET,
    SET,
    UPDATE,
    DEL,
}

#[derive(Debug, PartialEq)]
pub struct QueryMetaframe {
    content_size: usize,
    method: QueryMethod,
}

impl QueryMetaframe {
    pub fn from_buffer(
        self_version: &Version,
        buf: &String
    ) -> Result<QueryMetaframe, impl ResponseBytes> {
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
        if self_version.incompatible_with(&version) {
            return Err(ResponseCodes::ProtocolVersionMismatch);
        }
        // The size may have extra code point 0s - remove them
        let cs = mf_parts[4].trim_matches(char::from(0)).trim();
        let content_size = match cs.parse::<usize>() {
            Ok(csize) => csize,
            Err(e) => {
                eprintln!("Errored: {}", e);
                return Err(ResponseCodes::InvalidMetaframe)
            },
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
    pub fn get_content_size(&self) -> usize {
        self.content_size
    }
    pub fn get_method(&self) -> &QueryMethod {
        &self.method
    }
}

pub struct Dataframe(String);

impl Dataframe {
    pub fn from_buffer(
        target_size: usize,
        buffer: Vec<u8>,
    ) -> Result<Dataframe, impl ResponseBytes> {
        let buffer = String::from_utf8_lossy(&buffer);
        let buffer = buffer.trim();
        if buffer.len() != target_size {
            return Err(ResponseCodes::CorruptDataframe);
        }
        Ok(Dataframe(buffer.to_string()))
    }
    pub fn deflatten(&self) -> Vec<&str> {
        self.0.split_whitespace().collect()
    }
}

#[cfg(test)]
#[test]
fn test_metaframe() {
    use std::io::Write;
    let v = Version(0, 1, 0);
    let mut goodframe = String::from("TP/0.1.0/Q/GET/5");
    // let mut goodframe = [0u8; DEF_Q_META_BUFSIZE];
    // write!(&mut goodframe[..], "TP/0.1.1/Q/GET/5").unwrap();
    let res = QueryMetaframe::from_buffer(&v, &goodframe);
    let mf_should_be = QueryMetaframe {
        content_size: 5,
        method: QueryMethod::GET,
    };
    assert_eq!(res.ok().unwrap(), mf_should_be);
}

#[cfg(test)]
#[test]
fn benchmark_metaframe_parsing() {
    use std::io::Write;
    let v = Version(0, 1, 0);
    use devtimer::run_benchmark;
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut metaframes: Vec<String> = Vec::with_capacity(50000);
    (0..50000).for_each(|_| {
        let s = rng.gen_range(0, usize::MAX);
        let mut buf = format!("TP/0.1.0/Q/GET/5/{}", s);
        // let mut buf = [0u8; DEF_Q_META_BUFSIZE];
        // write!(&mut buf[..], "TP/0.1.1/Q/GET/{}", s).unwrap();
        metaframes.push(buf);
    });
    let b = run_benchmark(50000, |n| {
        let _ = QueryMetaframe::from_buffer(&v, &metaframes[n]).ok().unwrap();
    });
    b.print_stats();
}

pub enum ResultError {
    StandardError(ResponseCodes),
    UnknownError(String),
}

pub struct ResultMetaframe {
    content_size: usize,
    response: ResponseCodes,
}

impl ResultMetaframe {
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
