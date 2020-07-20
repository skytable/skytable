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

pub const DEF_QMETALINE_BUFSIZE: usize = 44;
pub const DEF_QMETALAYOUT_BUFSIZE: usize = 1024;
pub const DEF_QDATAFRAME_BUSIZE: usize = 4096;

pub mod responses {
    use lazy_static::lazy_static;
    lazy_static! {
        pub static ref RESP_OKAY_EMPTY: Vec<u8> = "0!0!0#".as_bytes().to_owned();
        pub static ref RESP_NOT_FOUND: Vec<u8> = "1!0!0#".as_bytes().to_owned();
        pub static ref RESP_OVERWRITE_ERROR: Vec<u8> = "2!0!0#".as_bytes().to_owned();
        pub static ref RESP_INVALID_MF: Vec<u8> = "3!0!0#".as_bytes().to_owned();
        pub static ref RESP_INCOMPLETE: Vec<u8> = "4!0!0#".as_bytes().to_owned();
        pub static ref RESP_SERVER_ERROR: Vec<u8> = "5!0!0#".as_bytes().to_owned();
    }
}

#[derive(Debug, PartialEq)]
pub enum RespCodes {
    /// `0`: Okay (Empty Response) - use the `ResponseBuilder` for building
    /// responses that contain data
    EmptyResponseOkay,
    /// `1`: Not Found
    NotFound,
    /// `2`: Overwrite Error
    OverwriteError,
    /// `3`: Invalid Metaframe
    InvalidMetaframe,
    /// `4`: Incomplete
    Incomplete,
    /// `5`: Server Error
    ServerError,
    /// `6`: Some other error - the wrapped `String` will be returned in the response body
    OtherError(String),
}

impl From<RespCodes> for u8 {
    fn from(rcode: RespCodes) -> u8 {
        use RespCodes::*;
        match rcode {
            EmptyResponseOkay => 0,
            NotFound => 1,
            OverwriteError => 2,
            InvalidMetaframe => 3,
            Incomplete => 4,
            ServerError => 5,
            OtherError(_) => 6,
        }
    }
}

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
            EmptyResponseOkay => RESP_NOT_FOUND.to_owned(),
            NotFound => RESP_NOT_FOUND.to_owned(),
            OverwriteError => RESP_OVERWRITE_ERROR.to_owned(),
            InvalidMetaframe => RESP_INVALID_MF.to_owned(),
            Incomplete => RESP_INCOMPLETE.to_owned(),
            ServerError => RESP_SERVER_ERROR.to_owned(),
            OtherError(e) => format!("6!{}!#{}", e.len(), e.len()).as_bytes().to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct QueryDataframe {
    pub data: Vec<String>,
    pub actiontype: ActionType,
}

pub enum ResponseBuilder {
    SimpleResponse, // TODO: Add pipelined response builder here
}

impl ResponseBuilder {
    pub fn new_simple(respcode: RespCodes) -> SimpleResponse {
        SimpleResponse::new(respcode.into())
    }
}

pub struct SimpleResponse {
    respcode: u8,
    metalayout_buf: String,
    dataframe_buf: String,
    size_tracker: usize,
}

impl SimpleResponse {
    pub fn new(respcode: u8) -> Self {
        SimpleResponse {
            respcode,
            metalayout_buf: String::with_capacity(2),
            dataframe_buf: String::with_capacity(40),
            size_tracker: 0,
        }
    }
    pub fn add_data(&mut self, data: &str) {
        self.metalayout_buf.push_str(&format!("{}#", data.len()));
        self.size_tracker += data.len() + 1;
        self.dataframe_buf.push_str(data);
        self.dataframe_buf.push('\n');
    }
    pub fn prepare_response(&self) -> Vec<u8> {
        format!(
            "{}!{}!{}\n{}\n{}",
            self.respcode,
            self.size_tracker,
            self.metalayout_buf.len(),
            self.metalayout_buf,
            self.dataframe_buf
        )
        .as_bytes()
        .to_owned()
    }
}

impl RespBytes for SimpleResponse {
    fn into_response(&self) -> Vec<u8> {
        self.prepare_response()
    }
}

#[cfg(test)]
#[test]
fn test_simple_response() {
    let mut s = ResponseBuilder::new_simple(RespCodes::EmptyResponseOkay);
    s.add_data("Sayan");
    s.add_data("loves");
    s.add_data("you");
    s.add_data("if");
    s.add_data("you");
    s.add_data("send");
    s.add_data("UTF8");
    s.add_data("bytes");
    assert_eq!(
        String::from_utf8_lossy(&s.into_response()),
        String::from("0!39!16\n5#5#3#2#3#4#4#5#\nSayan\nloves\nyou\nif\nyou\nsend\nUTF8\nbytes\n")
    );
}
