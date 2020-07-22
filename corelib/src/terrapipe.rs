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

//! This implements the Terrapipe protocol

/// Default query metaline buffer size
pub const DEF_QMETALINE_BUFSIZE: usize = 44;
/// Default query metalayout buffer size
pub const DEF_QMETALAYOUT_BUFSIZE: usize = 576;
/// Default query dataframe buffer size
pub const DEF_QDATAFRAME_BUSIZE: usize = 4096;
pub mod tags {
    //! This module is a collection of tags/strings used for evaluating queries
    //! and responses
    /// `GET` command tag
    pub const TAG_GET: &'static str = "GET";
    /// `SET` command tag
    pub const TAG_SET: &'static str = "SET";
    /// `UPDATE` command tag
    pub const TAG_UPDATE: &'static str = "UPDATE";
    /// `DEL` command tag
    pub const TAG_DEL: &'static str = "DEL";
    /// `HEYA` command tag
    pub const TAG_HEYA: &'static str = "HEYA";
}
pub mod responses {
    //! Empty responses, mostly errors, which are statically compiled
    use lazy_static::lazy_static;
    lazy_static! {
        /// Empty `0`(Okay) response - without any content
        pub static ref RESP_OKAY_EMPTY: Vec<u8> = "0!0!0".as_bytes().to_owned();
        /// `1` Not found response
        pub static ref RESP_NOT_FOUND: Vec<u8> = "1!0!0".as_bytes().to_owned();
        /// `2` Overwrite Error response
        pub static ref RESP_OVERWRITE_ERROR: Vec<u8> = "2!0!0".as_bytes().to_owned();
        /// `3` Invalid Metaframe response
        pub static ref RESP_INVALID_MF: Vec<u8> = "3!0!0".as_bytes().to_owned();
        /// `4` Incomplete frame response
        pub static ref RESP_INCOMPLETE: Vec<u8> = "4!0!0".as_bytes().to_owned();
        /// `5` Internal server error response
        pub static ref RESP_SERVER_ERROR: Vec<u8> = "5!0!0".as_bytes().to_owned();
    }
}

/// Response codes returned by the server
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
            EmptyResponseOkay => RESP_OKAY_EMPTY.to_owned(),
            NotFound => RESP_NOT_FOUND.to_owned(),
            OverwriteError => RESP_OVERWRITE_ERROR.to_owned(),
            InvalidMetaframe => RESP_INVALID_MF.to_owned(),
            Incomplete => RESP_INCOMPLETE.to_owned(),
            ServerError => RESP_SERVER_ERROR.to_owned(),
            OtherError(e) => format!("6!{}!#{}", e.len(), e.len()).as_bytes().to_owned(),
        }
    }
}

/// The query dataframe
#[derive(Debug)]
pub struct QueryDataframe {
    /// The data part
    pub data: Vec<String>,
    /// The query action type
    pub actiontype: ActionType,
}

/// This is enum represents types of responses which can be built from it
pub enum ResponseBuilder {
    SimpleResponse, // TODO: Add pipelined response builder here
}

impl ResponseBuilder {
    /// Create a new simple response
    pub fn new_simple(respcode: RespCodes) -> SimpleResponse {
        SimpleResponse::new(respcode.into())
    }
}

/// Representation of a simple response
pub struct SimpleResponse {
    respcode: u8,
    metalayout_buf: String,
    dataframe_buf: String,
    size_tracker: usize,
}

impl SimpleResponse {
    /// Create a new response with just a response code
    /// The data has to be added by using the `add_data()` member function
    pub fn new(respcode: u8) -> Self {
        SimpleResponse {
            respcode,
            metalayout_buf: String::with_capacity(2),
            dataframe_buf: String::with_capacity(40),
            size_tracker: 0,
        }
    }
    /// Add data to the response
    pub fn add_data(&mut self, data: String) {
        let datstr = data.len().to_string();
        self.metalayout_buf.push_str(&format!("{}#", datstr.len()));
        self.size_tracker += datstr.len() + 1;
        self.dataframe_buf.push_str(&data);
        self.dataframe_buf.push('\n');
    }
    /// Internal function used in the implementation of the `RespBytes` trait
    /// for creating a `Vec<u8>` which can be written to a TCP stream
    fn prepare_response(&self) -> Vec<u8> {
        format!(
            "*!{}!{}!{}\n{}\n{}",
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
    s.add_data("Sayan".to_owned());
    s.add_data("loves".to_owned());
    s.add_data("you".to_owned());
    s.add_data("if".to_owned());
    s.add_data("you".to_owned());
    s.add_data("send".to_owned());
    s.add_data("UTF8".to_owned());
    s.add_data("bytes".to_owned());
    assert_eq!(
        String::from_utf8_lossy(&s.into_response()),
        String::from("0!39!16\n5#5#3#2#3#4#4#5#\nSayan\nloves\nyou\nif\nyou\nsend\nUTF8\nbytes\n")
    );
}

pub enum QueryBuilder {
    SimpleQuery,
    // TODO(@ohsayan): Add pipelined queries here
}
// TODO(@ohsayan): I think we should move the client stuff into a separate repo
// altogether to let users customize the client as they like and avoid licensing
// issues

impl QueryBuilder {
    pub fn new_simple() -> SimpleQuery {
        SimpleQuery::new()
    }
}

pub struct SimpleQuery {
    metaline: String,
    metalayout: String,
    dataframe: String,
    size_tracker: usize,
}

impl SimpleQuery {
    pub fn new() -> Self {
        let mut metaline = String::with_capacity(DEF_QMETALINE_BUFSIZE);
        metaline.push_str("*!");
        SimpleQuery {
            metaline,
            size_tracker: 0,
            metalayout: String::with_capacity(DEF_QMETALAYOUT_BUFSIZE),
            dataframe: String::with_capacity(DEF_QDATAFRAME_BUSIZE),
        }
    }
    pub fn add(&mut self, cmd: &str) {
        let ref mut layout = self.metalayout;
        let ref mut df = self.dataframe;
        let len = cmd.len().to_string();
        self.size_tracker += cmd.len() + 1;
        layout.push_str(&len);
        layout.push('#');
        df.push_str(cmd);
        df.push('\n');
    }
    pub fn from_cmd(&mut self, cmd: String) {
        cmd.split_whitespace().for_each(|val| self.add(val));
    }
    pub fn prepare_response(&self) -> (usize, Vec<u8>) {
        let resp = format!(
            "{}{}!{}\n{}\n{}",
            self.metaline,
            self.size_tracker,
            self.metalayout.len(),
            self.metalayout,
            self.dataframe
        )
        .as_bytes()
        .to_owned();
        (resp.len(), resp)
    }
}
