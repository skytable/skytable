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
    Okay(Option<String>),
    NotFound,
    OverwriteError,
    InvalidMetaframe,
    Incomplete,
    ServerError,
    OtherError(Option<String>),
}

#[derive(Debug, PartialEq)]
pub enum ActionType {
    Simple,
    Pipeline,
}

pub trait Response {
    fn into_response(&self) -> Vec<u8>;
}
