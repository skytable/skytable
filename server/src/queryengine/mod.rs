/*
 * Created on Mon Aug 03 2020
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

//! # The Query Engine

use crate::coredb::CoreDB;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::{responses, RespCodes};
mod get;
pub mod queryutil;
mod set;
mod update;
mod tags {
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
    /// `EXISTS` command tag
    pub const TAG_EXISTS: &'static str = "EXISTS";
}

/// Execute a simple(*) query
pub fn execute_simple(db: &CoreDB, buf: Vec<DataGroup>) -> Response {
    let mut buf = (*buf[0].0).into_iter();
    while let Some(token) = buf.next() {
        match token.to_uppercase().as_str() {
            tags::TAG_GET => {
                // This is a GET query
                if let Some(key) = buf.next() {
                    if buf.next().is_none() {
                        let res = match db.get(&key.to_string()) {
                            Ok(v) => v,
                            Err(e) => return e.into_response(),
                        };
                        let mut resp = SResp::new();
                        resp.add_group(BytesWrapper(res));
                        return resp.into_response();
                    }
                }
            }
            tags::TAG_SET => {
                // This is a SET query
                if let Some(key) = buf.next() {
                    if let Some(value) = buf.next() {
                        if buf.next().is_none() {
                            match db.set(&key.to_string(), &value.to_string()) {
                                Ok(_) => {
                                    #[cfg(Debug)]
                                    {
                                        db.print_debug_table();
                                    }
                                    return responses::OKAY.to_owned();
                                }
                                Err(e) => return e.into_response(),
                            }
                        }
                    }
                }
            }
            tags::TAG_UPDATE => {
                // This is an UPDATE query
                if let Some(key) = buf.next() {
                    if let Some(value) = buf.next() {
                        if buf.next().is_none() {
                            match db.update(&key.to_string(), &value.to_string()) {
                                Ok(_) => {
                                    #[cfg(Debug)]
                                    {
                                        db.print_debug_table();
                                    }
                                    return responses::OKAY.to_owned();
                                }
                                Err(e) => return e.into_response(),
                            }
                        }
                    }
                }
            }
            tags::TAG_DEL => {
                // This is a DEL query
                if let Some(key) = buf.next() {
                    if buf.next().is_none() {
                        match db.del(&key.to_string()) {
                            Ok(_) => {
                                #[cfg(Debug)]
                                {
                                    db.print_debug_table();
                                }

                                return responses::OKAY.to_owned();
                            }
                            Err(e) => return e.into_response(),
                        }
                    }
                }
            }
            tags::TAG_EXISTS => {
                // This is an `EXISTS` query
                if let Some(key) = buf.next() {
                    if buf.next().is_none() {
                        let ex = db.exists(&key) as u8;
                        let mut resp = SResp::new();
                        resp.add_group(ex);
                        return resp.into_response();
                    }
                }
            }
            tags::TAG_HEYA => {
                // This is a `HEYA` query
                if buf.next().is_none() {
                    return "HEY!".into_response();
                }
            }
            _ => return RespCodes::OtherError(Some("Unknown command".to_owned())).into_response(),
        }
    }
    responses::ARG_ERR.to_owned()
}
