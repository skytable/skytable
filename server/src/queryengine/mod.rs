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
mod del;
mod exists;
mod get;
pub mod queryutil;
mod set;
mod update;
use std::mem;
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
pub fn execute_simple(db: &CoreDB, mut buf: Vec<DataGroup>) -> Response {
    if buf.len() != 1 {
        return responses::ARG_ERR.to_owned();
    }
    // TODO(@ohsayan): See how efficient this actually is
    let dg = mem::take(&mut buf[0].0); // get the datagroup, emptying the dg in the buf
    if dg.len() < 1 {
        return responses::ARG_ERR.to_owned();
    }
    match unsafe { dg.get_unchecked(0).to_uppercase().as_str() } {
        tags::TAG_GET => get::get(db, dg),
        tags::TAG_SET => set::set(db, dg),
        tags::TAG_EXISTS => exists::exists(db, dg),
        tags::TAG_DEL => del::del(db, dg),
        tags::TAG_UPDATE => update::update(db, dg),
        tags::TAG_HEYA => "HEYA".into_response(),
        _ => responses::ARG_ERR.to_owned(),
    }
}
