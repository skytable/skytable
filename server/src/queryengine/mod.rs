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
use crate::kvengine;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::responses;
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
    let mut responses: Vec<Response> = buf
        .into_iter()
        .map(|dg| match dg.get(0) {
            Some(act) => match act.to_uppercase().as_str() {
                tags::TAG_GET => kvengine::get::get(&db, dg),
                tags::TAG_SET => kvengine::set::set(&db, dg),
                tags::TAG_UPDATE => kvengine::update::update(&db, dg),
                tags::TAG_DEL => kvengine::del::del(&db, dg),
                tags::TAG_EXISTS => kvengine::exists::exists(&db, dg),
                tags::TAG_HEYA => kvengine::heya::heya(),
                _ => responses::UNKNOWN_COMMAND.to_owned(),
            },
            None => responses::PACKET_ERROR.to_owned(),
        })
        .collect();
    responses.pop().unwrap()
}
