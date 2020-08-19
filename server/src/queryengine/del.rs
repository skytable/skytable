/*
 * Created on Wed Aug 19 2020
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

//! # `DEL` queries
//! This module provides functions to work with `DEL` queries

use crate::coredb::CoreDB;
use corelib::builders::response::*;
use corelib::de::DataGroup;
use corelib::terrapipe::RespCodes;

/// Run a `DEL` query
pub fn del(handle: &CoreDB, act: Vec<String>) -> Response {
    if act.len() < 2 {
        return RespCodes::ActionError.into_response();
    }
    let mut resp = SResp::new();
    let mut respgroup = RespGroup::new();
    act.into_iter()
        .skip(1)
        .for_each(|key| match handle.del(&key) {
            Ok(_) => respgroup.add_item(RespCodes::Okay),
            Err(e) => respgroup.add_item(e),
        });
    resp.add_group(respgroup);
    resp.into_response()
}
