/*
 * Created on Fri Aug 14 2020
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

use crate::coredb::CoreDB;
use corelib::builders::response::*;
use corelib::de::Action;
use corelib::terrapipe::RespCodes;

pub fn get(handle: &CoreDB, act: Action) -> Vec<u8> {
    if act.len() < 2 {
        return RespCodes::ArgumentError.into_response();
    }
    let mut resp = SResp::new();
    let mut respgroup = RespGroup::new();
    act.into_iter()
        .skip(1)
        .for_each(|key| match handle.get(&key) {
            Ok(byts) => respgroup.add_item(BytesWrapper(byts)),
            Err(e) => respgroup.add_item(e),
        });
    resp.add_group(respgroup);
    resp.into_response()
}
