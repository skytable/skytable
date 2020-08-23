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
use crate::protocol::ActionGroup;
use crate::protocol::Connection;
use libtdb::TResult;
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
pub async fn execute_simple(db: &CoreDB, con: &mut Connection, buf: ActionGroup) -> TResult<()> {
    todo!()
}
