/*
 * Created on Mon Aug 03 2020
 *
 * This file is a part of TerrabaseDB
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
use crate::dbnet::Con;
use crate::protocol::ActionGroup;
use crate::protocol::{responses};
use crate::{admin, kvengine};
use libtdb::TResult;
mod tags {
    //! This module is a collection of tags/strings used for evaluating queries
    //! and responses
    /// `GET` action tag
    pub const TAG_GET: &'static str = "GET";
    /// `SET` action tag
    pub const TAG_SET: &'static str = "SET";
    /// `UPDATE` action tag
    pub const TAG_UPDATE: &'static str = "UPDATE";
    /// `DEL` action tag
    pub const TAG_DEL: &'static str = "DEL";
    /// `HEYA` action tag
    pub const TAG_HEYA: &'static str = "HEYA";
    /// `EXISTS` action tag
    pub const TAG_EXISTS: &'static str = "EXISTS";
    /// `MSET` action tag
    pub const TAG_MSET: &'static str = "MSET";
    /// `MGET` action tag
    pub const TAG_MGET: &'static str = "MGET";
    /// `MUPDATE` action tag
    pub const TAG_MUPDATE: &'static str = "MUPDATE";
    /// `SSET` action tag
    pub const TAG_SSET: &'static str = "SSET";
    /// `SDEL` action tag
    pub const TAG_SDEL: &'static str = "SDEL";
    /// `SUPDATE` action tag
    pub const TAG_SUPDATE: &'static str = "SUPDATE";
    /// `DBSIZE` action tag
    pub const TAG_DBSIZE: &'static str = "DBSIZE";
    /// `FLUSHDB` action tag
    pub const TAG_FLUSHDB: &'static str = "FLUSHDB";
    /// `USET` action tag
    pub const TAG_USET: &'static str = "USET";
    /// `KEYLEN` action tag
    pub const TAG_KEYLEN: &'static str = "KEYLEN";
    /// `MKSNAP` action tag
    pub const TAG_MKSNAP: &'static str = "MKSNAP";
}

/// Execute a simple(*) query
pub async fn execute_simple(db: &CoreDB, con: &mut Con<'_>, buf: ActionGroup) -> TResult<()> {
    let first = match buf.get_first() {
        None => {
            return con
                .write_response(responses::fresp::R_PACKET_ERR.to_owned())
                .await;
        }
        Some(f) => f.to_uppercase(),
    };
    match first.as_str() {
        tags::TAG_DEL => kvengine::del::del(db, con, buf).await?,
        tags::TAG_GET => kvengine::get::get(db, con, buf).await?,
        tags::TAG_HEYA => kvengine::heya::heya(db, con, buf).await?,
        tags::TAG_EXISTS => kvengine::exists::exists(db, con, buf).await?,
        tags::TAG_SET => kvengine::set::set(db, con, buf).await?,
        tags::TAG_MGET => kvengine::mget::mget(db, con, buf).await?,
        tags::TAG_MSET => kvengine::mset::mset(db, con, buf).await?,
        tags::TAG_UPDATE => kvengine::update::update(db, con, buf).await?,
        tags::TAG_MUPDATE => kvengine::mupdate::mupdate(db, con, buf).await?,
        tags::TAG_SSET => kvengine::strong::sset(db, con, buf).await?,
        tags::TAG_SDEL => kvengine::strong::sdel(db, con, buf).await?,
        tags::TAG_SUPDATE => kvengine::strong::supdate(db, con, buf).await?,
        tags::TAG_DBSIZE => kvengine::dbsize::dbsize(db, con, buf).await?,
        tags::TAG_FLUSHDB => kvengine::flushdb::flushdb(db, con, buf).await?,
        tags::TAG_USET => kvengine::uset::uset(db, con, buf).await?,
        tags::TAG_KEYLEN => kvengine::keylen::keylen(db, con, buf).await?,
        tags::TAG_MKSNAP => admin::mksnap::mksnap(db, con, buf).await?,
        _ => {
            con.write_response(responses::fresp::R_UNKNOWN_ACTION.to_owned())
                .await?
        }
    }
    Ok(())
}
