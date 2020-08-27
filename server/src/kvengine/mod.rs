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

//! # The Key/Value Engine
//! This is TerrabaseDB's K/V engine. It contains utilities to interface with
//! TDB's K/V store

pub mod del;
pub mod exists;
pub mod get;
pub mod mget;
pub mod mset;
pub mod set;
pub mod update;
pub mod heya {
    //! Respond to `HEYA` queries
    use crate::protocol;
    use libtdb::TResult;
    use protocol::{responses, Connection};
    /// Returns a `HEY!` `Response`
    pub async fn heya(con: &mut Connection) -> TResult<()> {
        con.write_response(responses::HEYA.to_owned()).await
    }
}
