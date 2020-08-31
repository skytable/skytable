/*
 * Created on Mon Aug 31 2020
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

//! #`JGET` queries
//! Functions for handling `JGET` queries

use crate::coredb::CoreDB;
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resp::{BytesWrapper, GroupBegin};
use bytes::Bytes;
use libtdb::TResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A key/value pair
/// When we write JSON to the stream in `JGET`, it looks something like:
/// ```json
/// {
///     "keythatexists" : "value",
///     "nilkey": null,
/// }
/// ```
#[derive(Serialize, Deserialize)]
pub struct KVPair(HashMap<String, Option<String>>);

impl KVPair {
    pub fn with_capacity(size: usize) -> Self {
        KVPair(HashMap::with_capacity(size))
    }
}

/// Run a `JGET` query
/// This returns a JSON key/value pair of keys and values
/// We need to write something like
/// ```json
/// &1\n
/// $15\n
/// {"key":"value"}\n
/// ```
///
pub async fn jget(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    let howmany = act.howmany();
    if howmany != 1 {
        return con
            .write_response(responses::fresp::R_ACTION_ERR.to_owned())
            .await;
    }
    todo!()
}
