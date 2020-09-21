/*
 * Created on Mon Sep 21 2020
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

//! # Strong Actions
//! Strong actions are like "do all" or "fail all" actions, built specifically for
//! multiple keys. So let's say you used `SSET` instead of `MSET` for setting keys:
//! what'd be the difference?
//! In this case, if all the keys are non-existing, which is a requirement for `MSET`,
//! only then would the keys be set. That is, only if all the keys can be set, will the action
//! run and return code `0` - otherwise the action won't do anything and return an overwrite error.  
//! Do note that this isn't the same as the gurantees provided by ACID transactions

use crate::coredb::{self, CoreDB};
use crate::protocol::{responses, ActionGroup, Connection};
use crate::resp::GroupBegin;
use libtdb::TResult;
use std::collections::hash_map::Entry;

/// Run an `SSET` query
pub async fn sset(handle: &CoreDB, con: &mut Connection, act: ActionGroup) -> TResult<()> {
    todo!()
}
