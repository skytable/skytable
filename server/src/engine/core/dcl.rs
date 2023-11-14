/*
 * Created on Fri Nov 10 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use crate::engine::{
    data::{tag::TagClass, DictEntryGeneric},
    error::{QueryError, QueryResult},
    fractal::GlobalInstanceLike,
    net::protocol::ClientLocalState,
    ql::dcl::{UserAdd, UserDel},
};

const KEY_PASSWORD: &str = "password";

pub fn create_user(global: &impl GlobalInstanceLike, mut user_add: UserAdd<'_>) -> QueryResult<()> {
    let username = user_add.username().to_owned();
    let password = match user_add.options_mut().remove(KEY_PASSWORD) {
        Some(DictEntryGeneric::Data(d))
            if d.kind() == TagClass::Str && user_add.options().is_empty() =>
        unsafe { d.into_str().unwrap_unchecked() },
        None | Some(_) => {
            // invalid properties
            return Err(QueryError::QExecDdlInvalidProperties);
        }
    };
    global.sys_store().create_new_user(username, password)
}

pub fn drop_user(
    global: &impl GlobalInstanceLike,
    cstate: &ClientLocalState,
    user_del: UserDel<'_>,
) -> QueryResult<()> {
    if cstate.username() == user_del.username() {
        // you can't delete yourself!
        return Err(QueryError::SysAuthError);
    }
    global.sys_store().drop_user(user_del.username())
}
