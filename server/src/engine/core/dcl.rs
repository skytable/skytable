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
    ql::dcl::{SysctlCommand, UserDecl, UserDel},
};

const KEY_PASSWORD: &str = "password";

pub fn exec<G: GlobalInstanceLike>(
    g: G,
    current_user: &ClientLocalState,
    cmd: SysctlCommand,
) -> QueryResult<()> {
    if cmd.needs_root() & !current_user.is_root() {
        return Err(QueryError::SysPermissionDenied);
    }
    match cmd {
        SysctlCommand::CreateUser(new) => create_user(&g, new),
        SysctlCommand::DropUser(drop) => drop_user(&g, current_user, drop),
        SysctlCommand::AlterUser(usermod) => alter_user(&g, current_user, usermod),
        SysctlCommand::ReportStatus => {
            if g.health().status_okay() {
                Ok(())
            } else {
                Err(QueryError::SysServerError)
            }
        }
    }
}

fn alter_user(
    global: &impl GlobalInstanceLike,
    cstate: &ClientLocalState,
    user: UserDecl,
) -> QueryResult<()> {
    if cstate.is_root() {
        // the root password can only be changed by shutting down the server
        return Err(QueryError::SysAuthError);
    }
    let (username, password) = get_user_data(user)?;
    global
        .state()
        .namespace()
        .sys_db()
        .alter_user(global, &username, &password)
}

fn create_user(global: &impl GlobalInstanceLike, user: UserDecl) -> QueryResult<()> {
    let (username, password) = get_user_data(user)?;
    global
        .state()
        .namespace()
        .sys_db()
        .create_user(global, username.into_boxed_str(), &password)
}

fn get_user_data<'a>(mut user: UserDecl<'a>) -> Result<(String, String), QueryError> {
    let password = match user.options_mut().remove(KEY_PASSWORD) {
        Some(DictEntryGeneric::Data(d))
            if d.kind() == TagClass::Str && user.options().is_empty() =>
        unsafe { d.into_str().unwrap_unchecked() },
        None | Some(_) => {
            // invalid properties
            return Err(QueryError::QExecDdlInvalidProperties);
        }
    };
    let username = user.username().to_owned();
    Ok((username, password))
}

fn drop_user(
    global: &impl GlobalInstanceLike,
    cstate: &ClientLocalState,
    user_del: UserDel<'_>,
) -> QueryResult<()> {
    if cstate.username() == user_del.username() {
        // you can't delete yourself!
        return Err(QueryError::SysAuthError);
    }
    global
        .state()
        .namespace()
        .sys_db()
        .drop_user(global, user_del.username())
}
