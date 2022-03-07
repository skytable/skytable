/*
 * Created on Mon Feb 21 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

/*
 * For our authn/authz, we have two important keys:
 * - The origin key: This is the key saved in the configuration file that can also be
 * used as the "recovery key" in the event the "root key" is lost. To claim the root
 * account, one needs this key. This is a variable width key with a maximum size of
 * 64
 * - The root key: This is the superuser key that can be used to create/deny other
 * accounts. On claiming the root account, this key is issued
 *
 * When the root account is claimed, it can be used to create "authmap users". authmap
 * users have access to everything but the ability to create/revoke other users
*/

mod keys;
pub mod provider;
pub use provider::{AuthError, AuthProvider, AuthResult, Authmap};
pub mod errors;

#[cfg(test)]
mod tests;

use crate::dbnet::connection::prelude::*;

const AUTH_CLAIM: &[u8] = b"claim";
const AUTH_LOGIN: &[u8] = b"login";
const AUTH_ADDUSER: &[u8] = b"adduser";
const AUTH_DELUSER: &[u8] = b"deluser";

action! {
    /// Handle auth. Should have passed the `auth` token
    fn auth(
        _handle: &Corestore,
        _con: &mut T,
        _auth: &mut AuthProviderHandle<'_, T, Strm>,
        _iter: ActionIter<'_>
    ) {
        todo!()
    }
    /// Handle a login operation only. The **`login` token is expected to be present**
    fn auth_login_only(
        _handle: &Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, T, Strm>,
        iter: ActionIter<'_>
    ) {
        let mut iter = iter;
        match iter.next() {
            Some(v) => match v {
                AUTH_LOGIN => {
                    // sweet, where's our username and password
                    let (username, password) = (iter.next(), iter.next());
                    match (username, password) {
                        (Some(username), Some(password)) => {
                            auth.provider_mut().login(username, password)?;
                            auth.swap_executor_to_authenticated();
                        },
                        _ => return util::err(groups::ACTION_ERR),
                    }
                    con.write_response(groups::OKAY).await?;
                    Ok(())
                }
                _ => util::err(errors::AUTH_CODE_PERMS),
            }
            None => util::err(groups::ACTION_ERR),
        }
    }
}
