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

/*
 * For our authn/authz, we have two important keys:
 * - The origin key: This is the key saved in the configuration file that can also be
 * used as the "recovery key" in the event the "root key" is lost. To claim the root
 * account, one needs this key. This is a fixed width key with 40 characters
 * - The root key: This is the superuser key that can be used to create/deny other
 * accounts. On claiming the root account, this key is issued
 *
 * When the root account is claimed, it can be used to create "standard users". standard
 * users have access to everything but the ability to create/revoke other users
*/

mod keys;
pub mod provider;
pub use provider::{AuthProvider, Authmap};

#[cfg(test)]
mod tests;

use crate::dbnet::prelude::*;

const AUTH_CLAIM: &[u8] = b"claim";
const AUTH_LOGIN: &[u8] = b"login";
const AUTH_LOGOUT: &[u8] = b"logout";
const AUTH_ADDUSER: &[u8] = b"adduser";
const AUTH_DELUSER: &[u8] = b"deluser";
const AUTH_RESTORE: &[u8] = b"restore";
const AUTH_LISTUSER: &[u8] = b"listuser";
const AUTH_WHOAMI: &[u8] = b"whoami";

action! {
    /// Handle auth. Should have passed the `auth` token
    fn auth(
        con: &mut Connection<C, P>,
        auth: &mut AuthProviderHandle,
        iter: ActionIter<'_>
    ) {
        let mut iter = iter;
        match iter.next_lowercase().unwrap_or_aerr::<P>()?.as_ref() {
            AUTH_LOGIN => self::_auth_login(con, auth, &mut iter).await,
            AUTH_CLAIM => self::_auth_claim(con, auth, &mut iter).await,
            AUTH_ADDUSER => {
                ensure_boolean_or_aerr::<P>(iter.len() == 1)?; // just the username
                let username = unsafe { iter.next_unchecked() };
                let key = auth.provider_mut().claim_user::<P>(username)?;
                con.write_string(&key).await?;
                Ok(())
            }
            AUTH_LOGOUT => {
                ensure_boolean_or_aerr::<P>(iter.is_empty())?; // nothing else
                auth.provider_mut().logout::<P>()?;
                auth.set_unauth();
                con._write_raw(P::RCODE_OKAY).await?;
                Ok(())
            }
            AUTH_DELUSER => {
                ensure_boolean_or_aerr::<P>(iter.len() == 1)?; // just the username
                auth.provider_mut().delete_user::<P>(unsafe { iter.next_unchecked() })?;
                con._write_raw(P::RCODE_OKAY).await?;
                Ok(())
            }
            AUTH_RESTORE => self::auth_restore(con, auth, &mut iter).await,
            AUTH_LISTUSER => self::auth_listuser(con, auth, &mut iter).await,
            AUTH_WHOAMI => self::auth_whoami(con, auth, &mut iter).await,
            _ => util::err(P::RCODE_UNKNOWN_ACTION),
        }
    }
    fn auth_whoami(con: &mut Connection<C, P>, auth: &mut AuthProviderHandle, iter: &mut ActionIter<'_>) {
        ensure_boolean_or_aerr::<P>(ActionIter::is_empty(iter))?;
        con.write_string(&auth.provider().whoami::<P>()?).await?;
        Ok(())
    }
    fn auth_listuser(con: &mut Connection<C, P>, auth: &mut AuthProviderHandle, iter: &mut ActionIter<'_>) {
        ensure_boolean_or_aerr::<P>(ActionIter::is_empty(iter))?;
        let usernames = auth.provider().collect_usernames::<P>()?;
        con.write_typed_non_null_array_header(usernames.len(), b'+').await?;
        for username in usernames {
            con.write_typed_non_null_array_element(username.as_bytes()).await?;
        }
        Ok(())
    }
    fn auth_restore(con: &mut Connection<C, P>, auth: &mut AuthProviderHandle, iter: &mut ActionIter<'_>) {
        let newkey = match iter.len() {
            1 => {
                // so this fella thinks they're root
                auth.provider().regenerate::<P>(
                    unsafe { iter.next_unchecked() }
                )?
            }
            2 => {
                // so this fella is giving us the origin key
                let origin = unsafe { iter.next_unchecked() };
                let id = unsafe { iter.next_unchecked() };
                auth.provider().regenerate_using_origin::<P>(origin, id)?
            }
            _ => return util::err(P::RCODE_ACTION_ERR),
        };
        con.write_string(&newkey).await?;
        Ok(())
    }
    fn _auth_claim(con: &mut Connection<C, P>, auth: &mut AuthProviderHandle, iter: &mut ActionIter<'_>) {
        ensure_boolean_or_aerr::<P>(iter.len() == 1)?; // just the origin key
        let origin_key = unsafe { iter.next_unchecked() };
        let key = auth.provider_mut().claim_root::<P>(origin_key)?;
        auth.set_auth();
        con.write_string(&key).await?;
        Ok(())
    }
    /// Handle a login operation only. The **`login` token is expected to be present**
    fn auth_login_only(
        con: &mut Connection<C, P>,
        auth: &mut AuthProviderHandle,
        iter: ActionIter<'_>
    ) {
        let mut iter = iter;
        match iter.next_lowercase().unwrap_or_aerr::<P>()?.as_ref() {
            AUTH_LOGIN => self::_auth_login(con, auth, &mut iter).await,
            AUTH_CLAIM => self::_auth_claim(con, auth, &mut iter).await,
            AUTH_RESTORE => self::auth_restore(con, auth, &mut iter).await,
            AUTH_WHOAMI => self::auth_whoami(con, auth, &mut iter).await,
            _ => util::err(P::AUTH_CODE_PERMS),
        }
    }
    fn _auth_login(con: &mut Connection<C, P>, auth: &mut AuthProviderHandle, iter: &mut ActionIter<'_>) {
        // sweet, where's our username and password
        ensure_boolean_or_aerr::<P>(iter.len() == 2)?; // just the uname and pass
        let (username, password) = unsafe { (iter.next_unchecked(), iter.next_unchecked()) };
        auth.provider_mut().login::<P>(username, password)?;
        auth.set_auth();
        con._write_raw(P::RCODE_OKAY).await?;
        Ok(())
    }
}
