/*
 * Created on Sun Mar 06 2022
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

use super::{errors, keys, AuthError};
use crate::corestore::array::Array;
use crate::corestore::htable::Coremap;
use std::sync::Arc;

// constants
/// Size of an authn key in bytes
pub const AUTHKEY_SIZE: usize = 40;
/// Size of an authn ID in bytes
pub const AUTHID_SIZE: usize = 40;

#[cfg(debug_assertions)]
pub mod testsuite_data {
    //! Temporary users created by the testsuite in debug mode
    pub const TESTSUITE_ROOT_USER: &str = "root";
    pub const TESTSUITE_TEST_USER: &str = "testuser";
    #[cfg(test)]
    pub const TESTSUITE_ROOT_TOKEN: &str = "XUOdVKhEONnnGwNwT7WeLqbspDgVtKex0/nwFwBSW7XJxioHwpg6H.";
    #[cfg(all(not(feature = "persist-suite"), test))]
    pub const TESTSUITE_TEST_TOKEN: &str = "mpobAB7EY8vnBs70d/..h1VvfinKIeEJgt1rg4wUkwF6aWCvGGR9le";
}

uninit_array! {
    const USER_ROOT_ARRAY: [u8; 40] = [b'r', b'o', b'o', b't'];
}
/// The root user
const USER_ROOT: AuthID = unsafe { AuthID::from_const(USER_ROOT_ARRAY, 4) };

/// An authn ID
type AuthID = Array<u8, AUTHID_SIZE>;
/// An authn key
pub type Authkey = [u8; AUTHKEY_SIZE];
/// Result of an auth operation
pub type AuthResult<T> = Result<T, AuthError>;
/// Authmap
pub type Authmap = Arc<Coremap<AuthID, Authkey>>;

/// The authn/authz provider
///
pub struct AuthProvider {
    origin: Option<Authkey>,
    /// the current user
    whoami: Option<AuthID>,
    /// a map of users
    authmap: Authmap,
}

impl AuthProvider {
    fn _new(authmap: Authmap, whoami: Option<AuthID>, origin: Option<Authkey>) -> Self {
        Self {
            authmap,
            whoami,
            origin,
        }
    }
    /// New provider with no origin-key
    pub fn new_disabled() -> Self {
        Self::_new(Default::default(), None, None)
    }
    /// New provider with zero users
    #[cfg(test)]
    pub fn new_blank(origin: Option<Authkey>) -> Self {
        Self::_new(Default::default(), None, origin)
    }
    /// New provider with users from the provided map
    ///
    /// ## Test suite
    /// The testsuite creates users `root` and `testuser`; this **does not** apply to
    /// release mode
    pub fn new(authmap: Arc<Coremap<AuthID, Authkey>>, origin: Option<Authkey>) -> Self {
        let slf = Self::_new(authmap, None, origin);
        #[cfg(debug_assertions)]
        {
            // 'root' user in test mode
            slf.authmap.true_if_insert(
                AuthID::try_from_slice(testsuite_data::TESTSUITE_ROOT_USER).unwrap(),
                Authkey::from([
                    172, 143, 117, 169, 158, 156, 33, 106, 139, 107, 20, 106, 91, 219, 34, 157, 98,
                    147, 142, 91, 222, 238, 205, 120, 72, 171, 90, 218, 147, 2, 75, 67, 44, 108,
                    185, 124, 55, 40, 156, 252,
                ]),
            );
            // 'testuser' user in test mode
            slf.authmap.true_if_insert(
                AuthID::try_from_slice(testsuite_data::TESTSUITE_TEST_USER).unwrap(),
                Authkey::from([
                    172, 183, 60, 221, 53, 240, 231, 217, 113, 112, 98, 16, 109, 62, 235, 95, 184,
                    107, 130, 139, 43, 197, 40, 31, 176, 127, 185, 22, 172, 124, 39, 225, 124, 71,
                    193, 115, 176, 162, 239, 93,
                ]),
            );
        }
        slf
    }
    pub const fn is_enabled(&self) -> bool {
        matches!(self.origin, Some(_))
    }
    pub fn claim_root(&mut self, origin_key: &[u8]) -> AuthResult<String> {
        self.verify_origin(origin_key)?;
        // the origin key was good, let's try claiming root
        let (key, store) = keys::generate_full();
        if self.authmap.true_if_insert(USER_ROOT, store) {
            // claimed, sweet, log them in
            self.whoami = Some(USER_ROOT);
            Ok(key)
        } else {
            Err(AuthError::AlreadyClaimed)
        }
    }
    fn are_you_root(&self) -> AuthResult<bool> {
        self.ensure_enabled()?;
        match self.whoami.as_ref().map(|v| v.eq(&USER_ROOT)) {
            Some(v) => Ok(v),
            None => Err(AuthError::Anonymous),
        }
    }
    pub fn claim_user(&self, claimant: &[u8]) -> AuthResult<String> {
        self.ensure_root()?;
        self._claim_user(claimant)
    }
    pub fn _claim_user(&self, claimant: &[u8]) -> AuthResult<String> {
        let (key, store) = keys::generate_full();
        if self
            .authmap
            .true_if_insert(Self::try_auth_id(claimant)?, store)
        {
            Ok(key)
        } else {
            Err(AuthError::AlreadyClaimed)
        }
    }
    pub fn login(&mut self, account: &[u8], token: &[u8]) -> AuthResult<()> {
        self.ensure_enabled()?;
        match self
            .authmap
            .get(account)
            .map(|token_hash| keys::verify_key(token, token_hash.as_slice()))
        {
            Some(Some(true)) => {
                // great, authenticated
                self.whoami = Some(Self::try_auth_id(account)?);
                Ok(())
            }
            _ => {
                // either the password was wrong, or the username was wrong
                Err(AuthError::BadCredentials)
            }
        }
    }
    pub fn regenerate_using_origin(&self, origin: &[u8], account: &[u8]) -> AuthResult<String> {
        self.verify_origin(origin)?;
        self._regenerate(account)
    }
    pub fn regenerate(&self, account: &[u8]) -> AuthResult<String> {
        self.ensure_root()?;
        self._regenerate(account)
    }
    /// Regenerate the token for the given user. This returns a new token
    fn _regenerate(&self, account: &[u8]) -> AuthResult<String> {
        let id = Self::try_auth_id(account)?;
        let (key, store) = keys::generate_full();
        if self.authmap.true_if_update(id, store) {
            Ok(key)
        } else {
            Err(AuthError::BadCredentials)
        }
    }
    fn try_auth_id(authid: &[u8]) -> AuthResult<AuthID> {
        if authid.is_ascii() && authid.len() <= AUTHID_SIZE {
            Ok(unsafe {
                // We just verified the length
                AuthID::from_slice(authid)
            })
        } else {
            Err(AuthError::Other(errors::AUTH_ERROR_ILLEGAL_USERNAME))
        }
    }
    pub fn logout(&mut self) -> AuthResult<()> {
        self.ensure_enabled()?;
        self.whoami.take().map(|_| ()).ok_or(AuthError::Anonymous)
    }
    fn ensure_enabled(&self) -> AuthResult<()> {
        self.origin.as_ref().map(|_| ()).ok_or(AuthError::Disabled)
    }
    pub fn verify_origin(&self, origin: &[u8]) -> AuthResult<()> {
        if self.get_origin()?.eq(origin) {
            Ok(())
        } else {
            Err(AuthError::BadCredentials)
        }
    }
    fn get_origin(&self) -> AuthResult<&Authkey> {
        match self.origin.as_ref() {
            Some(key) => Ok(key),
            None => Err(AuthError::Disabled),
        }
    }
    fn ensure_root(&self) -> AuthResult<()> {
        if self.are_you_root()? {
            Ok(())
        } else {
            Err(AuthError::PermissionDenied)
        }
    }
    pub fn delete_user(&self, user: &[u8]) -> AuthResult<()> {
        self.ensure_root()?;
        if user.eq(&USER_ROOT) {
            // can't delete root!
            Err(AuthError::Other(errors::AUTH_ERROR_FAILED_TO_DELETE_USER))
        } else if self.authmap.true_if_removed(user) {
            Ok(())
        } else {
            Err(AuthError::BadCredentials)
        }
    }
    /// List all the users
    pub fn collect_usernames(&self) -> AuthResult<Vec<String>> {
        self.ensure_root()?;
        Ok(self
            .authmap
            .iter()
            .map(|kv| String::from_utf8_lossy(kv.key()).to_string())
            .collect())
    }
}

impl Clone for AuthProvider {
    fn clone(&self) -> Self {
        Self {
            authmap: self.authmap.clone(),
            whoami: None,
            origin: self.origin,
        }
    }
}
