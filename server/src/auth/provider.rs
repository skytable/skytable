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
use core::mem::MaybeUninit;
use std::sync::Arc;

// constants
/// Size of an authn key in bytes
pub const AUTHKEY_SIZE: usize = 40;
/// Size of an authn ID in bytes
pub const AUTHID_SIZE: usize = 40;
#[sky_macros::array]
const USER_ROOT_ARRAY: [MaybeUninit<u8>; 40] = [b'r', b'o', b'o', b't'];
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
    pub fn new_disabled() -> Self {
        Self {
            authmap: Arc::default(),
            whoami: None,
            origin: None,
        }
    }
    pub fn new(authmap: Arc<Coremap<AuthID, Authkey>>, origin: Option<Authkey>) -> Self {
        Self {
            authmap,
            whoami: None,
            origin,
        }
    }
    pub const fn is_enabled(&self) -> bool {
        matches!(self.origin, Some(_))
    }
    pub fn claim_root(&mut self, origin_key: &[u8]) -> AuthResult<String> {
        let origin = self.get_origin()?;
        if origin == origin_key {
            // the origin key was good, let's try claiming root
            let (key, store) = keys::generate_full();
            if self.authmap.true_if_insert(USER_ROOT, store) {
                // claimed, sweet, log them in
                self.whoami = Some(USER_ROOT);
                Ok(key)
            } else {
                Err(AuthError::AlreadyClaimed)
            }
        } else {
            Err(AuthError::BadCredentials)
        }
    }
    fn are_you_root(&self) -> AuthResult<bool> {
        match self.whoami.as_ref().map(|v| v.eq(&USER_ROOT)) {
            Some(v) => Ok(v),
            None => Err(AuthError::Anonymous),
        }
    }
    pub fn claim_user(&self, claimant: &[u8]) -> AuthResult<String> {
        if self.are_you_root()? {
            self._claim_user(claimant)
        } else {
            Err(AuthError::PermissionDenied)
        }
    }
    fn _claim_user(&self, claimant: &[u8]) -> AuthResult<String> {
        let (key, store) = keys::generate_full();
        if self.authmap.true_if_insert(
            Array::try_from_slice(claimant).ok_or(AuthError::Other(errors::AUTH_ERROR_TOO_LONG))?,
            store,
        ) {
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
            Some(true) => {
                // great, authenticated
                self.whoami = Some(Array::try_from_slice(account).unwrap());
                Ok(())
            }
            Some(false) | None => {
                // imposter!
                Err(AuthError::BadCredentials)
            }
        }
    }
    pub fn logout(&mut self) -> AuthResult<()> {
        self.whoami.take().map(|_| ()).ok_or(AuthError::Anonymous)
    }
    fn ensure_enabled(&self) -> AuthResult<()> {
        self.origin.as_ref().map(|_| ()).ok_or(AuthError::Disabled)
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
