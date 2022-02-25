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

use crate::corestore::array::Array;
use crate::corestore::htable::Coremap;
use core::mem::MaybeUninit;
use std::sync::Arc;

mod keys;
#[cfg(test)]
mod tests;

// constants
/// Size of an authn key in bytes
const AUTHKEY_SIZE: usize = 40;
/// Size of an authn ID in bytes
const AUTHID_SIZE: usize = 40;
#[sky_macros::array]
const USER_ROOT_ARRAY: [MaybeUninit<u8>; 40] = [b'r', b'o', b'o', b't'];
/// The root user
const USER_ROOT: AuthID = unsafe { AuthID::from_const(USER_ROOT_ARRAY, 4) };

/// An authn ID
type AuthID = Array<u8, AUTHID_SIZE>;
/// An authn key
pub type Authkey = [u8; AUTHKEY_SIZE];
/// Result of an auth operation
type AuthResult<T> = Result<T, AuthError>;

/// The authn/authz provider
///
pub struct AuthProvider {
    origin: Option<Authkey>,
    /// the current user
    whoami: Option<AuthID>,
    /// a map of users
    authmap: Arc<Coremap<AuthID, Authkey>>,
}

/// Auth erros
#[derive(PartialEq, Debug)]
pub enum AuthError {
    /// The auth slot was already claimed
    AlreadyClaimed,
    /// Bad userid/tokens/keys
    BadCredentials,
    /// Auth is disabled
    Disabled,
    /// The action is not available to the current account
    PermissionDenied,
    /// The user is anonymous and doesn't have the right to execute this
    Anonymous,
}

impl AuthProvider {
    pub fn new(authmap: Arc<Coremap<AuthID, Authkey>>, origin: Option<Authkey>) -> Self {
        Self {
            authmap,
            whoami: None,
            origin,
        }
    }
    pub fn claim_root(&self, origin_key: &[u8]) -> AuthResult<String> {
        let origin = self.get_origin()?;
        if origin == origin_key {
            // the origin key was good, let's try claiming root
            let (key, store) = keys::generate_full();
            if self.authmap.true_if_insert(USER_ROOT, store) {
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
        if self
            .authmap
            .true_if_insert(Array::try_from_slice(claimant).unwrap(), store)
        {
            Ok(key)
        } else {
            Err(AuthError::AlreadyClaimed)
        }
    }
    pub fn login(&mut self, account: &[u8], token: &[u8]) -> AuthResult<()> {
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
    fn get_origin(&self) -> AuthResult<&Authkey> {
        match self.origin.as_ref() {
            Some(key) => Ok(key),
            None => Err(AuthError::Disabled),
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
