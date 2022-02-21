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

use crate::corestore::heap_array::HeapArray;
use crate::corestore::lazy::Once;
use crate::corestore::map::Skymap;
use openssl::rand::rand_bytes;

/// Size of an authn key in bytes
const AUTHKEY_SIZE: usize = 64;

/// An authn ID
type AuthID = HeapArray;
/// An authn key
type Authkey = [u8; AUTHKEY_SIZE];
/// An authn key that can only be assigned to once
type OnceAuthkey = Once<Authkey>;
/// Result of an auth operation
type AuthResult<T> = Result<T, AuthError>;

/// The authn/authz provider
pub struct AuthProvider {
    /// the origin key
    origin: OnceAuthkey,
    /// the root key
    root: OnceAuthkey,
    /// a map of standard users
    standard: Skymap<AuthID, Authkey>,
}

/// Auth erros
pub enum AuthError {
    /// The auth slot was already claimed
    AlreadyClaimed,
    /// Bad userid/tokens/keys
    BadCredentials,
    /// Auth is disabled
    Disabled,
}

impl AuthProvider {
    pub fn new(
        origin: Option<Authkey>,
        root: Option<Authkey>,
        standard: Skymap<AuthID, Authkey>,
    ) -> Self {
        Self {
            origin: OnceAuthkey::from(origin),
            root: OnceAuthkey::from(root),
            standard,
        }
    }
    /// Claim the root account
    pub fn claim_root(&self, origin: &[u8]) -> AuthResult<Authkey> {
        match self.origin.get() {
            Some(orig) if orig.eq(origin) => {
                let id = Self::generate_full();
                let idc = id.clone();
                if self.root.set(idc) {
                    Ok(id)
                } else {
                    Err(AuthError::AlreadyClaimed)
                }
            }
            Some(_) => Err(AuthError::BadCredentials),
            None => Err(AuthError::Disabled),
        }
    }
    /// Generate an authentication key
    fn generate_full() -> Authkey {
        let mut bytes: Authkey = [0u8; AUTHKEY_SIZE];
        rand_bytes(&mut bytes).unwrap();
        bytes
    }
}
