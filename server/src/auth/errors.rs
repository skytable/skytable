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

use crate::actions::ActionError;
use crate::auth::AuthError;

/// Skyhash respstring: already claimed (user was already claimed)
const AUTH_ERROR_ALREADYCLAIMED: &[u8] = b"!19\nerr-already-claimed\n";
/// Skyhash respcode(10): bad credentials (either bad creds or invalid user)
const AUTH_CODE_DENIED: &[u8] = b"!2\n10\n";
/// Skyhash respstring: auth is disabled
const AUTH_ERROR_DISABLED: &[u8] = b"!17\nerr-auth-disabled\n";
/// Skyhash respcode(11): Insufficient permissions (same for anonymous user)
const AUTH_CODE_PERMS: &[u8] = b"!2\n11\n";

impl From<AuthError> for ActionError {
    fn from(e: AuthError) -> Self {
        let r = match e {
            AuthError::AlreadyClaimed => AUTH_ERROR_ALREADYCLAIMED,
            AuthError::Anonymous | AuthError::PermissionDenied => AUTH_CODE_PERMS,
            AuthError::BadCredentials => AUTH_CODE_DENIED,
            AuthError::Disabled => AUTH_ERROR_DISABLED,
        };
        ActionError::ActionError(r)
    }
}
