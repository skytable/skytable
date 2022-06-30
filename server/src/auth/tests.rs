/*
 * Created on Tue Feb 22 2022
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

mod keys {
    use super::super::keys::{generate_full, verify_key};

    #[test]
    fn test_verify_key() {
        let (key, store) = generate_full();
        assert!(verify_key(key.as_bytes(), &store).unwrap());
    }
}

mod authn {
    use crate::{
        actions::ActionError,
        auth::AuthProvider,
        protocol::{interface::ProtocolSpec, Skyhash2},
    };

    const ORIG: &[u8; 40] = b"c4299d190fb9a00626797fcc138c56eae9971664";

    #[test]
    fn claim_root_okay() {
        let mut provider = AuthProvider::new_blank(Some(*ORIG));
        let _ = provider.claim_root::<Skyhash2>(ORIG).unwrap();
    }
    #[test]
    fn claim_root_wrongkey() {
        let mut provider = AuthProvider::new_blank(Some(*ORIG));
        let claim_err = provider.claim_root::<Skyhash2>(&ORIG[1..]).unwrap_err();
        assert_eq!(
            claim_err,
            ActionError::ActionError(Skyhash2::AUTH_CODE_BAD_CREDENTIALS)
        );
    }
    #[test]
    fn claim_root_disabled() {
        let mut provider = AuthProvider::new_disabled();
        assert_eq!(
            provider.claim_root::<Skyhash2>(b"abcd").unwrap_err(),
            ActionError::ActionError(Skyhash2::AUTH_ERROR_DISABLED)
        );
    }
    #[test]
    fn claim_root_already_claimed() {
        let mut provider = AuthProvider::new_blank(Some(*ORIG));
        let _ = provider.claim_root::<Skyhash2>(ORIG).unwrap();
        assert_eq!(
            provider.claim_root::<Skyhash2>(ORIG).unwrap_err(),
            ActionError::ActionError(Skyhash2::AUTH_ERROR_ALREADYCLAIMED)
        );
    }
    #[test]
    fn claim_user_okay_with_login() {
        let mut provider = AuthProvider::new_blank(Some(*ORIG));
        // claim root
        let rootkey = provider.claim_root::<Skyhash2>(ORIG).unwrap();
        // login as root
        provider
            .login::<Skyhash2>(b"root", rootkey.as_bytes())
            .unwrap();
        // claim user
        let _ = provider.claim_user::<Skyhash2>(b"sayan").unwrap();
    }

    #[test]
    fn claim_user_fail_not_root_with_login() {
        let mut provider = AuthProvider::new_blank(Some(*ORIG));
        // claim root
        let rootkey = provider.claim_root::<Skyhash2>(ORIG).unwrap();
        // login as root
        provider
            .login::<Skyhash2>(b"root", rootkey.as_bytes())
            .unwrap();
        // claim user
        let userkey = provider.claim_user::<Skyhash2>(b"user").unwrap();
        // login as user
        provider
            .login::<Skyhash2>(b"user", userkey.as_bytes())
            .unwrap();
        // now try to claim an user being a non-root account
        assert_eq!(
            provider.claim_user::<Skyhash2>(b"otheruser").unwrap_err(),
            ActionError::ActionError(Skyhash2::AUTH_CODE_PERMS)
        );
    }
    #[test]
    fn claim_user_fail_anonymous() {
        let mut provider = AuthProvider::new_blank(Some(*ORIG));
        // claim root
        let _ = provider.claim_root::<Skyhash2>(ORIG).unwrap();
        // logout
        provider.logout::<Skyhash2>().unwrap();
        // try to claim as an anonymous user
        assert_eq!(
            provider.claim_user::<Skyhash2>(b"newuser").unwrap_err(),
            ActionError::ActionError(Skyhash2::AUTH_CODE_PERMS)
        );
    }
}
