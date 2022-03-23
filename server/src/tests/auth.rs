/*
 * Created on Fri Mar 11 2022
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

use crate::auth::provider::testsuite_data;
use skytable::{query, Element, RespCode};

macro_rules! assert_autherror {
    ($con:expr, $query:expr, $eq:expr) => {
        runeq!($con, $query, Element::RespCode($eq))
    };
}
macro_rules! assert_auth_disabled {
    ($con:expr, $query:expr) => {
        assert_autherror!(
            $con,
            $query,
            RespCode::ErrorString("err-auth-disabled".to_owned())
        )
    };
}

macro_rules! assert_auth_perm_error {
    ($con:expr, $query:expr) => {
        assert_autherror!($con, $query, RespCode::AuthPermissionError)
    };
}

macro_rules! assert_auth_bad_credentials {
    ($con:expr, $query:expr) => {
        assert_autherror!($con, $query, RespCode::AuthBadCredentials)
    };
}

const ONLYAUTH: u8 = 0;
const NOAUTH: u8 = 1;

macro_rules! assert_authn_resp_matrix {
    ($con:expr, $query:expr, $username:ident, $password:ident, $resp:expr) => {
        runeq!(
            $con,
            ::skytable::query!("auth", "login", $username, $password),
            ::skytable::Element::RespCode(::skytable::RespCode::Okay)
        );
        runeq!($con, $query, $resp);
    };
    ($con:expr, $query:expr, $resp:expr) => {{
        runeq!($con, $query, $resp)
    }};
    ($con:expr, $query:expr, $authnd:ident, $resp:expr) => {{
        match $authnd {
            ONLYAUTH => {
                assert_authn_resp_matrix!($con, $query, ROOT_USER, ROOT_PASS, $resp);
                assert_authn_resp_matrix!($con, $query, USER, PASS, $resp);
            }
            NOAUTH => {
                assert_authn_resp_matrix!($con, $query, $resp);
                assert_authn_resp_matrix!($con, $query, ROOT_USER, ROOT_PASS, $resp);
                assert_authn_resp_matrix!($con, $query, USER, PASS, $resp);
            }
            _ => panic!("Unknown authnd state"),
        }
    }};
}

// auth claim
// auth claim fail because it is disabled
#[sky_macros::dbtest_func]
async fn auth_claim_fail_disabled() {
    assert_auth_disabled!(con, query!("auth", "claim", "blah"))
}
// auth claim fail because it has already been claimed
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn claim_root_fail_already_claimed() {
    runeq!(
        con,
        query!("auth", "claim", crate::TEST_AUTH_ORIGIN_KEY),
        Element::RespCode(RespCode::ErrorString("err-auth-already-claimed".to_owned()))
    )
}

// auth login
// auth login fail because it is disabled
#[sky_macros::dbtest_func]
async fn auth_login_fail() {
    assert_auth_disabled!(con, query!("auth", "login", "user", "blah"))
}
// auth login okay (testuser)
#[sky_macros::dbtest_func(port = 2005, auth_testuser = true)]
async fn auth_login_testuser() {
    runeq!(
        con,
        query!("heya", "abcd"),
        Element::String("abcd".to_owned())
    )
}
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn auth_login_testuser_fail_bad_creds() {
    assert_auth_bad_credentials!(con, query!("auth", "login", "testuser", "badpass"))
}
// auth login okay (root)
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true)]
async fn auth_login_rootuser() {
    runeq!(
        con,
        query!("heya", "abcd"),
        Element::String("abcd".to_owned())
    )
}
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn auth_login_rootuser_fail_bad_creds() {
    assert_auth_bad_credentials!(con, query!("auth", "login", "root", "badpass"))
}

// auth adduser
// auth adduser fail because disabled
#[sky_macros::dbtest_func]
async fn auth_adduser_fail_because_disabled() {
    assert_auth_disabled!(con, query!("auth", "adduser", "user"))
}
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn auth_adduser_fail_because_anonymous() {
    assert_auth_perm_error!(con, query!("auth", "adduser", "someuser"))
}
// auth adduser okay because root
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true)]
async fn auth_createuser_root_okay() {
    runmatch!(con, query!("auth", "adduser", "someuser"), Element::String)
}
// auth adduser fail because not root
#[sky_macros::dbtest_func(port = 2005, auth_testuser = true)]
async fn auth_createuser_testuser_fail() {
    assert_auth_perm_error!(con, query!("auth", "adduser", "someuser"))
}

// auth logout
// auth logout failed because auth is disabled
#[sky_macros::dbtest_func]
async fn auth_logout_fail_because_disabled() {
    assert_auth_disabled!(con, query!("auth", "logout"))
}
// auth logout failed because user is anonymous
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn auth_logout_fail_because_anonymous() {
    assert_auth_perm_error!(con, query!("auth", "logout"))
}
// auth logout okay because the correct user is logged in
#[sky_macros::dbtest_func(port = 2005, auth_testuser = true, norun = true)]
async fn auth_logout_okay_testuser() {
    assert_okay!(con, query!("auth", "logout"))
}
// auth logout okay because the correct user is logged in
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true, norun = true)]
async fn auth_logout_okay_rootuser() {
    assert_okay!(con, query!("auth", "logout"))
}

// auth deluser
// auth deluser failed because auth is disabled
#[sky_macros::dbtest_func]
async fn auth_deluser_fail_because_auth_disabled() {
    assert_auth_disabled!(con, query!("auth", "deluser", "testuser"))
}
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn auth_deluser_fail_because_anonymous() {
    assert_auth_perm_error!(con, query!("auth", "deluser", "someuser"))
}
// auth deluser failed because not root
#[sky_macros::dbtest_func(port = 2005, auth_testuser = true)]
async fn auth_deluser_fail_because_not_root() {
    assert_auth_perm_error!(con, query!("auth", "deluser", "testuser"))
}
// auth deluser okay because root
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true)]
async fn auth_deluser_okay_because_root() {
    runmatch!(
        con,
        query!("auth", "adduser", "supercooluser"),
        Element::String
    );
    assert_okay!(con, query!("auth", "deluser", "supercooluser"))
}

// restore
#[sky_macros::dbtest_func]
async fn restore_fail_because_disabled() {
    assert_auth_disabled!(con, query!("auth", "restore", "root"));
}
#[sky_macros::dbtest_func(port = 2005, auth_testuser = true)]
async fn restore_fail_because_not_root() {
    assert_auth_perm_error!(con, query!("auth", "restore", "root"));
}
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true)]
async fn restore_okay_because_root() {
    runmatch!(
        con,
        query!("auth", "adduser", "supercooldude"),
        Element::String
    );
    runmatch!(
        con,
        query!("auth", "restore", "supercooldude"),
        Element::String
    );
}
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true, norun = true)]
async fn restore_okay_with_origin_key() {
    runmatch!(con, query!("auth", "adduser", "someuser2"), Element::String);
    // now logout
    runeq!(
        con,
        query!("auth", "logout"),
        Element::RespCode(RespCode::Okay)
    );
    // we should still be able to restore using origin key
    runmatch!(
        con,
        query!("auth", "restore", crate::TEST_AUTH_ORIGIN_KEY, "someuser2"),
        Element::String
    );
}

// auth listuser
#[sky_macros::dbtest_func]
async fn listuser_fail_because_disabled() {
    assert_auth_disabled!(con, query!("auth", "listuser"));
}
#[sky_macros::dbtest_func(port = 2005, auth_testuser = true)]
async fn listuser_fail_because_not_root() {
    assert_auth_perm_error!(con, query!("auth", "listuser"))
}
#[sky_macros::dbtest_func(port = 2005, auth_rootuser = true)]
async fn listuser_okay_because_root() {
    let ret: Vec<String> = con.run_query(query!("auth", "listuser")).await.unwrap();
    assert!(ret.contains(&"root".to_owned()));
    assert!(ret.contains(&"testuser".to_owned()));
}

// auth whoami
#[sky_macros::dbtest_func]
async fn whoami_fail_because_disabled() {
    assert_auth_disabled!(con, query!("auth", "whoami"))
}
#[sky_macros::dbtest_func(port = 2005, norun = true)]
async fn whoami_fail_because_anonymous() {
    assert_auth_perm_error!(con, query!("auth", "whoami"))
}
#[sky_macros::dbtest_func(port = 2005, norun = true, auth_testuser = true)]
async fn auth_whoami_okay_testuser() {
    runeq!(
        con,
        query!("auth", "whoami"),
        Element::String(testsuite_data::TESTSUITE_TEST_USER.to_owned())
    )
}

#[sky_macros::dbtest_func(port = 2005, norun = true, auth_rootuser = true)]
async fn auth_whoami_okay_rootuser() {
    runeq!(
        con,
        query!("auth", "whoami"),
        Element::String(testsuite_data::TESTSUITE_ROOT_USER.to_owned())
    )
}

mod syntax_checks {
    use super::{NOAUTH, ONLYAUTH};
    use crate::auth::provider::testsuite_data::{
        TESTSUITE_ROOT_TOKEN as ROOT_PASS, TESTSUITE_ROOT_USER as ROOT_USER,
        TESTSUITE_TEST_TOKEN as PASS, TESTSUITE_TEST_USER as USER,
    };
    use skytable::{query, Element, RespCode};
    macro_rules! assert_authn_aerr {
        ($con:expr, $query:expr, $username:expr, $password:expr) => {{
            assert_authn_resp_matrix!(
                $con,
                $query,
                $username,
                $password,
                ::skytable::Element::RespCode(::skytable::RespCode::ActionError)
            )
        }};
        ($con:expr, $query:expr) => {{
            assert_authn_resp_matrix!(
                $con,
                $query,
                NOAUTH,
                ::skytable::Element::RespCode(::skytable::RespCode::ActionError)
            )
        }};
        ($con:expr, $query:expr, $authnd:ident) => {{
            assert_authn_resp_matrix!(
                $con,
                $query,
                $authnd,
                ::skytable::Element::RespCode(::skytable::RespCode::ActionError)
            );
        }};
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn login_aerr() {
        assert_authn_aerr!(con, query!("auth", "login", "lesserdata"));
        assert_authn_aerr!(con, query!("auth", "login", "user", "password", "extra"));
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn claim_aerr() {
        assert_authn_aerr!(con, query!("auth", "claim"));
        assert_authn_aerr!(con, query!("auth", "claim", "origin key", "but more data"));
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn adduser_aerr() {
        assert_authn_aerr!(
            con,
            query!("auth", "adduser", "user", "butextradata"),
            ONLYAUTH
        );
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn logout_aerr() {
        assert_authn_aerr!(con, query!("auth", "logout", "butextradata"), ONLYAUTH);
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn deluser_aerr() {
        assert_authn_aerr!(
            con,
            query!("auth", "deluser", "someuser", "butextradata"),
            ONLYAUTH
        );
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn regenerate_aerr() {
        assert_authn_aerr!(con, query!("auth", "restore"));
        assert_authn_aerr!(
            con,
            query!("auth", "restore", "someuser", "origin", "but extra data")
        );
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn listuser_aerr() {
        assert_authn_aerr!(con, query!("auth", "listuser", "extra argument"), ONLYAUTH);
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true)]
    async fn whoami_aerr() {
        assert_authn_aerr!(con, query!("auth", "whoami", "extra argument"));
    }
    #[sky_macros::dbtest_func(port = 2005, norun = true, auth_testuser = true)]
    async fn unknown_auth_action() {
        runeq!(
            con,
            query!("auth", "raspberry"),
            Element::RespCode(RespCode::ErrorString("Unknown action".to_owned()))
        )
    }
}
