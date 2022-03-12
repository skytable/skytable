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
        assert_autherror!($con, $query, RespCode::ErrorString("11".to_owned()))
    };
}

macro_rules! assert_auth_bad_credentials {
    ($con:expr, $query:expr) => {
        assert_autherror!($con, $query, RespCode::ErrorString("10".to_owned()))
    };
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
        Element::RespCode(RespCode::ErrorString("err-already-claimed".to_owned()))
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