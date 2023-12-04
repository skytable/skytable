/*
 * Created on Wed Nov 29 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

use {
    super::{INVALID_SYNTAX_ERR, UNKNOWN_STMT_ERR},
    sky_macros::dbtest,
    skytable::{error::Error, query},
};

#[dbtest]
fn deny_unknown_sysctl() {
    let mut db = db!();
    for stmt in [
        "sysctl magic moon",
        "sysctl create wormhole",
        "sysctl drop dem",
    ] {
        assert_err_eq!(
            db.query_parse::<()>(&query!(stmt)),
            Error::ServerError(UNKNOWN_STMT_ERR)
        );
    }
}

#[dbtest]
fn ensure_sysctl_status_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("sysctl report status blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_sysctl_create_user() {
    let mut db = db!();
    let query = format!("sysctl create user myuser with {{ password: ? }} blah");
    assert_err_eq!(
        db.query_parse::<()>(&query!(query, "mypass")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_sysctl_drop_user() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("sysctl drop user ? blah", "myuser",)),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}
