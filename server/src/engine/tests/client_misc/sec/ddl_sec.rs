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
fn deny_unknown() {
    let mut db = db!();
    for stmt in [
        "create magic blue",
        "alter rainbow hue",
        "drop sadistic view",
    ] {
        assert_err_eq!(
            db.query_parse::<()>(&query!(stmt)),
            Error::ServerError(UNKNOWN_STMT_ERR)
        );
    }
}

#[dbtest]
fn ensure_create_space_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("create space myspace with {} blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!("create space myspace blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_alter_space_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("alter space myspace with {} blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_drop_space_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("drop space myspace blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!("drop space myspace force blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_create_model_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "create model myspace.mymodel(username: string, password: binary) blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "create model myspace.mymodel(username: string, password: binary) with {} blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_alter_model_add_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "alter model myspace.mymodel add phone_number { type: uint64 } blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "alter model myspace.mymodel add (phone_number { type: uint64 }, email_id { type: string }) with {} blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_alter_model_update_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "alter model myspace.mymodel update password { type: string } blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "alter model myspace.mymodel update (username {type: binary}, password { type: string }) blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_alter_model_remove_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("alter model myspace.mymodel remove email_id blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "alter model myspace.mymodel remove (email_id, phone_number) blah"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn ensure_drop_model_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!("drop model myspace.mymodel blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!("drop model myspace.mymodel force blah")),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}
