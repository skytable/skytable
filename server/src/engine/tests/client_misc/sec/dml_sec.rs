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
    super::INVALID_SYNTAX_ERR,
    sky_macros::dbtest,
    skytable::{error::Error, query},
};

#[dbtest]
fn insert_ensure_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "insert into myspace.mymodel(?, ?) blah",
            "username",
            "password"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "insert into myspace.mymodel { username: ?, password: ? } blah",
            "username",
            "password"
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    );
}

#[dbtest]
fn select_ensure_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "select * from myspace.mymodel where username = ? blah",
            "username",
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    )
}

#[dbtest]
fn update_ensure_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "update myspace.mymodel set counter += ? where username = ? blah",
            1u64,
            "username",
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    )
}

#[dbtest]
fn delete_ensure_end_of_tokens() {
    let mut db = db!();
    assert_err_eq!(
        db.query_parse::<()>(&query!(
            "delete from myspace.mymodel where username = ? blah",
            "username",
        )),
        Error::ServerError(INVALID_SYNTAX_ERR)
    )
}
