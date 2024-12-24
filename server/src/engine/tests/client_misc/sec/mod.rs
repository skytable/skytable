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

mod dcl_sec;
mod ddl_sec;
mod dml_sec;

use {
    crate::engine::control_flow::errors::QueryError,
    sky_macros::dbtest,
    skytable::{
        error::{ConnectionSetupError, Error},
        query,
    },
};

const INVALID_SYNTAX_ERR: u16 = QueryError::QLInvalidSyntax.value_u8() as u16;
const EXPECTED_STATEMENT_ERR: u16 = QueryError::QLExpectedStatement.value_u8() as u16;
const UNKNOWN_STMT_ERR: u16 = QueryError::QLUnknownStatement.value_u8() as u16;
const ILLEGAL_PACKET: u16 = QueryError::SysNetworkSystemIllegalClientPacket.value_u8() as u16;

#[dbtest]
fn deny_unknown_tokens() {
    let mut db = db!();
    for token in [
        "model", "space", "where", "force", "into", "from", "with", "set", "add", "remove", "*",
        ",", "",
    ] {
        let result = db.query_parse::<()>(&query!(token));
        if token.is_empty() {
            // the server will reject empty queries
            assert_err_eq!(result, Error::ServerError(ILLEGAL_PACKET), "{token}")
        } else {
            assert_err_eq!(
                result,
                Error::ServerError(EXPECTED_STATEMENT_ERR),
                "{token}",
            );
        }
    }
}

#[dbtest(username = "root", password = "")]
fn ensure_empty_password_returns_hs_error_5() {
    let db = db_connect!();
    assert_err_eq!(
        db,
        Error::ConnectionSetupErr(ConnectionSetupError::HandshakeError(5))
    );
}

#[dbtest(username = "", password = "1234567890")]
fn ensure_empty_username_returns_hs_error_5() {
    let db = db_connect!();
    assert_err_eq!(
        db,
        Error::ConnectionSetupErr(ConnectionSetupError::HandshakeError(5))
    );
}

#[dbtest(username = "", password = "")]
fn ensure_empty_username_and_password_returns_hs_error_5() {
    let db = db_connect!();
    assert_err_eq!(
        db,
        Error::ConnectionSetupErr(ConnectionSetupError::HandshakeError(5))
    );
}
