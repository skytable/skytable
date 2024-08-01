/*
 * Created on Sun May 14 2023
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

use crate::engine::{
    core::dml, data::cell::Datacell, error::QueryError, fractal::test_utils::TestGlobal,
};

#[sky_macros::test]
fn simple() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_update_simple");
    assert_eq!(
        super::exec_update(
            &global,
            "create model myspace.mymodel(username: string, email: string, followers: uint64, following: uint64)",
            "insert into myspace.mymodel('sayan', 'sayan@example.com', 0, 100)",
            "update myspace.mymodel set followers += 200000, following -= 15, email = 'sn@example.com' where username = 'sayan'",
            "select * from myspace.mymodel where username = 'sayan'"
        ).unwrap(),
        intovec!["sayan", "sn@example.com", 200_000_u64, 85_u64],
    );
    assert_eq!(
        dml::update_flow_trace(),
        ["sametag;nonnull", "sametag;nonnull", "sametag;nonnull"]
    );
}

#[sky_macros::test]
fn with_null() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_update_with_null");
    assert_eq!(
        super::exec_update(
            &global,
            "create model myspace.mymodel(username: string, password: string, null email: string)",
            "insert into myspace.mymodel('sayan', 'pass123', null)",
            "update myspace.mymodel set email = 'sayan@example.com' where username = 'sayan'",
            "select * from myspace.mymodel where username='sayan'"
        )
        .unwrap(),
        intovec!["sayan", "pass123", "sayan@example.com"]
    );
    assert_eq!(dml::update_flow_trace(), ["sametag;orignull"]);
}

#[sky_macros::test]
fn with_list() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_update_with_list");
    assert_eq!(
        super::exec_update(
            &global,
            "create model myspace.mymodel(link: string, click_ids: list { type: string })",
            "insert into myspace.mymodel('example.com', [])",
            "update myspace.mymodel set click_ids += 'ios_client_uuid' where link = 'example.com'",
            "select * from myspace.mymodel where link = 'example.com'"
        )
        .unwrap(),
        intovec![
            "example.com",
            Datacell::new_list(intovec!["ios_client_uuid"])
        ]
    );
    assert_eq!(dml::update_flow_trace(), ["list;sametag"]);
}

#[sky_macros::test]
fn fail_operation_on_null() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_update_fail_operation_on_null");
    assert_eq!(
        super::exec_update(
            &global,
            "create model myspace.mymodel(username: string, password: string, null email: string)",
            "insert into myspace.mymodel('sayan', 'pass123', null)",
            "update myspace.mymodel set email += '.com' where username = 'sayan'",
            "select * from myspace.mymodel where username='sayan'"
        )
        .unwrap_err(),
        QueryError::QExecDmlValidationError
    );
    assert_eq!(
        dml::update_flow_trace(),
        ["unknown_reason;exitmainloop", "rollback"]
    );
}

#[sky_macros::test]
fn fail_unknown_fields() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_update_fail_unknown_fields");
    assert_eq!(
        super::exec_update(
            &global,
            "create model myspace.mymodel(username: string, password: string, null email: string)",
            "insert into myspace.mymodel('sayan', 'pass123', null)",
            "update myspace.mymodel set email2 = 'sayan@example.com', password += '4' where username = 'sayan'",
            "select * from myspace.mymodel where username='sayan'"
        )
        .unwrap_err(),
        QueryError::QExecUnknownField
    );
    assert_eq!(dml::update_flow_trace(), ["fieldnotfound", "rollback"]);
    // verify integrity
    assert_eq!(
        super::exec_select_only(
            &global,
            "select * from myspace.mymodel where username='sayan'"
        )
        .unwrap(),
        intovec!["sayan", "pass123", Datacell::null()]
    );
}

#[sky_macros::test]
fn fail_typedef_violation() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_update_fail_typedef_violation");
    assert_eq!(
        super::exec_update(
            &global,
            "create model myspace.mymodel(username: string, password: string, rank: uint8)",
            "insert into myspace.mymodel('sayan', 'pass123', 1)",
            "update myspace.mymodel set password = 'pass1234', rank = 'one' where username = 'sayan'",
            "select * from myspace.mymodel where username = 'sayan'"
        )
        .unwrap_err(),
        QueryError::QExecDmlValidationError
    );
    assert_eq!(
        dml::update_flow_trace(),
        ["sametag;nonnull", "unknown_reason;exitmainloop", "rollback"]
    );
    // verify integrity
    assert_eq!(
        super::exec_select_only(
            &global,
            "select * from myspace.mymodel where username = 'sayan'"
        )
        .unwrap(),
        intovec!["sayan", "pass123", 1u64]
    );
}
