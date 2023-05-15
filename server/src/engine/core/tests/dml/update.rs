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

use crate::engine::{core::GlobalNS, data::cell::Datacell, error::DatabaseError};

#[test]
fn simple() {
    let gns = GlobalNS::empty();
    assert_eq!(
        super::exec_update(
            &gns,
            "create model myspace.mymodel(username: string, email: string, followers: uint64, following: uint64)",
            "insert into myspace.mymodel('sayan', 'sayan@example.com', 0, 100)",
            "update myspace.mymodel set followers += 200000, following -= 15, email = 'sn@example.com' where username = 'sayan'",
            "select * from myspace.mymodel where username = 'sayan'"
        ).unwrap(),
        intovec!["sayan", "sn@example.com", 200_000_u64, 85_u64],
    );
}

#[test]
fn with_null() {
    let gns = GlobalNS::empty();
    assert_eq!(
        super::exec_update(
            &gns,
            "create model myspace.mymodel(username: string, password: string, null email: string)",
            "insert into myspace.mymodel('sayan', 'pass123', null)",
            "update myspace.mymodel set email = 'sayan@example.com' where username = 'sayan'",
            "select * from myspace.mymodel where username='sayan'"
        )
        .unwrap(),
        intovec!["sayan", "pass123", "sayan@example.com"]
    );
}

#[test]
fn fail_unknown_fields() {
    let gns = GlobalNS::empty();
    assert_eq!(
        super::exec_update(
            &gns,
            "create model myspace.mymodel(username: string, password: string, null email: string)",
            "insert into myspace.mymodel('sayan', 'pass123', null)",
            "update myspace.mymodel set email2 = 'sayan@example.com', password += '4' where username = 'sayan'",
            "select * from myspace.mymodel where username='sayan'"
        )
        .unwrap_err(),
        DatabaseError::FieldNotFound
    );
    // verify integrity
    assert_eq!(
        super::exec_select_only(&gns, "select * from myspace.mymodel where username='sayan'")
            .unwrap(),
        intovec!["sayan", "pass123", Datacell::null()]
    );
}

#[test]
fn fail_typedef_violation() {
    let gns = GlobalNS::empty();
    assert_eq!(
        super::exec_update(
            &gns,
            "create model myspace.mymodel(username: string, password: string, rank: uint8)",
            "insert into myspace.mymodel('sayan', 'pass123', 1)",
            "update myspace.mymodel set password = 'pass1234', rank = 'one' where username = 'sayan'",
            "select * from myspace.mymodel where username = 'sayan'"
        )
        .unwrap_err(),
        DatabaseError::DmlConstraintViolationFieldTypedef
    );
    // verify integrity
    assert_eq!(
        super::exec_select_only(
            &gns,
            "select * from myspace.mymodel where username = 'sayan'"
        )
        .unwrap(),
        intovec!["sayan", "pass123", 1u64]
    );
}
