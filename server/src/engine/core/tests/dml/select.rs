/*
 * Created on Thu May 11 2023
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

use crate::engine::{data::cell::Datacell, error::QueryError, fractal::test_utils::TestGlobal};

#[test]
fn simple_select_wildcard() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    assert_eq!(
        super::exec_select(
            &global,
            "create model myspace.mymodel(username: string, password: string)",
            "insert into myspace.mymodel('sayan', 'pass123')",
            "select * from myspace.mymodel where username = 'sayan'",
        )
        .unwrap(),
        intovec!["sayan", "pass123"]
    );
}

#[test]
fn simple_select_specified_same_order() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    assert_eq!(
        super::exec_select(
            &global,
            "create model myspace.mymodel(username: string, password: string)",
            "insert into myspace.mymodel('sayan', 'pass123')",
            "select username, password from myspace.mymodel where username = 'sayan'",
        )
        .unwrap(),
        intovec!["sayan", "pass123"]
    );
}

#[test]
fn simple_select_specified_reversed_order() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    assert_eq!(
        super::exec_select(
            &global,
            "create model myspace.mymodel(username: string, password: string)",
            "insert into myspace.mymodel('sayan', 'pass123')",
            "select password, username from myspace.mymodel where username = 'sayan'",
        )
        .unwrap(),
        intovec!["pass123", "sayan"]
    );
}

#[test]
fn select_null() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    assert_eq!(
        super::exec_select(
            &global,
            "create model myspace.mymodel(username: string, null password: string)",
            "insert into myspace.mymodel('sayan', null)",
            "select username, password from myspace.mymodel where username = 'sayan'",
        )
        .unwrap(),
        intovec!["sayan", Datacell::null()]
    );
}

#[test]
fn select_nonexisting() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    assert_eq!(
        super::exec_select(
            &global,
            "create model myspace.mymodel(username: string, null password: string)",
            "insert into myspace.mymodel('sayan', null)",
            "select username, password from myspace.mymodel where username = 'notsayan'",
        )
        .unwrap_err(),
        QueryError::QExecDmlRowNotFound
    );
}
