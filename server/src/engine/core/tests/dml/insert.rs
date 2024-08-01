/*
 * Created on Tue May 09 2023
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

#[derive(sky_macros::Wrapper, Debug)]
struct Tuple(Vec<(Box<str>, Datacell)>);

#[sky_macros::test]
fn insert_simple() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_insert_simple");
    super::exec_insert(
        &global,
        "create model myspace.mymodel(username: string, password: string)",
        "insert into myspace.mymodel('sayan', 'pass123')",
        "sayan",
        |row| {
            assert_veceq_transposed!(row.cloned_data(), Tuple(pairvec!(("password", "pass123"))));
        },
    )
    .unwrap();
}

#[sky_macros::test]
fn insert_with_null() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_insert_with_null");
    super::exec_insert(
        &global,
        "create model myspace.mymodel(username: string, null useless_password: string, null useless_email: string, null useless_random_column: uint64)",
        "insert into myspace.mymodel('sayan', null, null, null)",
        "sayan",
        |row| {
            assert_veceq_transposed!(
                row.cloned_data(),
                Tuple(
                    pairvec!(
                        ("useless_password", Datacell::null()),
                        ("useless_email", Datacell::null()),
                        ("useless_random_column", Datacell::null())
                    )
                )
            )
        }
    ).unwrap();
}

#[sky_macros::test]
fn insert_duplicate() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_insert_duplicate");
    super::exec_insert(
        &global,
        "create model myspace.mymodel(username: string, password: string)",
        "insert into myspace.mymodel('sayan', 'pass123')",
        "sayan",
        |row| {
            assert_veceq_transposed!(row.cloned_data(), Tuple(pairvec!(("password", "pass123"))));
        },
    )
    .unwrap();
    assert_eq!(
        super::exec_insert_only(&global, "insert into myspace.mymodel('sayan', 'pass123')")
            .unwrap_err(),
        QueryError::QExecDmlDuplicate
    );
}
