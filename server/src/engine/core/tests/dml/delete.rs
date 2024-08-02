/*
 * Created on Wed May 10 2023
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

use crate::engine::{error::QueryError, fractal::test_utils::TestGlobal};

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn simple_delete() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_delete_simple_delete");
    super::exec_delete(
        &global,
        "create model myspace.mymodel(username: string, password: string)",
        Some("insert into myspace.mymodel('sayan', 'pass123')"),
        "delete from myspace.mymodel where username = 'sayan'",
        "sayan",
    )
    .unwrap();
}

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn delete_nonexisting() {
    let global = TestGlobal::new_with_driver_id_instant_update("dml_delete_delete_nonexisting");
    assert_eq!(
        super::exec_delete(
            &global,
            "create model myspace.mymodel(username: string, password: string)",
            None,
            "delete from myspace.mymodel where username = 'sayan'",
            "sayan",
        )
        .unwrap_err(),
        QueryError::QExecDmlRowNotFound
    );
}
