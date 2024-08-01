/*
 * Created on Thu Feb 09 2023
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
    core::space::Space,
    data::{cell::Datacell, DictEntryGeneric},
    error::QueryError,
    fractal::test_utils::TestGlobal,
};

#[sky_macros::test]
fn alter_add_prop_env_var() {
    let global = TestGlobal::new_with_driver_id("alter_add_prop_env_var");
    super::exec_create_alter(
        &global,
        "create space myspace",
        "alter space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space,
                &Space::new_restore_empty(
                    space.get_uuid(),
                    into_dict!("env" => DictEntryGeneric::Map(into_dict!("MY_NEW_PROP" => Datacell::new_uint_default(100)))),
                )
            );
        },
    )
    .unwrap();
}

#[sky_macros::test]
fn alter_update_prop_env_var() {
    let global = TestGlobal::new_with_driver_id("alter_update_prop_env_var");
    let uuid = super::exec_create(
        &global,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.env().get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint_default(100).into())
            )
        },
    )
    .unwrap();
    super::exec_alter(
        &global,
        "alter space myspace with { env: { MY_NEW_PROP: 200 } }",
        |space| {
            assert_eq!(
                space,
                &Space::new_restore_empty(
                    uuid,
                    into_dict! ("env" => DictEntryGeneric::Map(into_dict!("MY_NEW_PROP" => Datacell::new_uint_default(200)))),
                )
            )
        },
    )
    .unwrap();
}

#[sky_macros::test]
fn alter_remove_prop_env_var() {
    let global = TestGlobal::new_with_driver_id("alter_remove_prop_env_var");
    let uuid = super::exec_create(
        &global,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.env().get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint_default(100).into())
            )
        },
    )
    .unwrap();
    super::exec_alter(
        &global,
        "alter space myspace with { env: { MY_NEW_PROP: null } }",
        |space| {
            assert_eq!(
                space,
                &Space::new_restore_empty(
                    uuid,
                    into_dict!("env" => DictEntryGeneric::Map(into_dict!()))
                )
            )
        },
    )
    .unwrap();
}

#[sky_macros::test]
fn alter_nx() {
    let global = TestGlobal::new_with_driver_id("alter_nx");
    assert_eq!(
        super::exec_alter(
            &global,
            "alter space myspace with { env: { MY_NEW_PROP: 100 } }",
            |_| {},
        )
        .unwrap_err(),
        QueryError::QExecObjectNotFound
    );
}

#[sky_macros::test]
fn alter_remove_all_env() {
    let global = TestGlobal::new_with_driver_id("alter_remove_all_env");
    let uuid = super::exec_create(
        &global,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.env().get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint_default(100).into())
            )
        },
    )
    .unwrap();
    super::exec_alter(&global, "alter space myspace with { env: null }", |space| {
        assert_eq!(
            space,
            &Space::new_restore_empty(
                uuid,
                into_dict!("env" => DictEntryGeneric::Map(into_dict!()))
            )
        )
    })
    .unwrap();
}
