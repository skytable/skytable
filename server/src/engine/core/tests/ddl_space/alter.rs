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
    core::space::{Space, SpaceMeta},
    data::cell::Datacell,
    error::QueryError,
    fractal::test_utils::TestGlobal,
};

#[test]
fn alter_add_prop_env_var() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    super::exec_create_alter(
        &global,
        "create space myspace",
        "alter space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space,
                &Space::new_with_uuid(
                    into_dict!(),
                    SpaceMeta::with_env(into_dict! ("MY_NEW_PROP" => Datacell::new_uint(100))),
                    space.get_uuid()
                )
            );
        },
    )
    .unwrap();
}

#[test]
fn alter_update_prop_env_var() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    let uuid = super::exec_create(
        &global,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            let rl = space.meta.dict().read();
            assert_eq!(
                SpaceMeta::get_env(&rl).get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint(100).into())
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
                &Space::new_with_uuid(
                    into_dict!(),
                    SpaceMeta::with_env(into_dict! ("MY_NEW_PROP" => Datacell::new_uint(200))),
                    uuid,
                )
            )
        },
    )
    .unwrap();
}

#[test]
fn alter_remove_prop_env_var() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    let uuid = super::exec_create(
        &global,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            let rl = space.meta.dict().read();
            assert_eq!(
                SpaceMeta::get_env(&rl).get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint(100).into())
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
                &Space::new_with_uuid(into_dict!(), SpaceMeta::with_env(into_dict!()), uuid)
            )
        },
    )
    .unwrap();
}

#[test]
fn alter_nx() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    assert_eq!(
        super::exec_alter(
            &global,
            "alter space myspace with { env: { MY_NEW_PROP: 100 } }",
            |_| {},
        )
        .unwrap_err(),
        QueryError::QPObjectNotFound
    );
}

#[test]
fn alter_remove_all_env() {
    let global = TestGlobal::new_with_tmp_nullfs_driver();
    let uuid = super::exec_create(
        &global,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            let rl = space.meta.dict().read();
            assert_eq!(
                SpaceMeta::get_env(&rl).get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint(100).into())
            )
        },
    )
    .unwrap();
    super::exec_alter(&global, "alter space myspace with { env: null }", |space| {
        assert_eq!(space, &Space::empty_with_uuid(uuid))
    })
    .unwrap();
}
