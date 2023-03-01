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
    core::{
        model::cell::Datacell,
        space::{Space, SpaceMeta},
        GlobalNS,
    },
    error::DatabaseError,
};

#[test]
fn alter_add_prop_env_var() {
    let gns = GlobalNS::empty();
    super::exec_create_empty_verify(&gns, "create space myspace");
    super::exec_alter_and_verify(
        &gns,
        "alter space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.unwrap(),
                &Space::new(
                    into_dict!(),
                    SpaceMeta::with_env(into_dict! ("MY_NEW_PROP" => Datacell::new_uint(100)))
                )
            )
        },
    )
}

#[test]
fn alter_update_prop_env_var() {
    let gns = GlobalNS::empty();
    super::exec_create_and_verify(
        &gns,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.unwrap().meta.env.read().get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint(100).into())
            )
        },
    );
    super::exec_alter_and_verify(
        &gns,
        "alter space myspace with { env: { MY_NEW_PROP: 200 } }",
        |space| {
            assert_eq!(
                space.unwrap(),
                &Space::new(
                    into_dict!(),
                    SpaceMeta::with_env(into_dict! ("MY_NEW_PROP" => Datacell::new_uint(200)))
                )
            )
        },
    )
}

#[test]
fn alter_remove_prop_env_var() {
    let gns = GlobalNS::empty();
    super::exec_create_and_verify(
        &gns,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.unwrap().meta.env.read().get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint(100).into())
            )
        },
    );
    super::exec_alter_and_verify(
        &gns,
        "alter space myspace with { env: { MY_NEW_PROP: null } }",
        |space| {
            assert_eq!(
                space.unwrap(),
                &Space::new(into_dict!(), SpaceMeta::with_env(into_dict!()))
            )
        },
    )
}

#[test]
fn alter_nx() {
    let gns = GlobalNS::empty();
    super::exec_alter_and_verify(
        &gns,
        "alter space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| assert_eq!(space.unwrap_err(), DatabaseError::DdlSpaceNotFound),
    )
}

#[test]
fn alter_remove_all_env() {
    let gns = GlobalNS::empty();
    super::exec_create_and_verify(
        &gns,
        "create space myspace with { env: { MY_NEW_PROP: 100 } }",
        |space| {
            assert_eq!(
                space.unwrap().meta.env.read().get("MY_NEW_PROP").unwrap(),
                &(Datacell::new_uint(100).into())
            )
        },
    );
    super::exec_alter_and_verify(&gns, "alter space myspace with { env: null }", |space| {
        assert_eq!(space.unwrap(), &Space::empty())
    })
}
