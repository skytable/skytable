/*
 * Created on Fri Aug 25 2023
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
        model::{Field, Layer, ModelData},
        space::Space,
    },
    data::{cell::Datacell, tag::TagSelector, uuid::Uuid, DictEntryGeneric},
    error::QueryError,
    fractal::{test_utils::TestGlobal, GlobalInstanceLike},
    idx::STIndex,
    ql::{
        ast::parse_ast_node_full,
        ddl::crt::{CreateModel, CreateSpace},
        tests::lex_insecure,
    },
};

fn multirun(f: impl FnOnce() + Copy) {
    for _ in 0..if cfg!(miri) { 2 } else { 10 } {
        f()
    }
}

fn with_variable<T>(var: T, f: impl FnOnce(T)) {
    f(var);
}

fn init_space(global: &impl GlobalInstanceLike, space_name: &str, env: &str) -> Uuid {
    let query = format!("create space {space_name} with {{ env: {env} }}");
    let stmt = lex_insecure(query.as_bytes()).unwrap();
    let stmt = parse_ast_node_full::<CreateSpace>(&stmt[2..]).unwrap();
    let name = stmt.space_name;
    Space::transactional_exec_create(global, stmt).unwrap();
    global
        .state()
        .namespace()
        .idx()
        .read()
        .get(name.as_str())
        .unwrap()
        .get_uuid()
}

#[sky_macros::test]
fn create_space() {
    with_variable("create_space_test.global.db-tlog", |log_name| {
        let uuid;
        // start 1
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            uuid = init_space(&global, "myspace", "{ SAYAN_MAX: 65536 }"); // good lord that doesn't sound like a good variable
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            let spaces = global.state().namespace().idx().read();
            let space = spaces.get("myspace").unwrap();
            assert_eq!(
                &*space,
                &Space::new_restore_empty(
                    uuid,
                    into_dict!("env" => DictEntryGeneric::Map(
                        into_dict!("SAYAN_MAX" => DictEntryGeneric::Data(Datacell::new_uint_default(65536)))
                    ))
                )
            );
        })
    })
}

#[sky_macros::test]
fn alter_space() {
    with_variable("alter_space_test.global.db-tlog", |log_name| {
        let uuid;
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            uuid = init_space(&global, "myspace", "{}");
            let stmt =
                lex_insecure("alter space myspace with { env: { SAYAN_MAX: 65536 } }".as_bytes())
                    .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Space::transactional_exec_alter(&global, stmt).unwrap();
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            let spaces = global.state().namespace().idx().read();
            let space = spaces.get("myspace").unwrap();
            assert_eq!(
                &*space,
                &Space::new_restore_empty(
                    uuid,
                    into_dict!("env" => DictEntryGeneric::Map(
                        into_dict!("SAYAN_MAX" => DictEntryGeneric::Data(Datacell::new_uint_default(65536))
                    )))
                )
            );
        })
    })
}

#[sky_macros::test]
fn drop_space() {
    with_variable("drop_space_test.global.db-tlog", |log_name| {
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            let _ = init_space(&global, "myspace", "{}");
            let stmt = lex_insecure("drop space myspace".as_bytes()).unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Space::transactional_exec_drop(&global, stmt).unwrap();
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            assert!(global
                .state()
                .namespace()
                .idx()
                .read()
                .get("myspace")
                .is_none());
        })
    })
}

fn init_model(
    global: &impl GlobalInstanceLike,
    space_name: &str,
    model_name: &str,
    decl: &str,
) -> Uuid {
    let query = format!("create model {space_name}.{model_name} ({decl})");
    let stmt = lex_insecure(query.as_bytes()).unwrap();
    let stmt = parse_ast_node_full::<CreateModel>(&stmt[2..]).unwrap();
    let model_name = stmt.model_name;
    ModelData::transactional_exec_create(global, stmt).unwrap();
    global
        .state()
        .namespace()
        .with_model(model_name, |model| Ok(model.get_uuid()))
        .unwrap()
}

fn init_default_model(global: &impl GlobalInstanceLike) -> Uuid {
    init_model(
        global,
        "myspace",
        "mymodel",
        "username: string, password: binary",
    )
}

#[sky_macros::test]
fn create_model() {
    with_variable("create_model_test.global.db-tlog", |log_name| {
        let _uuid_space;
        let uuid_model;
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            _uuid_space = init_space(&global, "myspace", "{}");
            uuid_model = init_default_model(&global);
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            global
                .state()
                .namespace()
                .with_model(("myspace", "mymodel").into(), |model| {
                    assert_eq!(
                        model,
                        &ModelData::new_restore(
                            uuid_model,
                            "username".into(),
                            TagSelector::String.into_full(),
                            into_dict! {
                                "username" => Field::new([Layer::str()].into(), false),
                                "password" => Field::new([Layer::bin()].into(), false),
                            }
                        )
                    );
                    Ok(())
                })
                .unwrap();
        })
    })
}

#[sky_macros::test]
fn alter_model_add() {
    with_variable("alter_model_add_test.global.db-tlog", |log_name| {
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            init_space(&global, "myspace", "{}");
            init_default_model(&global);
            let stmt = lex_insecure(
                b"alter model myspace.mymodel add profile_pic { type: binary, nullable: true }",
            )
            .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            ModelData::transactional_exec_alter(&global, stmt).unwrap();
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            global
                .state()
                .namespace()
                .with_model(("myspace", "mymodel").into(), |model| {
                    assert_eq!(
                        model.fields().st_get("profile_pic").unwrap(),
                        &Field::new([Layer::bin()].into(), true)
                    );
                    Ok(())
                })
                .unwrap();
        })
    })
}

#[sky_macros::test]
fn alter_model_remove() {
    with_variable("alter_model_remove_test.global.db-tlog", |log_name| {
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            init_space(&global, "myspace", "{}");
            init_model(
                &global,
                "myspace",
                "mymodel",
                "username: string, password: binary, null profile_pic: binary, null has_2fa: bool, null has_secure_key: bool, is_dumb: bool",
            );
            let stmt = lex_insecure(
                "alter model myspace.mymodel remove (has_secure_key, is_dumb)".as_bytes(),
            )
            .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            ModelData::transactional_exec_alter(&global, stmt).unwrap();
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            global
                .state()
                .namespace()
                .with_model(("myspace", "mymodel").into(), |model| {
                    assert!(model.fields().st_get("has_secure_key").is_none());
                    assert!(model.fields().st_get("is_dumb").is_none());
                    Ok(())
                })
                .unwrap();
        })
    })
}

#[sky_macros::test]
fn alter_model_update() {
    with_variable("alter_model_update_test.global.db-tlog", |log_name| {
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            init_space(&global, "myspace", "{}");
            init_model(
                &global,
                "myspace",
                "mymodel",
                "username: string, password: binary, profile_pic: binary",
            );
            let stmt =
                lex_insecure(b"alter model myspace.mymodel update profile_pic { nullable: true }")
                    .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            ModelData::transactional_exec_alter(&global, stmt).unwrap();
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            global
                .state()
                .namespace()
                .with_model(("myspace", "mymodel").into(), |model| {
                    assert_eq!(
                        model.fields().st_get("profile_pic").unwrap(),
                        &Field::new([Layer::bin()].into(), true)
                    );
                    Ok(())
                })
                .unwrap();
        })
    })
}

#[sky_macros::test]
fn drop_model() {
    with_variable("drop_model_test.global.db-tlog", |log_name| {
        {
            let global = TestGlobal::new_with_driver_id(log_name);
            init_space(&global, "myspace", "{}");
            init_default_model(&global);
            let stmt = lex_insecure(b"drop model myspace.mymodel").unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            ModelData::transactional_exec_drop(&global, stmt).unwrap();
        }
        multirun(|| {
            let global = TestGlobal::new_with_driver_id(log_name);
            assert_eq!(
                global
                    .state()
                    .namespace()
                    .with_model(("myspace", "mymodel").into(), |_| { Ok(()) })
                    .unwrap_err(),
                QueryError::QExecObjectNotFound
            );
        })
    })
}
