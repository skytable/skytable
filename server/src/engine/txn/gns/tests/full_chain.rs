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
        model::{Field, Layer, Model},
        space::{Space, SpaceMeta},
        GlobalNS,
    },
    data::{cell::Datacell, tag::TagSelector, uuid::Uuid, DictEntryGeneric},
    error::DatabaseError,
    idx::STIndex,
    ql::{
        ast::parse_ast_node_full,
        ddl::crt::{CreateModel, CreateSpace},
        tests::lex_insecure,
    },
    storage::v1::header_meta::HostRunMode,
    txn::gns::GNSTransactionDriverVFS,
};

fn multirun(f: impl FnOnce() + Copy) {
    for _ in 0..10 {
        f()
    }
}

fn with_variable<T>(var: T, f: impl FnOnce(T)) {
    f(var);
}

fn init_txn_driver(gns: &GlobalNS, log_name: &str) -> GNSTransactionDriverVFS {
    GNSTransactionDriverVFS::open_or_reinit_with_name(&gns, log_name, 0, HostRunMode::Prod, 0)
        .unwrap()
}

fn init_space(
    gns: &GlobalNS,
    driver: &mut GNSTransactionDriverVFS,
    space_name: &str,
    env: &str,
) -> Uuid {
    let query = format!("create space {space_name} with {{ env: {env} }}");
    let stmt = lex_insecure(query.as_bytes()).unwrap();
    let stmt = parse_ast_node_full::<CreateSpace>(&stmt[2..]).unwrap();
    let name = stmt.space_name;
    Space::transactional_exec_create(&gns, driver, stmt).unwrap();
    gns.spaces().read().get(name.as_str()).unwrap().get_uuid()
}

#[test]
fn create_space() {
    with_variable("create_space_test.gns.db-tlog", |log_name| {
        let uuid;
        // start 1
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            uuid = init_space(&gns, &mut driver, "myspace", "{ SAYAN_MAX: 65536 }"); // good lord that doesn't sound like a good variable
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(
                gns.spaces().read().get("myspace").unwrap(),
                &Space::new_restore_empty(
                    SpaceMeta::with_env(
                        into_dict!("SAYAN_MAX" => DictEntryGeneric::Data(Datacell::new_uint(65536)))
                    ),
                    uuid
                )
            );
            driver.close().unwrap();
        })
    })
}

#[test]
fn alter_space() {
    with_variable("alter_space_test.gns.db-tlog", |log_name| {
        let uuid;
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            uuid = init_space(&gns, &mut driver, "myspace", "{}");
            let stmt =
                lex_insecure("alter space myspace with { env: { SAYAN_MAX: 65536 } }".as_bytes())
                    .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Space::transactional_exec_alter(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(
                gns.spaces().read().get("myspace").unwrap(),
                &Space::new_restore_empty(
                    SpaceMeta::with_env(
                        into_dict!("SAYAN_MAX" => DictEntryGeneric::Data(Datacell::new_uint(65536)))
                    ),
                    uuid
                )
            );
            driver.close().unwrap();
        })
    })
}

#[test]
fn drop_space() {
    with_variable("drop_space_test.gns.db-tlog", |log_name| {
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            let _ = init_space(&gns, &mut driver, "myspace", "{}");
            let stmt = lex_insecure("drop space myspace".as_bytes()).unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Space::transactional_exec_drop(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(gns.spaces().read().get("myspace"), None);
            driver.close().unwrap();
        })
    })
}

fn init_model(
    gns: &GlobalNS,
    txn_driver: &mut GNSTransactionDriverVFS,
    space_name: &str,
    model_name: &str,
    decl: &str,
) -> Uuid {
    let query = format!("create model {space_name}.{model_name} ({decl})");
    let stmt = lex_insecure(query.as_bytes()).unwrap();
    let stmt = parse_ast_node_full::<CreateModel>(&stmt[2..]).unwrap();
    let model_name = stmt.model_name;
    Model::transactional_exec_create(&gns, txn_driver, stmt).unwrap();
    gns.with_model(model_name, |model| Ok(model.get_uuid()))
        .unwrap()
}

fn init_default_model(gns: &GlobalNS, driver: &mut GNSTransactionDriverVFS) -> Uuid {
    init_model(
        gns,
        driver,
        "myspace",
        "mymodel",
        "username: string, password: binary",
    )
}

#[test]
fn create_model() {
    with_variable("create_model_test.gns.db-tlog", |log_name| {
        let _uuid_space;
        let uuid_model;
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            _uuid_space = init_space(&gns, &mut driver, "myspace", "{}");
            uuid_model = init_default_model(&gns, &mut driver);
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            gns.with_model(("myspace", "mymodel"), |model| {
                assert_eq!(
                    model,
                    &Model::new_restore(
                        uuid_model,
                        "username".into(),
                        TagSelector::Str.into_full(),
                        into_dict! {
                            "username" => Field::new([Layer::str()].into(), false),
                            "password" => Field::new([Layer::bin()].into(), false),
                        }
                    )
                );
                Ok(())
            })
            .unwrap();
            driver.close().unwrap();
        })
    })
}

#[test]
fn alter_model_add() {
    with_variable("alter_model_add_test.gns.db-tlog", |log_name| {
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            init_space(&gns, &mut driver, "myspace", "{}");
            init_default_model(&gns, &mut driver);
            let stmt = lex_insecure(
                b"alter model myspace.mymodel add profile_pic { type: binary, nullable: true }",
            )
            .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Model::transactional_exec_alter(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            gns.with_model(("myspace", "mymodel"), |model| {
                assert_eq!(
                    model
                        .intent_read_model()
                        .fields()
                        .st_get("profile_pic")
                        .unwrap(),
                    &Field::new([Layer::bin()].into(), true)
                );
                Ok(())
            })
            .unwrap();
            driver.close().unwrap();
        })
    })
}

#[test]
fn alter_model_remove() {
    with_variable("alter_model_remove_test.gns.db-tlog", |log_name| {
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            init_space(&gns, &mut driver, "myspace", "{}");
            init_model(
                &gns,
                &mut driver,
                "myspace",
                "mymodel",
                "username: string, password: binary, null profile_pic: binary, null has_2fa: bool, null has_secure_key: bool, is_dumb: bool",
            );
            let stmt = lex_insecure(
                "alter model myspace.mymodel remove (has_secure_key, is_dumb)".as_bytes(),
            )
            .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Model::transactional_exec_alter(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap()
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            gns.with_model(("myspace", "mymodel"), |model| {
                let irm = model.intent_read_model();
                assert!(irm.fields().st_get("has_secure_key").is_none());
                assert!(irm.fields().st_get("is_dumb").is_none());
                Ok(())
            })
            .unwrap();
            driver.close().unwrap()
        })
    })
}

#[test]
fn alter_model_update() {
    with_variable("alter_model_update_test.gns.db-tlog", |log_name| {
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            init_space(&gns, &mut driver, "myspace", "{}");
            init_model(
                &gns,
                &mut driver,
                "myspace",
                "mymodel",
                "username: string, password: binary, profile_pic: binary",
            );
            let stmt =
                lex_insecure(b"alter model myspace.mymodel update profile_pic { nullable: true }")
                    .unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Model::transactional_exec_alter(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            gns.with_model(("myspace", "mymodel"), |model| {
                assert_eq!(
                    model
                        .intent_read_model()
                        .fields()
                        .st_get("profile_pic")
                        .unwrap(),
                    &Field::new([Layer::bin()].into(), true)
                );
                Ok(())
            })
            .unwrap();
            driver.close().unwrap();
        })
    })
}

#[test]
fn drop_model() {
    with_variable("drop_model_test.gns.db-tlog", |log_name| {
        {
            let gns = GlobalNS::empty();
            let mut driver = init_txn_driver(&gns, log_name);
            init_space(&gns, &mut driver, "myspace", "{}");
            init_default_model(&gns, &mut driver);
            let stmt = lex_insecure(b"drop model myspace.mymodel").unwrap();
            let stmt = parse_ast_node_full(&stmt[2..]).unwrap();
            Model::transactional_exec_drop(&gns, &mut driver, stmt).unwrap();
            driver.close().unwrap();
        }
        multirun(|| {
            let gns = GlobalNS::empty();
            let driver = init_txn_driver(&gns, log_name);
            assert_eq!(
                gns.with_model(("myspace", "mymodel"), |_| { Ok(()) })
                    .unwrap_err(),
                DatabaseError::DdlModelNotFound
            );
            driver.close().unwrap();
        })
    })
}
