/*
 * Created on Thu Feb 22 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use {
    crate::{
        engine::{
            core::{dml, index::RowData, model::ModelData, space::Space, EntityID, EntityIDRef},
            data::lit::Lit,
            error::QueryResult,
            fractal::{test_utils::TestGlobal, GlobalInstanceLike},
            ql::{
                ast,
                ddl::crt::{CreateModel, CreateSpace},
                dml::{ins::InsertStatement, upd::UpdateStatement},
                tests::lex_insecure,
            },
        },
        util::test_utils,
    },
    crossbeam_epoch::pin,
};

const TEST_DATASET_SIZE: usize = 1000;
const TEST_UPDATE_DATASET_SIZE: usize = 8200; // this peculiar size to force the buffer to flush

fn create_test_kv_strings(change_count: usize) -> Vec<(String, String)> {
    (1..=change_count)
        .map(|i| {
            (
                format!("user-{i:0>change_count$}"),
                format!("password-{i:0>change_count$}"),
            )
        })
        .collect()
}

fn create_test_kv_int(change_count: usize) -> Vec<(u64, String)> {
    (0..change_count)
        .map(|i| (i as u64, format!("password-{i:0>change_count$}")))
        .collect()
}

fn create_model_and_space(global: &TestGlobal, create_model: &str) -> QueryResult<EntityID> {
    let tokens = lex_insecure(create_model.as_bytes()).unwrap();
    let create_model: CreateModel = ast::parse_ast_node_full(&tokens[2..]).unwrap();
    let mdl_name = EntityID::new(
        create_model.model_name.space(),
        create_model.model_name.entity(),
    );
    // first create space
    let create_space_str = format!("create space {}", create_model.model_name.space());
    let create_space_tokens = lex_insecure(create_space_str.as_bytes()).unwrap();
    let create_space: CreateSpace = ast::parse_ast_node_full(&create_space_tokens[2..]).unwrap();
    Space::transactional_exec_create(global, create_space)?;
    ModelData::transactional_exec_create(global, create_model).map(|_| mdl_name)
}

fn run_insert(global: &TestGlobal, insert: &str) -> QueryResult<()> {
    let tokens = lex_insecure(insert.as_bytes()).unwrap();
    let insert: InsertStatement = ast::parse_ast_node_full(&tokens[1..]).unwrap();
    dml::insert(global, insert)
}

fn run_update(global: &TestGlobal, update: &str) -> QueryResult<()> {
    let tokens = lex_insecure(update.as_bytes()).unwrap();
    let insert: UpdateStatement = ast::parse_ast_node_full(&tokens[1..]).unwrap();
    dml::update(global, insert)
}

fn auto_hook<T>(msg: &str, f: impl Fn() -> T) -> T {
    let hook = std::panic::take_hook();
    let decl_owned = msg.to_owned();
    std::panic::set_hook(Box::new(move |pinfo| {
        eprintln!("panic due to `{decl_owned}`: {pinfo}")
    }));
    let r = f();
    std::panic::set_hook(hook);
    r
}

fn create_and_close(log_name: &str, decl: &str) {
    auto_hook(decl, || {
        test_utils::with_variable(log_name, |log_name| {
            // create and close
            {
                let global = TestGlobal::new_with_driver_id(log_name);
                let _ = create_model_and_space(&global, decl).unwrap();
            }
            // open
            {
                let global = TestGlobal::new_with_driver_id(log_name);
                drop(global);
            }
        })
    })
}

fn run_sample_inserts<K, V>(
    log_name: &str,
    decl: &str,
    key_values: Vec<(K, V)>,
    make_insert_query: impl Fn(&K, &V) -> String,
    as_pk: for<'a> fn(&'a K) -> Lit<'a>,
    check_row: impl Fn(&K, &V, &RowData),
) {
    auto_hook(decl, || {
        test_utils::with_variable(log_name, |log_name| {
            // create, insert and close
            let mdl_name;
            {
                let mut global = TestGlobal::new_with_driver_id(log_name);
                global.set_max_data_pressure(key_values.len());
                mdl_name = create_model_and_space(&global, decl).unwrap();
                for (username, password) in key_values.iter() {
                    run_insert(&global, &make_insert_query(username, password)).unwrap();
                }
            }
            // reopen and verify 100 times
            test_utils::multi_run(100, || {
                let global = TestGlobal::new_with_driver_id(log_name);
                global
                    .state()
                    .namespace()
                    .with_model(
                        EntityIDRef::new(mdl_name.space(), mdl_name.entity()),
                        |model| {
                            let g = pin();
                            for (username, password) in key_values.iter() {
                                let row = model
                                    .primary_index()
                                    .select(as_pk(username), &g)
                                    .unwrap()
                                    .d_data()
                                    .read();
                                check_row(username, password, &row)
                            }
                            Ok(())
                        },
                    )
                    .unwrap()
            })
        })
    })
}

fn run_sample_updates<K, V>(
    log_name: &str,
    decl: &str,
    key_values: Vec<(K, V)>,
    make_insert_query: impl Fn(&K, &V) -> String,
    make_update_query: impl Fn(&K, &V) -> String,
    as_pk: for<'a> fn(&'a K) -> Lit<'a>,
    check_row: impl Fn(&K, &V, &RowData),
) {
    auto_hook(decl, || {
        test_utils::with_variable((log_name, TEST_UPDATE_DATASET_SIZE), |(log_name, n)| {
            /*
                - we first open the log and then insert n values
                - we then reopen the log 100 times, changing n / 100 values every time (we set the string to an empty one)
                - we finally reopen the log and check if all the keys have empty string as the password
            */
            let mdl_name;
            {
                // insert n values
                let mut global = TestGlobal::new_with_driver_id(log_name);
                global.set_max_data_pressure(n);
                mdl_name = create_model_and_space(&global, decl).unwrap();
                for (username, password) in key_values.iter() {
                    run_insert(&global, &make_insert_query(username, password)).unwrap();
                }
            }
            {
                // reopen and update multiple times
                // this effectively opens the log 100 times
                let changes_per_cycle = n / 10;
                let reopen_count = n / changes_per_cycle;
                // now update values
                let mut actual_position = 0;
                for _ in 0..reopen_count {
                    let mut global = TestGlobal::new_with_driver_id(log_name);
                    global.set_max_data_pressure(changes_per_cycle);
                    let mut j = 0;
                    for _ in 0..changes_per_cycle {
                        let (username, pass) = &key_values[actual_position];
                        run_update(&global, &make_update_query(username, pass)).unwrap();
                        actual_position += 1;
                        j += 1;
                    }
                    assert_eq!(j, changes_per_cycle);
                    drop(global);
                }
                assert_eq!(actual_position, n);
            }
            {
                let global = TestGlobal::new_with_driver_id(log_name);
                for (txn_id, (username, password)) in key_values
                    .iter()
                    .enumerate()
                    .map(|(i, x)| ((i + n) as u64, x))
                {
                    global
                        .state()
                        .namespace()
                        .with_model(
                            EntityIDRef::new(mdl_name.space(), mdl_name.entity()),
                            |model| {
                                let g = pin();
                                let row = model
                                    .primary_index()
                                    .select(as_pk(username), &g)
                                    .unwrap()
                                    .d_data()
                                    .read();
                                check_row(username, password, &row);
                                assert_eq!(row.get_txn_revised().value_u64(), txn_id);
                                Ok(())
                            },
                        )
                        .unwrap();
                }
            }
        })
    })
}

/*
    test runs
*/

#[test]
fn empty_model_data() {
    create_and_close(
        "empty_model_data_variable_index_key",
        "create model milky_way.solar_system(planet_name: string, population: uint64)",
    );
    create_and_close(
        "empty_model_data_fixed_index_key",
        "create model milky_way.solar_system(planet_id: uint64, population: uint64)",
    );
}

#[test]
fn model_data_inserts() {
    run_sample_inserts(
        "model_data_inserts_variable_pk",
        "create model apps.social(user_name: string, password: string)",
        create_test_kv_strings(TEST_DATASET_SIZE),
        |k, v| format!("insert into apps.social('{k}', '{v}')"),
        |k| Lit::new_str(k),
        |_, v, row| assert_eq!(row.fields().get("password").unwrap().str(), v),
    );
    run_sample_inserts(
        "model_data_inserts_fixed_pk",
        "create model apps.social(user_id: uint64, password: string)",
        create_test_kv_int(TEST_DATASET_SIZE),
        |k, v| format!("insert into apps.social({k}, '{v}')"),
        |k| Lit::new_uint(*k),
        |_, v, row| assert_eq!(row.fields().get("password").unwrap().str(), v),
    )
}

#[test]
fn model_data_updates() {
    run_sample_updates(
        "model_data_updates_variable_key",
        "create model apps.social(user_name: string, password: string)",
        create_test_kv_strings(TEST_UPDATE_DATASET_SIZE),
        |k, v| format!("insert into apps.social('{k}', '{v}')"),
        |k, _| format!("update apps.social set password = '' where user_name = '{k}'"),
        |k| Lit::new_str(k),
        |username, _, row| {
            let pass = row.fields().get("password").unwrap().str();
            assert!(
                pass.is_empty(),
                "failed for {username} because pass is {pass}",
            );
        },
    );
    run_sample_updates(
        "model_data_updates_fixed_key",
        "create model apps.social(user_name: uint64, password: string)",
        create_test_kv_int(TEST_UPDATE_DATASET_SIZE),
        |k, v| format!("insert into apps.social({k}, '{v}')"),
        |k, _| format!("update apps.social set password = '' where user_name = {k}"),
        |k| Lit::new_uint(*k),
        |username, _, row| {
            let pass = row.fields().get("password").unwrap().str();
            assert!(
                pass.is_empty(),
                "failed for {username} because pass is {pass}",
            );
        },
    );
}
