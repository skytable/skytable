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
            core::{dml, model::Model, space::Space, EntityIDRef},
            data::lit::Lit,
            error::QueryResult,
            fractal::{test_utils::TestGlobal, GlobalInstanceLike},
            ql::{
                ast,
                ddl::crt::{CreateModel, CreateSpace},
                dml::{ins::InsertStatement, upd::UpdateStatement},
                tests::lex_insecure,
            },
            storage::common::interface::fs_test::VirtualFS,
        },
        util::test_utils,
    },
    crossbeam_epoch::pin,
};

fn create_model_and_space(global: &TestGlobal<VirtualFS>, create_model: &str) -> QueryResult<()> {
    let tokens = lex_insecure(create_model.as_bytes()).unwrap();
    let create_model: CreateModel = ast::parse_ast_node_full(&tokens[2..]).unwrap();
    // first create space
    let create_space_str = format!("create space {}", create_model.model_name.space());
    let create_space_tokens = lex_insecure(create_space_str.as_bytes()).unwrap();
    let create_space: CreateSpace = ast::parse_ast_node_full(&create_space_tokens[2..]).unwrap();
    Space::transactional_exec_create(global, create_space)?;
    Model::transactional_exec_create(global, create_model).map(|_| ())
}

fn run_insert(global: &TestGlobal<VirtualFS>, insert: &str) -> QueryResult<()> {
    let tokens = lex_insecure(insert.as_bytes()).unwrap();
    let insert: InsertStatement = ast::parse_ast_node_full(&tokens[1..]).unwrap();
    dml::insert(global, insert)
}

fn run_update(global: &TestGlobal<VirtualFS>, update: &str) -> QueryResult<()> {
    let tokens = lex_insecure(update.as_bytes()).unwrap();
    let insert: UpdateStatement = ast::parse_ast_node_full(&tokens[1..]).unwrap();
    dml::update(global, insert)
}

#[test]
fn empty_model_data() {
    test_utils::with_variable("empty_model_data", |log_name| {
        // create and close
        {
            let global = TestGlobal::new_with_vfs_driver(log_name);
            let _ = create_model_and_space(
                &global,
                "create model milky_way.solar_system(planet_name: string, population: uint64)",
            )
            .unwrap();
        }
        // open
        {
            let global = TestGlobal::new_with_vfs_driver(log_name);
            drop(global);
        }
    })
}

fn create_test_kv(change_count: usize) -> Vec<(String, String)> {
    (1..=change_count)
        .map(|i| {
            (
                format!("user-{i:0>change_count$}"),
                format!("password-{i:0>change_count$}"),
            )
        })
        .collect()
}

#[test]
fn model_data_inserts() {
    test_utils::with_variable(("model_data_inserts", 1000), |(log_name, change_count)| {
        let key_values = create_test_kv(change_count);
        // create, insert and close
        {
            let mut global = TestGlobal::new_with_vfs_driver(log_name);
            global.set_max_data_pressure(change_count);
            let _ = create_model_and_space(
                &global,
                "create model apps.social(user_name: string, password: string)",
            )
            .unwrap();
            for (username, password) in key_values.iter() {
                run_insert(
                    &global,
                    &format!("insert into apps.social('{username}', '{password}')"),
                )
                .unwrap();
            }
        }
        // reopen and verify 100 times
        test_utils::multi_run(100, || {
            let global = TestGlobal::new_with_vfs_driver(log_name);
            global.load_model_drivers().unwrap();
            global
                .state()
                .with_model(EntityIDRef::new("apps", "social"), |model| {
                    let g = pin();
                    for (username, password) in key_values.iter() {
                        assert_eq!(
                            model
                                .primary_index()
                                .select(Lit::new_str(username.as_str()), &g)
                                .unwrap()
                                .d_data()
                                .read()
                                .fields()
                                .get("password")
                                .unwrap()
                                .str(),
                            password.as_str()
                        )
                    }
                    Ok(())
                })
                .unwrap()
        })
    })
}

#[test]
fn model_data_updates() {
    test_utils::with_variable(("model_data_updates", 8200), |(log_name, n)| {
        let key_values = create_test_kv(n);
        /*
            - we first open the log and then insert n values
            - we then reopen the log 100 times, changing n / 100 values every time (we set the string to an empty one)
            - we finally reopen the log and check if all the keys have empty string as the password
        */
        {
            // insert n values
            let mut global = TestGlobal::new_with_vfs_driver(log_name);
            global.set_max_data_pressure(n);
            let _ = create_model_and_space(
                &global,
                "create model apps.social(user_name: string, password: string)",
            )
            .unwrap();
            for (username, password) in key_values.iter() {
                run_insert(
                    &global,
                    &format!("insert into apps.social('{username}', '{password}')"),
                )
                .unwrap();
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
                let mut global = TestGlobal::new_with_vfs_driver(log_name);
                global.set_max_data_pressure(changes_per_cycle);
                global.load_model_drivers().unwrap();
                let mut j = 0;
                for _ in 0..changes_per_cycle {
                    let (username, _) = &key_values[actual_position];
                    run_update(
                        &global,
                        &format!(
                            "update apps.social set password = '' where user_name = '{username}'"
                        ),
                    )
                    .unwrap();
                    actual_position += 1;
                    j += 1;
                }
                assert_eq!(j, changes_per_cycle);
                drop(global);
            }
            assert_eq!(actual_position, n);
        }
        {
            let global = TestGlobal::new_with_vfs_driver(log_name);
            global.load_model_drivers().unwrap();
            for (txn_id, (username, _)) in key_values
                .iter()
                .enumerate()
                .map(|(i, x)| ((i + n) as u64, x))
            {
                global
                    .state()
                    .with_model(EntityIDRef::new("apps", "social"), |model| {
                        let g = pin();
                        let row = model
                            .primary_index()
                            .select(Lit::new_str(username.as_str()), &g)
                            .unwrap()
                            .d_data()
                            .read();
                        let pass = row.fields().get("password").unwrap().str();
                        assert!(
                            pass.is_empty(),
                            "failed for {username} because pass is {pass}",
                        );
                        assert_eq!(row.get_txn_revised().value_u64(), txn_id);
                        Ok(())
                    })
                    .unwrap();
            }
        }
    })
}
