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
                dml::ins::InsertStatement,
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

#[test]
fn model_data_deltas() {
    test_utils::with_variable(("model_data_deltas", 1000), |(log_name, change_count)| {
        // create, insert and close
        {
            let mut global = TestGlobal::new_with_vfs_driver(log_name);
            global.set_max_data_pressure(change_count);
            let _ = create_model_and_space(
                &global,
                "create model apps.social(user_name: string, password: string)",
            )
            .unwrap();
            for i in 1..=change_count {
                run_insert(
                    &global,
                    &format!("insert into apps.social('user-{i:0>1000}', 'password-{i:0>1000}')"),
                )
                .unwrap();
            }
        }
        // reopen and verify a 100 times
        test_utils::multi_run(100, || {
            let global = TestGlobal::new_with_vfs_driver(log_name);
            global.load_model_drivers().unwrap();
            global
                .state()
                .with_model(EntityIDRef::new("apps", "social"), |model| {
                    let g = pin();
                    for i in 1..=change_count {
                        assert_eq!(
                            model
                                .primary_index()
                                .select(Lit::new_string(format!("user-{i:0>1000}")), &g)
                                .unwrap()
                                .d_data()
                                .read()
                                .fields()
                                .get("password")
                                .cloned()
                                .unwrap()
                                .into_str()
                                .unwrap(),
                            format!("password-{i:0>1000}")
                        )
                    }
                    Ok(())
                })
                .unwrap()
        })
    })
}
