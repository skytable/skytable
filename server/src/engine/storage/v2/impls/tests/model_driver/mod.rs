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
            core::{dml, model::ModelData, space::Space, EntityID, EntityIDRef},
            error::QueryResult,
            fractal::{test_utils::TestGlobal, GlobalInstanceLike},
            idx::MTIndex,
            ql::{
                ast::{self, traits::ASTNode},
                ddl::crt::{CreateModel, CreateSpace},
                dml::{ins::InsertStatement, upd::UpdateStatement},
                tests::lex_insecure,
            },
            storage::common::interface::fs::{FSContext, FileSystem},
        },
        util,
    },
    std::collections::BTreeMap,
};

mod compaction_test;
mod generic;
mod skew;

/*
    utils
*/

const TEST_DATASET_SIZE: usize = if cfg!(miri) { 10 } else { 1000 };
const TEST_UPDATE_DATASET_SIZE: usize = 8200; // this peculiar size to force the buffer to flush

fn create_test_kv_strings(count: usize) -> Vec<(String, String)> {
    (1..=count).map(|i| create_test_kv(i, count)).collect()
}

fn create_test_kv(i: usize, width: usize) -> (String, String) {
    (
        format!("user-{i:0>width$}"),
        format!("password-{i:0>width$}"),
    )
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

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn truncate() {
    FileSystem::set_context(FSContext::Local);
    const LOG_NAME: &str = "truncate_log.db";
    let _fs = FileSystem::with_files([LOG_NAME]);
    {
        let mut global = TestGlobal::new_with_driver_id(LOG_NAME);
        global.set_max_data_pressure(0);
        create_model_and_space(
            &global,
            "create model truncation.entries(k: string, v: string)",
        )
        .unwrap();
        for num in 1u64..=10 {
            run_insert(
                &global,
                &format!("insert into truncation.entries('k-{num}', 'v-{num}')"),
            )
            .unwrap();
        }
        {
            let truncate_query =
                lex_insecure("truncate model truncation.entries".as_bytes()).unwrap();
            let truncate_query = ASTNode::from_insecure_tokens_full(&truncate_query[1..]).unwrap();
            dml::truncate(&global, truncate_query).unwrap();
        }
        assert_eq!(
            global
                .state()
                .namespace()
                .with_model(EntityIDRef::new("truncation", "entries"), |mdl| {
                    Ok(mdl.primary_index().count())
                })
                .unwrap(),
            0
        );
    }
    {
        let mut global = TestGlobal::new_with_driver_id(LOG_NAME);
        global.set_max_data_pressure(0);
        assert_eq!(
            global
                .state()
                .namespace()
                .with_model(EntityIDRef::new("truncation", "entries"), |mdl| {
                    Ok(mdl.primary_index().count())
                })
                .unwrap(),
            0
        );
        for num in 11u64..=20 {
            run_insert(
                &global,
                &format!("insert into truncation.entries('k-{num}', 'v-{num}')"),
            )
            .unwrap();
        }
    }
    util::test_utils::multi_run(if cfg!(miri) { 2 } else { 100 }, || {
        let global = TestGlobal::new_with_driver_id(LOG_NAME);
        assert_eq!(
            global
                .state()
                .namespace()
                .with_model(EntityIDRef::new("truncation", "entries"), |mdl| {
                    let mut entries = BTreeMap::new();
                    mdl.primary_index()
                        .__raw_index()
                        .mt_iter_kv(&crossbeam_epoch::pin())
                        .map(|(k, row)| {
                            (
                                k.str().unwrap().to_string(),
                                row.read()
                                    .fields()
                                    .into_iter()
                                    .map(|(_, dc)| dc.str().to_string())
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .for_each(|(k, v)| {
                            entries.insert(k, v);
                        });
                    Ok(entries)
                })
                .unwrap(),
            (11..=20)
                .map(|i| (format!("k-{i}"), vec![format!("v-{i}")]))
                .collect()
        );
    });
}
