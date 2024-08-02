/*
 * Created on Mon Apr 15 2024
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
            core::{index::RowData, EntityIDRef},
            data::lit::Lit,
            fractal::{test_utils::TestGlobal, GlobalInstanceLike},
            storage::common::interface::fs::{FSContext, FileSystem},
        },
        util::test_utils,
    },
    crossbeam_epoch::pin,
};

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
                let _ = super::create_model_and_space(&global, decl).unwrap();
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
                mdl_name = super::create_model_and_space(&global, decl).unwrap();
                for (username, password) in key_values.iter() {
                    super::run_insert(&global, &make_insert_query(username, password)).unwrap();
                }
            }
            // reopen and verify 100 times
            test_utils::multi_run(if cfg!(miri) { 2 } else { 100 }, || {
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
        test_utils::with_variable(
            (log_name, super::TEST_UPDATE_DATASET_SIZE),
            |(log_name, n)| {
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
                    mdl_name = super::create_model_and_space(&global, decl).unwrap();
                    for (username, password) in key_values.iter() {
                        super::run_insert(&global, &make_insert_query(username, password)).unwrap();
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
                            super::run_update(&global, &make_update_query(username, pass)).unwrap();
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
            },
        )
    })
}

/*
    test runs
*/

#[sky_macros::test]
fn empty_model_data() {
    FileSystem::set_context(FSContext::Local);
    let mut fs = FileSystem::instance();
    fs.mark_file_for_removal("empty_model_data_variable_index_key");
    fs.mark_file_for_removal("empty_model_data_fixed_index_key");
    create_and_close(
        "empty_model_data_variable_index_key",
        "create model milky_way.solar_system(planet_name: string, population: uint64)",
    );
    create_and_close(
        "empty_model_data_fixed_index_key",
        "create model milky_way.solar_system(planet_id: uint64, population: uint64)",
    );
}

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn model_data_inserts() {
    FileSystem::set_context(FSContext::Local);
    let mut fs = FileSystem::instance();
    fs.mark_file_for_removal("model_data_inserts_variable_pk");
    fs.mark_file_for_removal("model_data_inserts_fixed_pk");
    run_sample_inserts(
        "model_data_inserts_variable_pk",
        "create model apps.social(user_name: string, password: string)",
        super::create_test_kv_strings(super::TEST_DATASET_SIZE),
        |k, v| format!("insert into apps.social('{k}', '{v}')"),
        |k| Lit::new_str(k),
        |_, v, row| assert_eq!(row.fields().get("password").unwrap().str(), v),
    );
    run_sample_inserts(
        "model_data_inserts_fixed_pk",
        "create model apps.social(user_id: uint64, password: string)",
        super::create_test_kv_int(super::TEST_DATASET_SIZE),
        |k, v| format!("insert into apps.social({k}, '{v}')"),
        |k| Lit::new_uint(*k),
        |_, v, row| assert_eq!(row.fields().get("password").unwrap().str(), v),
    )
}

#[sky_macros::non_miri_test] // FIXME(@ohsayan): takes forever in miri. no need
#[cfg(not(all(target_os = "windows", target_pointer_width = "32")))]
fn model_data_updates() {
    FileSystem::set_context(FSContext::Local);
    let mut fs = FileSystem::instance();
    fs.mark_file_for_removal("model_data_updates_variable_key");
    fs.mark_file_for_removal("model_data_updates_fixed_key");
    run_sample_updates(
        "model_data_updates_variable_key",
        "create model apps.social(user_name: string, password: string)",
        super::create_test_kv_strings(super::TEST_UPDATE_DATASET_SIZE),
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
        super::create_test_kv_int(super::TEST_UPDATE_DATASET_SIZE),
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
