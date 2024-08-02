/*
 * Created on Sun Apr 21 2024
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
    crate::engine::{
        core::EntityIDRef,
        data::lit::Lit,
        fractal::{test_utils::TestGlobal, GlobalInstanceLike},
        storage::{
            common::{
                interface::fs::{FSContext, FileSystem},
                paths_v1,
            },
            v2::{
                impls::mdl_journal::{self, BatchInfo},
                raw::journal,
            },
        },
    },
    crossbeam_epoch::pin,
    std::thread,
};

const DATASET_SIZE: usize = if cfg!(miri) { 10 } else { 1000 };

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn compaction_test() {
    FileSystem::set_context(FSContext::Local);
    let mut fs = FileSystem::instance();
    fs.mark_file_for_removal("compaction_test_model");
    let driver_path;
    {
        /*
            create a model and apply 2000 events to it, with 1:1 redundancy ratio
        */
        let global = TestGlobal::new_with_driver_id_instant_update("compaction_test_model");
        super::create_model_and_space(
            &global,
            "create model compaction_test_model.compaction_test_model(username: string, password: string)",
        )
        .unwrap();
        for (key, val) in super::create_test_kv_strings(DATASET_SIZE) {
            super::run_insert(
                &global,
                &format!(
                    "insert into compaction_test_model.compaction_test_model('{key}', '{val}')"
                ),
            )
            .unwrap();
            super::run_update(
                &global,
                &format!("update compaction_test_model.compaction_test_model set password = 'password' where username = '{key}'"),
            )
            .unwrap()
        }
        assert_eq!(global.get_net_commited_events(), DATASET_SIZE * 2);
        /*
            get the model driver and compact it
        */
        {
            let space_uuid = global
                .state()
                .namespace()
                .idx()
                .read()
                .get("compaction_test_model")
                .unwrap()
                .get_uuid();
            let mut idx_models = global.state().namespace().idx_models().write();
            let mdl = idx_models
                .get_mut(&EntityIDRef::new(
                    "compaction_test_model",
                    "compaction_test_model",
                ))
                .unwrap();
            let mut driver = mdl.driver().batch_driver().lock();
            let orig_driver = driver.take();
            // now we want to compact this
            driver_path = paths_v1::model_path(
                "compaction_test_model",
                space_uuid,
                "compaction_test_model",
                mdl.data().get_uuid(),
            );
            let new_jrnl =
                journal::compact_journal::<true, _>(&driver_path, orig_driver.unwrap(), mdl.data())
                    .unwrap();
            let _ = driver.replace(new_jrnl);
        }
        /*
            write two more events (net = 1002) (ref (1) and (2))
        */
        for (k, v) in
            (DATASET_SIZE + 1..=DATASET_SIZE + 2).map(|i| super::create_test_kv(i, DATASET_SIZE))
        {
            super::run_insert(
                &global,
                &format!("insert into compaction_test_model.compaction_test_model('{k}', '{v}')"),
            )
            .unwrap();
        }
        assert_eq!(global.get_net_commited_events(), (DATASET_SIZE * 2) + 2);
        drop(global);
    }
    /*
        reopen the model and verify data. separate thread to ensure new local state
    */
    let global = thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            FileSystem::set_context(FSContext::Local);
            let global = TestGlobal::new_with_driver_id("compaction_test_model");
            let last_batch_runs = mdl_journal::get_last_batch_run_info();
            assert_eq!(
                last_batch_runs,
                vec![
                    BatchInfo {
                        items_count: DATASET_SIZE,
                        redundant_count: 0
                    }, // we consolidated this one
                    BatchInfo {
                        items_count: 1,
                        redundant_count: 0
                    }, // this was after the consolidation (1)
                    BatchInfo {
                        items_count: 1,
                        redundant_count: 0
                    }, // this was after the consolidation (2)
                ]
            );
            global
        })
        .unwrap()
        .join()
        .unwrap();
    let models = global.state().namespace().idx_models().read();
    let mdl = models
        .get(&EntityIDRef::new(
            "compaction_test_model",
            "compaction_test_model",
        ))
        .unwrap();
    assert_eq!(mdl.data().primary_index().count(), DATASET_SIZE + 2);
    let pin = pin();
    /*
        verify the pre compaction rows
    */
    for (key, _) in super::create_test_kv_strings(DATASET_SIZE) {
        let row = mdl
            .data()
            .primary_index()
            .select(Lit::new_str(&key), &pin)
            .unwrap()
            .d_data()
            .read();
        assert_eq!(
            row.fields()
                .get("password")
                .unwrap()
                .clone()
                .into_str()
                .unwrap(),
            "password"
        );
        // alll pre compaction rows are set to txn id 0
        assert_eq!(row.get_txn_revised().value_u64(), 0);
    }
    /*
        verify the post compaction rows
    */
    let kv_1001 = super::create_test_kv(DATASET_SIZE + 1, DATASET_SIZE);
    let kv_1002 = super::create_test_kv(DATASET_SIZE + 2, DATASET_SIZE);
    let row_1001 = mdl
        .data()
        .primary_index()
        .select(Lit::new_str(&kv_1001.0), &pin)
        .unwrap()
        .d_data()
        .read();
    assert_eq!(
        row_1001
            .fields()
            .get("password")
            .unwrap()
            .clone()
            .into_str()
            .unwrap(),
        kv_1001.1
    );
    assert_eq!(row_1001.get_txn_revised().value_u64(), 1);
    let row_1002 = mdl
        .data()
        .primary_index()
        .select(Lit::new_str(&kv_1002.0), &pin)
        .unwrap()
        .d_data()
        .read();
    assert_eq!(
        row_1002
            .fields()
            .get("password")
            .unwrap()
            .clone()
            .into_str()
            .unwrap(),
        kv_1002.1
    );
    assert_eq!(row_1002.get_txn_revised().value_u64(), 2);
    assert_eq!(
        mdl.data().delta_state().data_current_version().value_u64(),
        3
    ); // this is important! the next row must get (compaction:id0, insert(row1001):id1, insert(row1002):id2) id3
}
