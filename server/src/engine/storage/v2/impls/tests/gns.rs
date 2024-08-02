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

use std::thread;

use crate::engine::{
    core::{dcl, model::ModelData, space::Space, system_db::VerifyUser, EntityIDRef},
    fractal::{test_utils::TestGlobal, GlobalInstanceLike},
    idx::STIndex,
    net::protocol::ClientLocalState,
    storage::{
        common::interface::fs::{FSContext, FileSystem},
        v2::{
            impls::gns_log::{self, ReadEventTracing},
            raw::journal,
        },
        GNSDriver,
    },
    txn::gns::sysctl::AlterUserTxn,
};

#[sky_macros::non_miri_test] // FIXME(@ohsayan): massive slowdown with miri..not sure why
fn compaction_test() {
    FileSystem::set_context(FSContext::Local);
    let mut fs = FileSystem::instance();
    fs.mark_file_for_removal("compaction_test_gns");
    let global = TestGlobal::new_with_driver_id("compaction_test_gns");
    // create a space
    super::exec(
        &global,
        "create space myspace",
        Space::transactional_exec_create,
    )
    .unwrap();
    // create a model and alter
    super::exec(
        &global,
        "create model myspace.mymodel(username: string, password: string, phone: uint64)",
        ModelData::transactional_exec_create,
    )
    .unwrap();
    super::exec(
        &global,
        "alter model myspace.mymodel update phone { nullable: true }",
        ModelData::transactional_exec_alter,
    )
    .unwrap();
    // create an user and alter
    super::exec_step(
        &global,
        "sysctl create user sayan with { password: 'mypassword12345678' }",
        1,
        |g, n| dcl::exec_ref(g, &ClientLocalState::test_new("root", true), n),
    )
    .unwrap();
    super::exec_step(
        &global,
        "sysctl alter user sayan with { password: 'mypassword23456789' }",
        1,
        |g, n| dcl::exec_ref(g, &ClientLocalState::test_new("root", true), n),
    )
    .unwrap();
    assert_eq!(gns_log::get_executed_event_count(), 5);
    {
        // now shut down global
        let (gns_data, old_driver) = global.finish_into_driver();
        // compact journal
        let mut new_jrnl =
            journal::compact_journal::<true, _>("compaction_test_gns", old_driver, &gns_data)
                .unwrap();
        assert_eq!(
            gns_log::get_executed_event_count(),
            3, // create space, create model, create user
        );
        // commit this event
        new_jrnl
            .commit_event(AlterUserTxn::new(
                "sayan",
                &rcrypt::hash(
                    "hickory dickory dock, the mouse didn't go up the clock",
                    rcrypt::DEFAULT_COST,
                )
                .unwrap(),
            ))
            .unwrap();
        assert_eq!(gns_log::get_executed_event_count(), 1);
        // close
        GNSDriver::close_driver(&mut new_jrnl).unwrap();
    }
    let tg = thread::spawn(|| {
        FileSystem::set_context(FSContext::Local);
        let tg = TestGlobal::new_with_driver_id("compaction_test_gns");
        assert_eq!(
            gns_log::get_tracing(),
            ReadEventTracing {
                total: 4,  // create space, create model, create user, alter user (post compaction)
                repeat: 1, // the last alter after compaction
            }
        );
        tg
    })
    .join()
    .unwrap();
    assert_eq!(
        tg.state().namespace().sys_db().verify_user(
            "sayan",
            b"hickory dickory dock, the mouse didn't go up the clock"
        ),
        VerifyUser::Okay
    );
    assert!(tg.state().namespace().idx().read().contains_key("myspace"));
    assert!(tg
        .state()
        .namespace()
        .idx_models()
        .read()
        .get(&EntityIDRef::new("myspace", "mymodel"))
        .unwrap()
        .data()
        .fields()
        .st_get("phone")
        .unwrap()
        .is_nullable());
}
