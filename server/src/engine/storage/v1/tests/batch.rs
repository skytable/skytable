/*
 * Created on Wed Sep 06 2023
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

use {
    crate::{
        engine::{
            core::{
                index::{DcFieldIndex, PrimaryIndexKey, Row},
                model::{
                    delta::{DataDelta, DataDeltaKind, DeltaVersion},
                    Field, Layer, Model,
                },
            },
            data::{cell::Datacell, tag::TagSelector, uuid::Uuid},
            idx::MTIndex,
            storage::{
                common::interface::{fs_test::VirtualFS, fs_traits::FileOpen},
                v1::{
                    batch_jrnl::{
                        DataBatchPersistDriver, DataBatchRestoreDriver, DecodedBatchEvent,
                        DecodedBatchEventKind, NormalBatch,
                    },
                    rw::SDSSFileIO,
                    spec,
                },
            },
        },
        util::test_utils,
    },
    crossbeam_epoch::pin,
};

fn pkey(v: impl Into<Datacell>) -> PrimaryIndexKey {
    PrimaryIndexKey::try_from_dc(v.into()).unwrap()
}

fn open_file(
    fpath: &str,
) -> FileOpen<SDSSFileIO<VirtualFS>, (SDSSFileIO<VirtualFS>, super::super::Header)> {
    SDSSFileIO::open_or_create_perm_rw::<spec::DataBatchJournalV1>(fpath).unwrap()
}

fn open_batch_data(fpath: &str, mdl: &Model) -> DataBatchPersistDriver<VirtualFS> {
    match open_file(fpath) {
        FileOpen::Created(f) => DataBatchPersistDriver::new(f, true),
        FileOpen::Existing((f, _header)) => {
            let mut dbr = DataBatchRestoreDriver::new(f).unwrap();
            dbr.read_data_batch_into_model(mdl).unwrap();
            DataBatchPersistDriver::new(dbr.into_file().unwrap(), false)
        }
    }
    .unwrap()
}

fn new_delta(
    schema: u64,
    txnid: u64,
    pk: impl Into<Datacell>,
    data: DcFieldIndex,
    change: DataDeltaKind,
) -> DataDelta {
    new_delta_with_row(
        txnid,
        Row::new(
            pkey(pk),
            data,
            DeltaVersion::__new(schema),
            DeltaVersion::__new(txnid),
        ),
        change,
    )
}

fn new_delta_with_row(txnid: u64, row: Row, change: DataDeltaKind) -> DataDelta {
    DataDelta::new(DeltaVersion::__new(txnid), row, change)
}

fn flush_deltas_and_re_read<const N: usize>(
    mdl: &Model,
    dt: [DataDelta; N],
    fname: &str,
) -> Vec<NormalBatch> {
    let mut restore_driver = flush_batches_and_return_restore_driver(dt, mdl, fname);
    let batch = restore_driver.read_all_batches().unwrap();
    batch
}

fn flush_batches_and_return_restore_driver<const N: usize>(
    dt: [DataDelta; N],
    mdl: &Model,
    fname: &str,
) -> DataBatchRestoreDriver<VirtualFS> {
    // delta queue
    let g = pin();
    for delta in dt {
        mdl.delta_state().append_new_data_delta(delta, &g);
    }
    let file = open_file(fname).into_created().unwrap();
    {
        let mut persist_driver = DataBatchPersistDriver::new(file, true).unwrap();
        persist_driver.write_new_batch(&mdl, N).unwrap();
        persist_driver.close().unwrap();
    }
    DataBatchRestoreDriver::new(open_file(fname).into_existing().unwrap().0).unwrap()
}

#[test]
fn empty_multi_open_reopen() {
    let uuid = Uuid::new();
    let mdl = Model::new_restore(
        uuid,
        "username".into(),
        TagSelector::String.into_full(),
        into_dict!(
            "username" => Field::new([Layer::str()].into(), false),
            "password" => Field::new([Layer::bin()].into(), false)
        ),
    );
    for _ in 0..100 {
        let writer = open_batch_data("empty_multi_open_reopen.db-btlog", &mdl);
        writer.close().unwrap();
    }
}

#[test]
fn unskewed_delta() {
    let uuid = Uuid::new();
    let mdl = Model::new_restore(
        uuid,
        "username".into(),
        TagSelector::String.into_full(),
        into_dict!(
            "username" => Field::new([Layer::str()].into(), false),
            "password" => Field::new([Layer::bin()].into(), false)
        ),
    );
    let deltas = [
        new_delta(
            0,
            0,
            "sayan",
            into_dict!("password" => Datacell::new_bin("37ae4b773a9fc7a20164eb16".as_bytes().into())),
            DataDeltaKind::Insert,
        ),
        new_delta(
            0,
            1,
            "badguy",
            into_dict!("password" => Datacell::new_bin("5fe3cbdc470b667cb1ba288a".as_bytes().into())),
            DataDeltaKind::Insert,
        ),
        new_delta(
            0,
            2,
            "doggo",
            into_dict!("password" => Datacell::new_bin("c80403f9d0ae4d5d0e829dd0".as_bytes().into())),
            DataDeltaKind::Insert,
        ),
        new_delta(0, 3, "badguy", into_dict!(), DataDeltaKind::Delete),
    ];
    let batches = flush_deltas_and_re_read(&mdl, deltas, "unskewed_delta.db-btlog");
    assert_eq!(
        batches,
        vec![NormalBatch::new(
            vec![
                DecodedBatchEvent::new(
                    0,
                    pkey("sayan"),
                    DecodedBatchEventKind::Insert(vec![Datacell::new_bin(
                        b"37ae4b773a9fc7a20164eb16".to_vec().into_boxed_slice()
                    )])
                ),
                DecodedBatchEvent::new(
                    1,
                    pkey("badguy"),
                    DecodedBatchEventKind::Insert(vec![Datacell::new_bin(
                        b"5fe3cbdc470b667cb1ba288a".to_vec().into_boxed_slice()
                    )])
                ),
                DecodedBatchEvent::new(
                    2,
                    pkey("doggo"),
                    DecodedBatchEventKind::Insert(vec![Datacell::new_bin(
                        b"c80403f9d0ae4d5d0e829dd0".to_vec().into_boxed_slice()
                    )])
                ),
                DecodedBatchEvent::new(3, pkey("badguy"), DecodedBatchEventKind::Delete)
            ],
            0
        )]
    )
}

#[test]
fn skewed_delta() {
    // prepare model definition
    let uuid = Uuid::new();
    let mdl = Model::new_restore(
        uuid,
        "catname".into(),
        TagSelector::String.into_full(),
        into_dict!(
            "catname" => Field::new([Layer::str()].into(), false),
            "is_good" => Field::new([Layer::bool()].into(), false),
            "magical" => Field::new([Layer::bool()].into(), false),
        ),
    );
    let row = Row::new(
        pkey("Schrödinger's cat"),
        into_dict!("is_good" => Datacell::new_bool(true), "magical" => Datacell::new_bool(false)),
        DeltaVersion::__new(0),
        DeltaVersion::__new(2),
    );
    {
        // update the row
        let mut wl = row.d_data().write();
        wl.set_txn_revised(DeltaVersion::__new(3));
        *wl.fields_mut().get_mut("magical").unwrap() = Datacell::new_bool(true);
    }
    // prepare deltas
    let deltas = [
        // insert catname: Schrödinger's cat, is_good: true
        new_delta_with_row(0, row.clone(), DataDeltaKind::Insert),
        // insert catname: good cat, is_good: true, magical: false
        new_delta(
            0,
            1,
            "good cat",
            into_dict!("is_good" => Datacell::new_bool(true), "magical" => Datacell::new_bool(false)),
            DataDeltaKind::Insert,
        ),
        // insert catname: bad cat, is_good: false, magical: false
        new_delta(
            0,
            2,
            "bad cat",
            into_dict!("is_good" => Datacell::new_bool(false), "magical" => Datacell::new_bool(false)),
            DataDeltaKind::Insert,
        ),
        // update catname: Schrödinger's cat, is_good: true, magical: true
        new_delta_with_row(3, row.clone(), DataDeltaKind::Update),
    ];
    let batch = flush_deltas_and_re_read(&mdl, deltas, "skewed_delta.db-btlog");
    assert_eq!(
        batch,
        vec![NormalBatch::new(
            vec![
                DecodedBatchEvent::new(
                    1,
                    pkey("good cat"),
                    DecodedBatchEventKind::Insert(vec![
                        Datacell::new_bool(true),
                        Datacell::new_bool(false)
                    ])
                ),
                DecodedBatchEvent::new(
                    2,
                    pkey("bad cat"),
                    DecodedBatchEventKind::Insert(vec![
                        Datacell::new_bool(false),
                        Datacell::new_bool(false)
                    ])
                ),
                DecodedBatchEvent::new(
                    3,
                    pkey("Schrödinger's cat"),
                    DecodedBatchEventKind::Update(vec![
                        Datacell::new_bool(true),
                        Datacell::new_bool(true)
                    ])
                )
            ],
            0
        )]
    )
}

#[test]
fn skewed_shuffled_persist_restore() {
    let uuid = Uuid::new();
    let model = Model::new_restore(
        uuid,
        "username".into(),
        TagSelector::String.into_full(),
        into_dict!("username" => Field::new([Layer::str()].into(), false), "password" => Field::new([Layer::str()].into(), false)),
    );
    let mongobongo = Row::new(
        pkey("mongobongo"),
        into_dict!("password" => "dumbo"),
        DeltaVersion::__new(0),
        DeltaVersion::__new(4),
    );
    let rds = Row::new(
        pkey("rds"),
        into_dict!("password" => "snail"),
        DeltaVersion::__new(0),
        DeltaVersion::__new(5),
    );
    let deltas = [
        new_delta(
            0,
            0,
            "sayan",
            into_dict!("password" => "pwd123456"),
            DataDeltaKind::Insert,
        ),
        new_delta(
            0,
            1,
            "joseph",
            into_dict!("password" => "pwd234567"),
            DataDeltaKind::Insert,
        ),
        new_delta(
            0,
            2,
            "haley",
            into_dict!("password" => "pwd345678"),
            DataDeltaKind::Insert,
        ),
        new_delta(
            0,
            3,
            "charlotte",
            into_dict!("password" => "pwd456789"),
            DataDeltaKind::Insert,
        ),
        new_delta_with_row(4, mongobongo.clone(), DataDeltaKind::Insert),
        new_delta_with_row(5, rds.clone(), DataDeltaKind::Insert),
        new_delta_with_row(6, mongobongo.clone(), DataDeltaKind::Delete),
        new_delta_with_row(7, rds.clone(), DataDeltaKind::Delete),
    ];
    for i in 0..deltas.len() {
        // prepare pretest
        let fname = format!("skewed_shuffled_persist_restore_round{i}.db-btlog");
        let mut deltas = deltas.clone();
        let mut randomizer = test_utils::randomizer();
        test_utils::shuffle_slice(&mut deltas, &mut randomizer);
        // restore
        let mut restore_driver = flush_batches_and_return_restore_driver(deltas, &model, &fname);
        restore_driver.read_data_batch_into_model(&model).unwrap();
    }
    let g = pin();
    for delta in &deltas[..4] {
        let row = model
            .primary_index()
            .__raw_index()
            .mt_get(delta.row().d_key(), &g)
            .unwrap();
        let row_data = row.read();
        assert_eq!(row_data.fields().len(), 1);
        assert_eq!(
            row_data.fields().get("password").unwrap(),
            delta
                .row()
                .d_data()
                .read()
                .fields()
                .get("password")
                .unwrap()
        );
    }
}
