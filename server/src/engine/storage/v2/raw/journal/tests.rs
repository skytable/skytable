/*
 * Created on Fri Feb 09 2024
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

/*
    event log test
*/

use {
    super::{
        raw::{JournalSettings, RawJournalAdapterEvent},
        BatchAdapter, BatchAdapterSpec, BatchDriver, DispatchFn, EventLogAdapter, EventLogDriver,
        EventLogSpec,
    },
    crate::{
        engine::{
            error::StorageError,
            mem::unsafe_apis,
            storage::{
                common::sdss::sdss_r1::rw::{TrackedReaderContext, TrackedWriter},
                v2::raw::{
                    journal::raw::{create_journal, open_journal, RawJournalWriter},
                    spec::{ModelDataBatchAofV1, SystemDatabaseV1},
                },
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
    sky_macros::TaggedEnum,
    std::{
        cell::{Ref, RefCell, RefMut},
        rc::Rc,
    },
};

// event definitions

#[derive(TaggedEnum, Clone, Copy, Debug)]
#[repr(u8)]
pub enum TestEvent {
    Push = 0,
    Pop = 1,
    Clear = 2,
}

pub trait IsTestEvent {
    const EVCODE: TestEvent;
    fn encode(self, _: &mut Vec<u8>);
}

macro_rules! impl_test_event {
    ($($ty:ty as $code:expr $(=> $expr:expr)?),* $(,)?) => {
        $(impl IsTestEvent for $ty {
            const EVCODE: TestEvent = $code;
            fn encode(self, buf: &mut Vec<u8>) { let _ = buf; fn _do_it(s: $ty, b: &mut Vec<u8>, f: impl Fn($ty, &mut Vec<u8>)) { f(s, b) } $(_do_it(self, buf, $expr))? }
        })*
    }
}

pub struct EventPush<'a>(&'a str);
pub struct EventPop;
pub struct EventClear;

impl_test_event!(
    EventPush<'_> as TestEvent::Push => |me, buf| {
        buf.extend(&(me.0.len() as u64).to_le_bytes());
        buf.extend(me.0.as_bytes())
    },
    EventPop as TestEvent::Pop,
    EventClear as TestEvent::Clear,
);

impl<TE: IsTestEvent> RawJournalAdapterEvent<EventLogAdapter<TestDBAdapter>> for TE {
    fn md(&self) -> u64 {
        Self::EVCODE.dscr_u64()
    }
    fn write_buffered(self, buf: &mut Vec<u8>, _: ()) {
        TE::encode(self, buf)
    }
}

// adapter

pub struct TestDBAdapter;
impl EventLogSpec for TestDBAdapter {
    type Spec = SystemDatabaseV1;
    type GlobalState = TestDB;
    type EventMeta = TestEvent;
    type DecodeDispatch = [DispatchFn<TestDB>; 3];
    const DECODE_DISPATCH: Self::DecodeDispatch = [
        |db, payload| {
            if payload.len() < sizeof!(u64) {
                Err(StorageError::RawJournalDecodeEventCorruptedMetadata.into())
            } else {
                let length =
                    u64::from_le_bytes(unsafe { unsafe_apis::memcpy(&payload[..sizeof!(u64)]) });
                let payload = &payload[sizeof!(u64)..];
                if payload.len() as u64 != length {
                    Err(StorageError::RawJournalDecodeEventCorruptedPayload.into())
                } else {
                    let string = String::from_utf8(payload.to_owned()).unwrap();
                    db._mut().push(string);
                    Ok(())
                }
            }
        },
        |db, _| {
            let _ = db._mut().pop();
            Ok(())
        },
        |db, _| {
            db._mut().clear();
            Ok(())
        },
    ];
}

#[derive(Default)]
pub struct TestDB {
    data: RefCell<Vec<String>>,
}

impl TestDB {
    fn _mut(&self) -> RefMut<Vec<String>> {
        self.data.borrow_mut()
    }
    fn _ref(&self) -> Ref<Vec<String>> {
        self.data.borrow()
    }
    fn push(&self, log: &mut EventLogDriver<TestDBAdapter>, key: &str) -> RuntimeResult<()> {
        log.commit_event(EventPush(key))?;
        self._mut().push(key.into());
        Ok(())
    }
    fn pop(&self, log: &mut EventLogDriver<TestDBAdapter>) -> RuntimeResult<()> {
        assert!(!self._ref().is_empty());
        log.commit_event(EventPop)?;
        self._mut().pop().unwrap();
        Ok(())
    }
    fn clear(&self, log: &mut EventLogDriver<TestDBAdapter>) -> RuntimeResult<()> {
        log.commit_event(EventClear)?;
        self._mut().clear();
        Ok(())
    }
}

fn open_log() -> (
    TestDB,
    super::raw::RawJournalWriter<EventLogAdapter<TestDBAdapter>>,
) {
    let db = TestDB::default();
    let log = open_journal("jrnl", &db, JournalSettings::default()).unwrap();
    (db, log)
}

#[test]
fn test_this_data() {
    array!(
        const DATA1: [&str] = ["acai berry", "billberry", "cranberry"];
        const DATA2: [&str] = ["acai berry", "billberry", "cranberry", "bradbury"];
        const DATA3: [&str] = ["acai berry", "billberry", "cranberry"];
        const DATA4: [&str] = ["acai berry", "billberry", "cranberry", "dewberry"];
    );
    {
        let db = TestDB::default();
        let mut log = create_journal("jrnl").unwrap();
        for key in DATA1 {
            db.push(&mut log, key).unwrap();
        }
        RawJournalWriter::close_driver(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA1);
        db.push(&mut log, DATA2[3]).unwrap();
        RawJournalWriter::close_driver(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA2);
        db.pop(&mut log).unwrap();
        RawJournalWriter::close_driver(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA3);
        db.push(&mut log, DATA4[3]).unwrap();
        RawJournalWriter::close_driver(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA4);
        db.clear(&mut log).unwrap();
        RawJournalWriter::close_driver(&mut log).unwrap();
    }
}

/*
    batch test
*/

#[derive(Debug, PartialEq, Clone, Copy, TaggedEnum)]
#[repr(u8)]
pub enum BatchType {
    GenericBatch = 0,
}

#[derive(Debug, PartialEq, Clone, Copy, TaggedEnum)]
#[repr(u8)]
pub enum BatchEventType {
    Push = 0,
    EarlyExit = 1,
}

pub struct BatchState {
    pending_inserts: Vec<String>,
}

#[derive(Debug, Default)]
pub struct BatchDB {
    inner: RefCell<BatchDBInner>,
}

impl BatchDB {
    fn new() -> Self {
        Self::default()
    }
    fn _mut(&self) -> RefMut<BatchDBInner> {
        self.inner.borrow_mut()
    }
    fn _ref(&self) -> Ref<BatchDBInner> {
        self.inner.borrow()
    }
    /// As soon as two changes occur, we sync to disk
    fn push(&self, driver: &mut BatchDriver<BatchDBAdapter>, key: &str) -> RuntimeResult<()> {
        let mut me = self._mut();
        me.data.push(key.into());
        let changed = me.data.len() - me.last_flushed_at;
        if changed == 2 {
            // this is the second change about to happen, so flush it!
            driver.commit_event(BatchDBFlush(&me, me.data.len()))?;
            me.last_flushed_at = me.data.len();
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct BatchDBInner {
    data: Vec<String>,
    last_flushed_at: usize,
}

struct BatchDBFlush<'a>(&'a BatchDBInner, usize);
impl<'a> RawJournalAdapterEvent<BatchAdapter<BatchDBAdapter>> for BatchDBFlush<'a> {
    fn md(&self) -> u64 {
        BatchType::GenericBatch.dscr_u64()
    }
    fn write_direct(
        self,
        f: &mut TrackedWriter<
            <BatchAdapter<BatchDBAdapter> as super::raw::RawJournalAdapter>::Spec,
        >,
        ctx: Rc<RefCell<BatchContext>>,
    ) -> RuntimeResult<()> {
        // write: [expected commit][body][actual commit]
        // for this dummy impl, we're expecting to write the full dataset but we're going to actually write the part
        // that has actually changed, enabling us to test the underlying impl
        let expected_commit = self.1 as u64;
        f.dtrack_write(&expected_commit.to_le_bytes())?;
        // now write all the keys
        let change_cnt = self.1 - self.0.last_flushed_at;
        let actual = &self.0.data[self.0.last_flushed_at..self.0.last_flushed_at + change_cnt];
        for key in actual {
            f.dtrack_write(&[BatchEventType::Push.dscr()])?;
            f.dtrack_write(&(key.len() as u64).to_le_bytes())?;
            f.dtrack_write(key.as_bytes())?;
        }
        // did we do something at all?
        if self.1 != actual.len() {
            // early exit!
            f.dtrack_write(&[BatchEventType::EarlyExit.dscr()])?;
        }
        ctx.borrow_mut().actual_write = actual.len();
        // actual commit
        f.dtrack_write(&(actual.len() as u64).to_le_bytes())?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BatchContext {
    actual_write: usize,
}

pub struct BatchDBAdapter;
impl BatchAdapterSpec for BatchDBAdapter {
    type Spec = ModelDataBatchAofV1;
    type GlobalState = BatchDB;
    type BatchType = BatchType;
    type EventType = BatchEventType;
    type BatchMetadata = ();
    type CommitContext = Rc<RefCell<BatchContext>>;
    type BatchState = BatchState;
    fn initialize_batch_state(_: &Self::GlobalState) -> Self::BatchState {
        BatchState {
            pending_inserts: vec![],
        }
    }
    fn is_early_exit(ev: &Self::EventType) -> bool {
        BatchEventType::EarlyExit.eq(ev)
    }
    fn decode_batch_metadata(
        _: &Self::GlobalState,
        _: &mut TrackedReaderContext<Self::Spec>,
        _: Self::BatchType,
    ) -> RuntimeResult<Self::BatchMetadata> {
        Ok(())
    }
    fn update_state_for_new_event(
        _: &Self::GlobalState,
        bs: &mut Self::BatchState,
        f: &mut TrackedReaderContext<Self::Spec>,
        _: &Self::BatchMetadata,
        event_type: Self::EventType,
    ) -> RuntimeResult<()> {
        match event_type {
            BatchEventType::EarlyExit => unreachable!(),
            BatchEventType::Push => {}
        }
        let key_len = u64::from_le_bytes(f.read_block()?);
        let mut key = vec![0; key_len as usize];
        f.read(&mut key)?;
        bs.pending_inserts.push(String::from_utf8(key).unwrap());
        Ok(())
    }
    fn finish(
        bs: Self::BatchState,
        _: Self::BatchMetadata,
        gs: &Self::GlobalState,
    ) -> RuntimeResult<()> {
        for event in bs.pending_inserts {
            gs._mut().data.push(event);
            gs._mut().last_flushed_at += 1;
        }
        Ok(())
    }
}

#[test]
fn batch_simple() {
    {
        let mut batch_drv = BatchAdapter::create("mybatch").unwrap();
        let db = BatchDB::new();
        db.push(&mut batch_drv, "key1").unwrap();
        db.push(&mut batch_drv, "key2").unwrap();
        BatchAdapter::close(&mut batch_drv).unwrap();
    }
    {
        let db = BatchDB::new();
        let mut batch_drv = BatchAdapter::open("mybatch", &db, JournalSettings::default()).unwrap();
        db.push(&mut batch_drv, "key3").unwrap();
        db.push(&mut batch_drv, "key4").unwrap();
        assert_eq!(db._ref().data, ["key1", "key2", "key3", "key4"]);
        BatchAdapter::close(&mut batch_drv).unwrap();
    }
    {
        let db = BatchDB::new();
        let mut batch_drv = BatchAdapter::open("mybatch", &db, JournalSettings::default()).unwrap();
        db.push(&mut batch_drv, "key5").unwrap();
        db.push(&mut batch_drv, "key6").unwrap();
        assert_eq!(
            db._ref().data,
            ["key1", "key2", "key3", "key4", "key5", "key6"]
        );
        BatchAdapter::close(&mut batch_drv).unwrap();
    }
    {
        let db = BatchDB::new();
        let mut batch_drv =
            BatchAdapter::<BatchDBAdapter>::open("mybatch", &db, JournalSettings::default())
                .unwrap();
        assert_eq!(
            db._ref().data,
            ["key1", "key2", "key3", "key4", "key5", "key6"]
        );
        BatchAdapter::close(&mut batch_drv).unwrap();
    }
}
