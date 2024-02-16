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
        raw::{RawJournalAdapter, RawJournalAdapterEvent},
        BatchAdapter, BatchJournal, BatchJournalDriver, DispatchFn, EventLog, EventLogAdapter,
        EventLogDriver,
    },
    crate::{
        engine::{
            error::StorageError,
            mem::unsafe_apis,
            storage::{
                common::{
                    interface::{
                        fs_test::VirtualFS,
                        fs_traits::{FSInterface, FileInterface},
                    },
                    sdss::sdss_r1::rw::{TrackedReaderContext, TrackedWriter},
                },
                v2::raw::{
                    journal::raw::{create_journal, open_journal},
                    spec::{ModelDataBatchAofV1, SystemDatabaseV1},
                },
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
    sky_macros::TaggedEnum,
    std::cell::{Ref, RefCell, RefMut},
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
            fn encode(self, buf: &mut Vec<u8>) { let _ = buf; fn do_it(s: $ty, b: &mut Vec<u8>, f: impl Fn($ty, &mut Vec<u8>)) { f(s, b) } $(do_it(self, buf, $expr))? }
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

impl<TE: IsTestEvent> RawJournalAdapterEvent<EventLog<TestDBAdapter>> for TE {
    fn md(&self) -> u64 {
        Self::EVCODE.dscr_u64()
    }
    fn write_buffered(self, buf: &mut Vec<u8>) {
        TE::encode(self, buf)
    }
}

// adapter

pub struct TestDBAdapter;
impl EventLogAdapter for TestDBAdapter {
    type Spec = SystemDatabaseV1;
    type GlobalState = TestDB;
    type EventMeta = TestEvent;
    type DecodeDispatch = [DispatchFn<TestDB>; 3];
    const DECODE_DISPATCH: Self::DecodeDispatch = [
        |db, payload| {
            if payload.len() < sizeof!(u64) {
                Err(StorageError::RawJournalCorrupted.into())
            } else {
                let length =
                    u64::from_le_bytes(unsafe { unsafe_apis::memcpy(&payload[..sizeof!(u64)]) });
                let payload = &payload[sizeof!(u64)..];
                if payload.len() as u64 != length {
                    Err(StorageError::RawJournalCorrupted.into())
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
    fn push(
        &self,
        log: &mut EventLogDriver<TestDBAdapter, VirtualFS>,
        key: &str,
    ) -> RuntimeResult<()> {
        log.commit_event(EventPush(key))?;
        self._mut().push(key.into());
        Ok(())
    }
    fn pop(&self, log: &mut EventLogDriver<TestDBAdapter, VirtualFS>) -> RuntimeResult<()> {
        assert!(!self._ref().is_empty());
        log.commit_event(EventPop)?;
        self._mut().pop().unwrap();
        Ok(())
    }
    fn clear(&self, log: &mut EventLogDriver<TestDBAdapter, VirtualFS>) -> RuntimeResult<()> {
        log.commit_event(EventClear)?;
        self._mut().clear();
        Ok(())
    }
}

fn open_log() -> (
    TestDB,
    super::raw::RawJournalWriter<EventLog<TestDBAdapter>, VirtualFS>,
) {
    let db = TestDB::default();
    let log = open_journal("jrnl", &db).unwrap();
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
        EventLog::close(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA1);
        db.push(&mut log, DATA2[3]).unwrap();
        EventLog::close(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA2);
        db.pop(&mut log).unwrap();
        EventLog::close(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA3);
        db.push(&mut log, DATA4[3]).unwrap();
        EventLog::close(&mut log).unwrap();
    }
    {
        let (db, mut log) = open_log();
        assert_eq!(db._ref().as_slice(), DATA4);
        EventLog::close(&mut log).unwrap();
    }
}

/*
    batch test
*/

struct BatchDB {
    data: RefCell<BatchDBInner>,
}

struct BatchDBInner {
    data: Vec<String>,
    changed: usize,
    last_idx: usize,
}

impl BatchDB {
    const THRESHOLD: usize = 1;
    fn new() -> Self {
        Self {
            data: RefCell::new(BatchDBInner {
                data: vec![],
                changed: 0,
                last_idx: 0,
            }),
        }
    }
    fn _mut(&self) -> RefMut<BatchDBInner> {
        self.data.borrow_mut()
    }
    fn _ref(&self) -> Ref<BatchDBInner> {
        self.data.borrow()
    }
    fn push(
        &self,
        log: &mut BatchJournalDriver<BatchDBAdapter, VirtualFS>,
        key: &str,
    ) -> RuntimeResult<()> {
        let mut me = self._mut();
        me.data.push(key.into());
        if me.changed == Self::THRESHOLD {
            me.changed += 1;
            log.commit_event(FlushBatch::new(&me, me.last_idx, me.changed))?;
            me.changed = 0;
            me.last_idx = me.data.len();
            Ok(())
        } else {
            me.changed += 1;
            Ok(())
        }
    }
}

struct BatchDBAdapter;
#[derive(Debug, Clone, Copy, TaggedEnum, PartialEq)]
#[repr(u8)]
enum BatchEvent {
    NewBatch = 0,
}
impl BatchAdapter for BatchDBAdapter {
    type Spec = ModelDataBatchAofV1;
    type GlobalState = BatchDB;
    type BatchMeta = BatchEvent;
    fn decode_batch<Fs: FSInterface>(
        gs: &Self::GlobalState,
        f: &mut TrackedReaderContext<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
        meta: Self::BatchMeta,
    ) -> RuntimeResult<()> {
        let mut gs = gs._mut();
        assert_eq!(meta, BatchEvent::NewBatch);
        let mut batch_size = u64::from_le_bytes(f.read_block()?);
        while batch_size != 0 {
            let keylen = u64::from_le_bytes(f.read_block()?);
            let mut key = vec![0; keylen as usize];
            f.read(&mut key)?;
            gs.data.push(String::from_utf8(key).unwrap());
            gs.last_idx += 1;
            batch_size -= 1;
        }
        Ok(())
    }
}
struct FlushBatch<'a> {
    data: &'a BatchDBInner,
    start: usize,
    cnt: usize,
}

impl<'a> FlushBatch<'a> {
    fn new(data: &'a BatchDBInner, start: usize, cnt: usize) -> Self {
        Self { data, start, cnt }
    }
}

impl<'a> RawJournalAdapterEvent<BatchJournal<BatchDBAdapter>> for FlushBatch<'a> {
    fn md(&self) -> u64 {
        BatchEvent::NewBatch.dscr_u64()
    }
    fn write_direct<Fs: FSInterface>(
        self,
        w: &mut TrackedWriter<Fs::File, <BatchJournal<BatchDBAdapter> as RawJournalAdapter>::Spec>,
    ) -> RuntimeResult<()> {
        // length
        w.dtrack_write(&(self.cnt as u64).to_le_bytes())?;
        // now write all the new keys
        for key in &self.data.data[self.start..self.start + self.cnt] {
            w.dtrack_write(&(key.len() as u64).to_le_bytes())?;
            w.dtrack_write(key.as_bytes())?;
        }
        Ok(())
    }
}

#[test]
fn batch_simple() {
    {
        let mut log = create_journal::<_, VirtualFS>("batch_jrnl").unwrap();
        let db = BatchDB::new();
        db.push(&mut log, "a").unwrap();
        db.push(&mut log, "b").unwrap();
        BatchJournal::close(&mut log).unwrap();
    }
    {
        let db = BatchDB::new();
        let mut log = open_journal::<_, VirtualFS>("batch_jrnl", &db).unwrap();
        db.push(&mut log, "c").unwrap();
        db.push(&mut log, "d").unwrap();
        BatchJournal::close(&mut log).unwrap();
    }
    {
        let db = BatchDB::new();
        let mut log = open_journal::<_, VirtualFS>("batch_jrnl", &db).unwrap();
        db.push(&mut log, "e").unwrap();
        db.push(&mut log, "f").unwrap();
        BatchJournal::close(&mut log).unwrap();
    }
    {
        let db = BatchDB::new();
        let mut log =
            open_journal::<BatchJournal<BatchDBAdapter>, VirtualFS>("batch_jrnl", &db).unwrap();
        assert_eq!(db._ref().data, ["a", "b", "c", "d", "e", "f"]);
        BatchJournal::close(&mut log).unwrap();
    }
}
