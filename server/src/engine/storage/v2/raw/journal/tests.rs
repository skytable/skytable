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
    super::{raw::RawJournalAdapterEvent, DispatchFn, EventLog, EventLogAdapter, EventLogDriver},
    crate::{
        engine::{
            error::StorageError,
            mem::unsafe_apis,
            storage::{
                common::interface::fs_test::VirtualFS,
                v2::raw::{
                    journal::raw::{create_journal, open_journal},
                    spec::SystemDatabaseV1,
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
