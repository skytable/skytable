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
    super::{raw::create_journal, EventLog, EventLogAdapter, EventLogJournal},
    crate::engine::{
        error::StorageError,
        fractal,
        mem::unsafe_apis,
        storage::{
            common::interface::fs_test::VirtualFS,
            v2::raw::{journal::raw::open_journal, spec::SystemDatabaseV1},
        },
        RuntimeResult,
    },
    std::cell::{Ref, RefCell, RefMut},
};

#[derive(Default)]
pub struct SimpleDB {
    values: RefCell<Vec<String>>,
}

impl SimpleDB {
    fn as_mut(&self) -> RefMut<Vec<String>> {
        self.values.borrow_mut()
    }
    fn as_ref(&self) -> Ref<Vec<String>> {
        self.values.borrow()
    }
    fn push(
        &self,
        log: &mut EventLogJournal<SimpleDBAdapter, VirtualFS>,
        key: &str,
    ) -> RuntimeResult<()> {
        log.commit_event(DbEvent::Push(key))?;
        self.as_mut().push(key.into());
        Ok(())
    }
    fn pop(&self, log: &mut EventLogJournal<SimpleDBAdapter, VirtualFS>) -> RuntimeResult<()> {
        log.commit_event(DbEvent::Pop)?;
        self.as_mut().pop().unwrap();
        Ok(())
    }
    fn clear(&self, log: &mut EventLogJournal<SimpleDBAdapter, VirtualFS>) -> RuntimeResult<()> {
        log.commit_event(DbEvent::Clear)?;
        self.as_mut().clear();
        Ok(())
    }
}

enum DbEvent<'a> {
    Push(&'a str),
    Pop,
    Clear,
}

enum DbEventDecoded {
    Push(String),
    Pop,
    Clear,
}

struct SimpleDBAdapter;

impl EventLogAdapter for SimpleDBAdapter {
    type SdssSpec = SystemDatabaseV1;
    type GlobalState = SimpleDB;
    type Event<'a> = DbEvent<'a>;
    type DecodedEvent = DbEventDecoded;
    type EventMeta = u64;
    type Error = fractal::error::Error;
    const EV_MAX: u8 = 2;
    unsafe fn meta_from_raw(m: u64) -> Self::EventMeta {
        m
    }
    fn event_md<'a>(event: &Self::Event<'a>) -> u64 {
        match event {
            DbEvent::Push(_) => 0,
            DbEvent::Pop => 1,
            DbEvent::Clear => 2,
        }
    }
    fn encode<'a>(event: Self::Event<'a>) -> Box<[u8]> {
        if let DbEvent::Push(k) = event {
            let mut buf = Vec::new();
            buf.extend(&(k.len() as u64).to_le_bytes());
            buf.extend(k.as_bytes());
            buf.into_boxed_slice()
        } else {
            Default::default()
        }
    }
    fn decode(block: Vec<u8>, m: u64) -> Result<Self::DecodedEvent, Self::Error> {
        Ok(match m {
            0 => {
                if block.len() < sizeof!(u64) {
                    return Err(StorageError::RawJournalCorrupted.into());
                }
                let len =
                    u64::from_le_bytes(unsafe { unsafe_apis::memcpy(&block[..sizeof!(u64)]) });
                let block = &block[sizeof!(u64)..];
                if block.len() as u64 != len {
                    return Err(StorageError::RawJournalCorrupted.into());
                }
                DbEventDecoded::Push(String::from_utf8_lossy(block).into())
            }
            1 => DbEventDecoded::Pop,
            2 => DbEventDecoded::Clear,
            _ => panic!(),
        })
    }
    fn apply_event(g: &Self::GlobalState, ev: Self::DecodedEvent) -> Result<(), Self::Error> {
        match ev {
            DbEventDecoded::Push(new) => g.as_mut().push(new),
            DbEventDecoded::Pop => {
                let _ = g.as_mut().pop();
            }
            DbEventDecoded::Clear => g.as_mut().clear(),
        }
        Ok(())
    }
}

#[test]
fn event_log_basic_events() {
    array!(const VALUES: [&str] = ["key1", "key2", "key3", "fancykey", "done"]);
    {
        let mut log = create_journal::<EventLog<SimpleDBAdapter>, _>("jrnl1").unwrap();
        let db = SimpleDB::default();
        for value in VALUES {
            db.push(&mut log, value).unwrap();
        }
        db.pop(&mut log).unwrap();
        EventLogJournal::close_driver(&mut log).unwrap();
    }
    {
        let db = SimpleDB::default();
        let mut log = open_journal::<EventLog<SimpleDBAdapter>, VirtualFS>("jrnl1", &db).unwrap();
        EventLogJournal::close_driver(&mut log).unwrap();
        assert_eq!(
            db.as_ref().as_slice().last().unwrap(),
            VALUES[VALUES.len() - 2]
        );
        assert_eq!(db.as_ref().as_slice(), &VALUES[..VALUES.len() - 1]);
    }
}
