/*
 * Created on Tue Jan 30 2024
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

mod journal_ops;
mod recovery;

use {
    super::{
        CommitPreference, DriverEvent, DriverEventKind, JournalInitializer, RawJournalAdapter,
        RawJournalAdapterEvent, RawJournalWriter,
    },
    crate::engine::{
        error::StorageError,
        storage::{common::sdss::sdss_r1::rw::TrackedReader, v2::raw::spec::SystemDatabaseV1},
        RuntimeResult,
    },
    std::cell::RefCell,
};

/*
    impls for journal tests
*/

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleDB {
    data: RefCell<Vec<String>>,
}
impl SimpleDB {
    fn new() -> Self {
        Self {
            data: RefCell::default(),
        }
    }
    fn data(&self) -> std::cell::Ref<'_, Vec<String>> {
        self.data.borrow()
    }
    fn clear(&mut self, log: &mut RawJournalWriter<SimpleDBJournal>) -> RuntimeResult<()> {
        log.commit_event(DbEventClear)?;
        self.data.get_mut().clear();
        Ok(())
    }
    fn pop(&mut self, log: &mut RawJournalWriter<SimpleDBJournal>) -> RuntimeResult<()> {
        self.data.get_mut().pop().unwrap();
        log.commit_event(DbEventPop)?;
        Ok(())
    }
    fn push(
        &mut self,
        log: &mut RawJournalWriter<SimpleDBJournal>,
        new: impl ToString,
    ) -> RuntimeResult<()> {
        let new = new.to_string();
        log.commit_event(DbEventPush(&new))?;
        self.data.get_mut().push(new);
        Ok(())
    }
}

/*
    event impls
*/

#[derive(Debug)]
pub struct SimpleDBJournal;
struct DbEventPush<'a>(&'a str);
struct DbEventPop;
struct DbEventClear;
trait SimpleDBEvent: Sized {
    const OPC: u8;
    fn write_buffered(self, _: &mut Vec<u8>);
}
macro_rules! impl_db_event {
    ($($ty:ty as $code:expr $(=> $expr:expr)?),*) => {
        $(impl SimpleDBEvent for $ty {
            const OPC: u8 = $code;
            fn write_buffered(self, buf: &mut Vec<u8>) { let _ = buf; fn _do_it(s: $ty, b: &mut Vec<u8>, f: impl Fn($ty, &mut Vec<u8>)) { f(s, b) } $(_do_it(self, buf, $expr))? }
        })*
    }
}

impl_db_event!(
    DbEventPush<'_> as 0 => |me, buf| {
        buf.extend(&(me.0.len() as u64).to_le_bytes());
        buf.extend(me.0.as_bytes());
    },
    DbEventPop as 1,
    DbEventClear as 2
);

impl<T: SimpleDBEvent> RawJournalAdapterEvent<SimpleDBJournal> for T {
    fn md(&self) -> u64 {
        T::OPC as _
    }
    fn write_buffered(self, buf: &mut Vec<u8>, _: ()) {
        T::write_buffered(self, buf)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum EventMeta {
    NewKey,
    Pop,
    Clear,
}
impl RawJournalAdapter for SimpleDBJournal {
    const COMMIT_PREFERENCE: CommitPreference = CommitPreference::Buffered;
    type Spec = SystemDatabaseV1;
    type GlobalState = SimpleDB;
    type EventMeta = EventMeta;
    type CommitContext = ();
    type Context<'a> = () where Self: 'a;
    fn initialize(_: &JournalInitializer) -> Self {
        Self
    }
    fn enter_context<'a>(_: &'a mut RawJournalWriter<Self>) -> Self::Context<'a> {
        ()
    }
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        Some(match meta {
            0 => EventMeta::NewKey,
            1 => EventMeta::Pop,
            2 => EventMeta::Clear,
            _ => return None,
        })
    }
    fn commit_buffered<'a, E: RawJournalAdapterEvent<Self>>(
        &mut self,
        buf: &mut Vec<u8>,
        event: E,
        ctx: (),
    ) {
        event.write_buffered(buf, ctx)
    }
    fn decode_apply<'a>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        file: &mut TrackedReader<Self::Spec>,
    ) -> RuntimeResult<()> {
        match meta {
            EventMeta::NewKey => {
                let key_size = u64::from_le_bytes(file.read_block()?);
                let mut keybuf = vec![0u8; key_size as usize];
                file.tracked_read(&mut keybuf)?;
                match String::from_utf8(keybuf) {
                    Ok(k) => gs.data.borrow_mut().push(k),
                    Err(_) => {
                        return Err(StorageError::RawJournalDecodeEventCorruptedPayload.into())
                    }
                }
            }
            EventMeta::Clear => gs.data.borrow_mut().clear(),
            EventMeta::Pop => {
                let _ = gs.data.borrow_mut().pop().unwrap();
            }
        }
        Ok(())
    }
}

/*
    basic tests
*/

#[test]
fn encode_decode_meta() {
    let dv1 = DriverEvent::new(u128::MAX - 1, DriverEventKind::Reopened, 0, 0, 0);
    let encoded1 = dv1.encode_self();
    let decoded1 = DriverEvent::decode(encoded1).unwrap();
    assert_eq!(dv1, decoded1);
}
