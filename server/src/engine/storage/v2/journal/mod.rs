/*
 * Created on Sun Jan 21 2024
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

#![allow(dead_code)]

mod raw;
#[cfg(test)]
mod tests;

use {
    self::raw::{CommitPreference, JournalInitializer, RawJournalAdapter},
    crate::engine::{
        error::StorageError,
        fractal,
        storage::common::{
            checksum::SCrc64,
            interface::fs_traits::{FSInterface, FileInterface},
            sdss::sdss_r1::{
                rw::{TrackedReader, TrackedWriter},
                FileSpecV1,
            },
        },
        RuntimeResult,
    },
    std::marker::PhantomData,
};

pub type EventLogJournal<E, Fs> = raw::RawJournalWriter<EventLog<E>, Fs>;

pub struct EventLog<E: EventLogAdapter>(PhantomData<E>);

pub trait EventLogAdapter {
    type SdssSpec: FileSpecV1;
    type GlobalState;
    type Event<'a>;
    type DecodedEvent;
    type EventMeta: Copy;
    type Error: Into<fractal::error::Error>;
    const EV_MAX: u8;
    unsafe fn meta_from_raw(m: u64) -> Self::EventMeta;
    fn event_md<'a>(event: &Self::Event<'a>) -> u64;
    fn encode<'a>(event: Self::Event<'a>) -> Box<[u8]>;
    fn decode(block: Vec<u8>, kind: Self::EventMeta) -> Result<Self::DecodedEvent, Self::Error>;
    fn apply_event(g: &Self::GlobalState, ev: Self::DecodedEvent) -> Result<(), Self::Error>;
}

impl<E: EventLogAdapter> RawJournalAdapter for EventLog<E> {
    const COMMIT_PREFERENCE: CommitPreference = CommitPreference::Direct;
    type Spec = <E as EventLogAdapter>::SdssSpec;
    type GlobalState = <E as EventLogAdapter>::GlobalState;
    type Event<'a> = <E as EventLogAdapter>::Event<'a>;
    type DecodedEvent = <E as EventLogAdapter>::DecodedEvent;
    type EventMeta = <E as EventLogAdapter>::EventMeta;
    fn initialize(_: &JournalInitializer) -> Self {
        Self(PhantomData)
    }
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        if meta > <E as EventLogAdapter>::EV_MAX as u64 {
            return None;
        }
        unsafe {
            // UNSAFE(@ohsayan): checked max
            Some(<E as EventLogAdapter>::meta_from_raw(meta))
        }
    }
    fn get_event_md<'a>(&self, event: &Self::Event<'a>) -> u64 {
        <E as EventLogAdapter>::event_md(event)
    }
    fn commit_direct<'a, Fs: FSInterface>(
        &mut self,
        w: &mut TrackedWriter<Fs::File, Self::Spec>,
        event: Self::Event<'a>,
    ) -> RuntimeResult<()> {
        let pl = <E as EventLogAdapter>::encode(event);
        let plen = (pl.len() as u64).to_le_bytes();
        let mut checksum = SCrc64::new();
        checksum.update(&plen);
        checksum.update(&pl);
        let checksum = checksum.finish().to_le_bytes();
        /*
            [CK][PLEN][PL]
        */
        w.tracked_write(&checksum)?;
        w.tracked_write(&plen)?;
        w.tracked_write(&pl)
    }
    fn parse_event<'a, Fs: FSInterface>(
        file: &mut TrackedReader<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
        m: Self::EventMeta,
    ) -> RuntimeResult<Self::DecodedEvent> {
        /*
            verify checksum
        */
        let expected_checksum = u64::from_le_bytes(file.read_block()?);
        let plen = u64::from_le_bytes(file.read_block()?);
        let mut pl = vec![0; plen as usize];
        file.tracked_read(&mut pl)?;
        let mut this_checksum = SCrc64::new();
        this_checksum.update(&plen.to_le_bytes());
        this_checksum.update(&pl);
        if this_checksum.finish() != expected_checksum {
            return Err(StorageError::RawJournalCorrupted.into());
        }
        <E as EventLogAdapter>::decode(pl, m).map_err(Into::into)
    }
    fn apply_event<'a>(
        gs: &Self::GlobalState,
        _: Self::EventMeta,
        event: Self::DecodedEvent,
    ) -> RuntimeResult<()> {
        <E as EventLogAdapter>::apply_event(gs, event).map_err(Into::into)
    }
}
