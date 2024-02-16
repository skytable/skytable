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

use {
    self::raw::{CommitPreference, RawJournalAdapter, RawJournalAdapterEvent, RawJournalWriter},
    crate::{
        engine::{
            error::StorageError,
            storage::common::{
                checksum::SCrc64,
                interface::fs_traits::{FSInterface, FileInterface},
                sdss::sdss_r1::{
                    rw::{TrackedReader, TrackedReaderContext, TrackedWriter},
                    FileSpecV1,
                },
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
    std::{marker::PhantomData, mem, ops::Index},
};

mod raw;
#[cfg(test)]
mod tests;

pub type EventLogDriver<EL, Fs> = RawJournalWriter<EventLog<EL>, Fs>;
pub struct EventLog<EL: EventLogAdapter>(PhantomData<EL>);
impl<EL: EventLogAdapter> EventLog<EL> {
    pub fn close<Fs: FSInterface>(me: &mut EventLogDriver<EL, Fs>) -> RuntimeResult<()> {
        RawJournalWriter::close_driver(me)
    }
}

type DispatchFn<G> = fn(&G, Vec<u8>) -> RuntimeResult<()>;

pub trait EventLogAdapter {
    type Spec: FileSpecV1;
    type GlobalState;
    type EventMeta: TaggedEnum<Dscr = u8>;
    type DecodeDispatch: Index<usize, Output = DispatchFn<Self::GlobalState>>;
    const DECODE_DISPATCH: Self::DecodeDispatch;
    const ENSURE: () = assert!(
        (mem::size_of::<Self::DecodeDispatch>() / mem::size_of::<DispatchFn<Self::GlobalState>>())
            == Self::EventMeta::VARIANT_COUNT as usize
    );
}

impl<EL: EventLogAdapter> RawJournalAdapter for EventLog<EL> {
    const COMMIT_PREFERENCE: CommitPreference = {
        let _ = EL::ENSURE;
        CommitPreference::Direct
    };
    type Spec = <EL as EventLogAdapter>::Spec;
    type GlobalState = <EL as EventLogAdapter>::GlobalState;
    type Context<'a> = () where Self: 'a;
    type EventMeta = <EL as EventLogAdapter>::EventMeta;
    fn initialize(_: &raw::JournalInitializer) -> Self {
        Self(PhantomData)
    }
    fn enter_context<'a, Fs: FSInterface>(
        _: &'a mut RawJournalWriter<Self, Fs>,
    ) -> Self::Context<'a> {
    }
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        <<EL as EventLogAdapter>::EventMeta as TaggedEnum>::try_from_raw(meta as u8)
    }
    fn commit_direct<'a, Fs: FSInterface, E>(
        &mut self,
        w: &mut TrackedWriter<Fs::File, Self::Spec>,
        ev: E,
    ) -> RuntimeResult<()>
    where
        E: RawJournalAdapterEvent<Self>,
    {
        let mut pl = vec![];
        ev.write_buffered(&mut pl);
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
    fn decode_apply<'a, Fs: FSInterface>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        file: &mut TrackedReader<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
    ) -> RuntimeResult<()> {
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
        <EL as EventLogAdapter>::DECODE_DISPATCH
            [<<EL as EventLogAdapter>::EventMeta as TaggedEnum>::dscr_u64(&meta) as usize](
            gs, pl
        )
    }
}

pub type BatchJournalDriver<BA, Fs> = RawJournalWriter<BatchJournal<BA>, Fs>;
pub struct BatchJournal<BA: BatchAdapter>(PhantomData<BA>);

impl<BA: BatchAdapter> BatchJournal<BA> {
    pub fn close<Fs: FSInterface>(me: &mut BatchJournalDriver<BA, Fs>) -> RuntimeResult<()> {
        RawJournalWriter::close_driver(me)
    }
}

pub trait BatchAdapter {
    type Spec: FileSpecV1;
    type GlobalState;
    type BatchMeta: TaggedEnum<Dscr = u8>;
    fn decode_batch<Fs: FSInterface>(
        gs: &Self::GlobalState,
        f: &mut TrackedReaderContext<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
        meta: Self::BatchMeta,
    ) -> RuntimeResult<()>;
}

impl<BA: BatchAdapter> RawJournalAdapter for BatchJournal<BA> {
    const COMMIT_PREFERENCE: CommitPreference = CommitPreference::Direct;
    type Spec = <BA as BatchAdapter>::Spec;
    type GlobalState = <BA as BatchAdapter>::GlobalState;
    type Context<'a> = () where BA: 'a;
    type EventMeta = <BA as BatchAdapter>::BatchMeta;
    fn initialize(_: &raw::JournalInitializer) -> Self {
        Self(PhantomData)
    }
    fn enter_context<'a, Fs: FSInterface>(
        _: &'a mut RawJournalWriter<Self, Fs>,
    ) -> Self::Context<'a> {
    }
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        <<BA as BatchAdapter>::BatchMeta as TaggedEnum>::try_from_raw(meta as u8)
    }
    fn commit_direct<'a, Fs: FSInterface, E>(
        &mut self,
        w: &mut TrackedWriter<Fs::File, Self::Spec>,
        ev: E,
    ) -> RuntimeResult<()>
    where
        E: RawJournalAdapterEvent<Self>,
    {
        ev.write_direct::<Fs>(w)?;
        let checksum = w.reset_partial();
        w.tracked_write(&checksum.to_le_bytes())
    }
    fn decode_apply<'a, Fs: FSInterface>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        file: &mut TrackedReader<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
    ) -> RuntimeResult<()> {
        let mut reader_ctx = file.context();
        <BA as BatchAdapter>::decode_batch::<Fs>(gs, &mut reader_ctx, meta)?;
        let (real_checksum, file) = reader_ctx.finish();
        let stored_checksum = u64::from_le_bytes(file.read_block()?);
        if real_checksum == stored_checksum {
            Ok(())
        } else {
            Err(StorageError::RawJournalCorrupted.into())
        }
    }
}
