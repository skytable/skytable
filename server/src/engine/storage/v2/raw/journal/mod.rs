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

use {
    self::raw::{CommitPreference, RawJournalAdapterEvent, RawJournalWriter},
    crate::{
        engine::{
            control_flow::errors::StorageError,
            storage::common::{
                checksum::SCrc64,
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
pub use raw::{
    compact_journal, compact_journal_direct, create_journal, open_journal, read_journal,
    repair_journal, JournalHeuristics, JournalRepairMode, JournalSettings, JournalStats,
    RawJournalAdapter, RawJournalAdapterEvent as JournalAdapterEvent, RepairResult,
};

/*
    implementation of a blanket event log

    ---
    1. linear
    2. append-only
    3. single-file
    4. multi-stage integrity checked
*/

/// An event log driver
pub type EventLogDriver<EL> = RawJournalWriter<EventLogAdapter<EL>>;
/// The event log adapter
#[derive(Debug)]
pub struct EventLogAdapter<EL: EventLogSpec>(PhantomData<EL>);
type DispatchFn<G> = fn(&G, &mut JournalHeuristics, Vec<u8>) -> RuntimeResult<()>;

/// Specification for an event log
pub trait EventLogSpec: Sized {
    /// the SDSS spec for this log
    type Spec: FileSpecV1;
    /// the global state for this log
    type GlobalState;
    /// event metadata
    type EventMeta: TaggedEnum<Dscr = u8>;
    type FullSyncCtx<'a>;
    type DecodeDispatch: Index<usize, Output = DispatchFn<Self::GlobalState>>;
    const DECODE_DISPATCH: Self::DecodeDispatch;
    const ENSURE: () = assert!(
        (mem::size_of::<Self::DecodeDispatch>() / mem::size_of::<DispatchFn<Self::GlobalState>>())
            == Self::EventMeta::VARIANT_COUNT as usize
    );
    /// make a full rewrite of the event log
    fn rewrite_log<'a>(
        writer: &mut RawJournalWriter<EventLogAdapter<Self>>,
        ctx: Self::FullSyncCtx<'a>,
    ) -> RuntimeResult<()>;
}

impl<EL: EventLogSpec> RawJournalAdapter for EventLogAdapter<EL> {
    const COMMIT_PREFERENCE: CommitPreference = {
        let _ = EL::ENSURE;
        CommitPreference::Direct
    };
    type Spec = <EL as EventLogSpec>::Spec;
    type GlobalState = <EL as EventLogSpec>::GlobalState;
    type Context<'a>
        = ()
    where
        Self: 'a;
    type EventMeta = <EL as EventLogSpec>::EventMeta;
    type CommitContext = ();
    type FullSyncCtx<'a> = EL::FullSyncCtx<'a>;
    fn initialize(_: &raw::JournalInitializer) -> Self {
        Self(PhantomData)
    }
    fn enter_context<'a>(_: &'a mut RawJournalWriter<Self>) -> Self::Context<'a> {}
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        <<EL as EventLogSpec>::EventMeta as TaggedEnum>::try_from_raw(meta as u8)
    }
    fn rewrite_full_journal<'a>(
        writer: &mut RawJournalWriter<Self>,
        ctx: Self::FullSyncCtx<'a>,
    ) -> RuntimeResult<()> {
        EL::rewrite_log(writer, ctx)
    }
    fn commit_direct<'a, E>(
        &mut self,
        w: &mut TrackedWriter<Self::Spec>,
        ev: E,
        ctx: (),
    ) -> RuntimeResult<()>
    where
        E: RawJournalAdapterEvent<Self>,
    {
        let mut pl = vec![];
        ev.write_buffered(&mut pl, ctx);
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
        e!(w.tracked_write(&pl))
    }
    fn decode_apply<'a>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        file: &mut TrackedReader<Self::Spec>,
        heuristics: &mut JournalHeuristics,
    ) -> RuntimeResult<()> {
        let expected_checksum = u64::from_le_bytes(file.read_block()?);
        let plen = u64::from_le_bytes(file.read_block()?);
        let mut pl = vec![0; plen as usize];
        file.tracked_read(&mut pl)?;
        let mut this_checksum = SCrc64::new();
        this_checksum.update(&plen.to_le_bytes());
        this_checksum.update(&pl);
        if this_checksum.finish() != expected_checksum {
            return Err(StorageError::RawJournalDecodeCorruptionInBatchMetadata.into());
        }
        heuristics.increment_server_event_count();
        <EL as EventLogSpec>::DECODE_DISPATCH
            [<<EL as EventLogSpec>::EventMeta as TaggedEnum>::dscr_u64(&meta) as usize](
            gs, heuristics, pl,
        )
    }
}

/*
    implementation of a batch journal

    ---

    1. linear
    2. append-only
    3. event batches
    4. integrity checked
*/

/// Batch journal driver
pub type BatchDriver<BA> = RawJournalWriter<BatchAdapter<BA>>;
/// Batch journal adapter
#[derive(Debug)]
pub struct BatchAdapter<BA: BatchAdapterSpec>(PhantomData<BA>);

#[cfg(test)]
impl<BA: BatchAdapterSpec> BatchAdapter<BA> {
    /// Open a new batch journal
    pub fn open(
        name: &str,
        gs: &BA::GlobalState,
        settings: JournalSettings,
    ) -> RuntimeResult<(BatchDriver<BA>, JournalStats)>
    where
        BA::Spec: FileSpecV1<DecodeArgs = ()>,
    {
        raw::open_journal::<BatchAdapter<BA>>(name, gs, settings)
    }
    /// Create a new batch journal
    pub fn create(name: &str) -> RuntimeResult<BatchDriver<BA>>
    where
        BA::Spec: FileSpecV1<EncodeArgs = ()>,
    {
        raw::create_journal::<BatchAdapter<BA>>(name)
    }
    /// Close a batch journal
    pub fn close(me: &mut BatchDriver<BA>) -> RuntimeResult<()> {
        RawJournalWriter::close_driver(me)
    }
}

#[derive(Debug, PartialEq)]
pub enum BatchEventExecutionLogic {
    General,
    Custom,
}

/// A specification for a batch journal
///
/// NB: This trait's impl is fairly complex and is going to require careful handling to get it right. Also, the event has to have
/// a specific on-disk layout: `[EXPECTED COMMIT][ANY ADDITIONAL METADATA][BATCH BODY][ACTUAL COMMIT]`
pub trait BatchAdapterSpec: Sized {
    /// the SDSS spec for this journal
    type Spec: FileSpecV1;
    /// global state used for syncing events
    type GlobalState;
    /// batch type tag
    type BatchRootType: TaggedEnum<Dscr = u8>;
    /// event type tag (event in batch)
    type EventType: TaggedEnum<Dscr = u8> + PartialEq;
    /// custom batch metadata
    type BatchMetadata;
    /// batch state
    type BatchState;
    /// commit context
    type CommitContext;
    type FullSyncCtx<'a>;
    /// get event execution logic
    fn get_event_logic(md: &Self::BatchRootType) -> BatchEventExecutionLogic;
    /// return true if the given event tag indicates an early exit
    fn is_early_exit(event_type: &Self::EventType) -> bool;
    /// initialize the batch state
    fn initialize_batch_state(gs: &Self::GlobalState) -> Self::BatchState;
    /// decode batch start metadata
    fn decode_batch_metadata(
        gs: &Self::GlobalState,
        f: &mut TrackedReaderContext<Self::Spec>,
        meta: Self::BatchRootType,
    ) -> RuntimeResult<Self::BatchMetadata>;
    /// decode new event and update state. if called, it is guaranteed that the event is not an early exit
    fn update_state_for_new_event(
        gs: &Self::GlobalState,
        bs: &mut Self::BatchState,
        f: &mut TrackedReaderContext<Self::Spec>,
        batch_info: &Self::BatchMetadata,
        event_type: Self::EventType,
        heuristics: &mut JournalHeuristics,
    ) -> RuntimeResult<()>;
    /// finish applying all changes to the global state
    fn finish(
        batch_state: Self::BatchState,
        batch_meta: Self::BatchMetadata,
        gs: &Self::GlobalState,
        heuristics: &mut JournalHeuristics,
    ) -> RuntimeResult<()>;
    /// Consolidate all records into one batch
    fn consolidate_batch<'a>(
        writer: &mut RawJournalWriter<BatchAdapter<Self>>,
        ctx: Self::FullSyncCtx<'a>,
    ) -> RuntimeResult<()>;
    /// If an event falls outside the standard execution routine, then this is the entrypoint for any such execution
    fn decode_execute_custom(
        _gs: &Self::GlobalState,
        _f: &mut TrackedReader<Self::Spec>,
        _meta: Self::BatchRootType,
    ) -> RuntimeResult<()> {
        unimplemented!()
    }
}

impl<Ba: BatchAdapterSpec> RawJournalAdapter for BatchAdapter<Ba> {
    const COMMIT_PREFERENCE: CommitPreference = CommitPreference::Direct;
    type Spec = <Ba as BatchAdapterSpec>::Spec;
    type GlobalState = <Ba as BatchAdapterSpec>::GlobalState;
    type Context<'a>
        = ()
    where
        Self: 'a;
    type EventMeta = <Ba as BatchAdapterSpec>::BatchRootType;
    type CommitContext = <Ba as BatchAdapterSpec>::CommitContext;
    type FullSyncCtx<'a> = Ba::FullSyncCtx<'a>;
    fn rewrite_full_journal<'a>(
        writer: &mut RawJournalWriter<Self>,
        ctx: Self::FullSyncCtx<'a>,
    ) -> RuntimeResult<()> {
        Ba::consolidate_batch(writer, ctx)
    }
    fn initialize(_: &raw::JournalInitializer) -> Self {
        Self(PhantomData)
    }
    fn enter_context<'a>(_: &'a mut RawJournalWriter<Self>) -> Self::Context<'a> {}
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        <<Ba as BatchAdapterSpec>::BatchRootType as TaggedEnum>::try_from_raw(meta as u8)
    }
    fn commit_direct<'a, E>(
        &mut self,
        w: &mut TrackedWriter<Self::Spec>,
        ev: E,
        ctx: Self::CommitContext,
    ) -> RuntimeResult<()>
    where
        E: RawJournalAdapterEvent<Self>,
    {
        match Ba::get_event_logic(&ev.md()) {
            // standard logic
            BatchEventExecutionLogic::General => {
                ev.write_direct(w, ctx)?;
                let checksum = w.reset_partial();
                e!(w.tracked_write(&checksum.to_le_bytes()))
            }
            // custom logic, pass directly to handler
            BatchEventExecutionLogic::Custom => ev.write_direct(w, ctx),
        }
    }
    fn decode_apply<'a>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        f: &mut TrackedReader<Self::Spec>,
        heuristics: &mut JournalHeuristics,
    ) -> RuntimeResult<()> {
        match Ba::get_event_logic(&meta) {
            BatchEventExecutionLogic::Custom => Ba::decode_execute_custom(gs, f, meta),
            BatchEventExecutionLogic::General => {
                let mut f = f.context();
                {
                    // get metadata
                    // read batch size
                    let _stored_expected_commit_size = u64::from_le_bytes(f.read_block()?);
                    // read custom metadata
                    let batch_md =
                        <Ba as BatchAdapterSpec>::decode_batch_metadata(gs, &mut f, meta)?;
                    // now read in every event
                    let mut real_commit_size = 0;
                    let mut batch_state = <Ba as BatchAdapterSpec>::initialize_batch_state(gs);
                    loop {
                        if real_commit_size == _stored_expected_commit_size {
                            break;
                        }
                        let event_type =
                            <<Ba as BatchAdapterSpec>::EventType as TaggedEnum>::try_from_raw(
                                f.read_block().map(|[b]| b)?,
                            )
                            .ok_or(StorageError::InternalDecodeStructureIllegalData)?;
                        // is this an early exit marker? if so, exit
                        if <Ba as BatchAdapterSpec>::is_early_exit(&event_type) {
                            break;
                        }
                        // update batch state
                        Ba::update_state_for_new_event(
                            gs,
                            &mut batch_state,
                            &mut f,
                            &batch_md,
                            event_type,
                            heuristics,
                        )?;
                        real_commit_size += 1;
                    }
                    // read actual commit size
                    let _stored_actual_commit_size = u64::from_le_bytes(f.read_block()?);
                    if _stored_actual_commit_size == real_commit_size {
                        // finish applying batch
                        Ba::finish(batch_state, batch_md, gs, heuristics)?;
                    } else {
                        return Err(StorageError::RawJournalDecodeBatchContentsMismatch.into());
                    }
                }
                // and finally, verify checksum
                let (real_checksum, file) = f.finish();
                let stored_checksum = u64::from_le_bytes(file.read_block()?);
                if real_checksum == stored_checksum {
                    Ok(())
                } else {
                    Err(StorageError::RawJournalDecodeBatchIntegrityFailure.into())
                }
            }
        }
    }
}
