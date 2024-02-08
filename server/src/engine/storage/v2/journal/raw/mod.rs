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

#[cfg(test)]
mod tests;

use {
    crate::engine::{
        error::StorageError,
        mem::unsafe_apis::memcpy,
        storage::common::{
            checksum::SCrc64,
            interface::fs_traits::{FSInterface, FileInterface},
            sdss::sdss_r1::{
                rw::{SdssFile, TrackedReader, TrackedWriter},
                FileSpecV1,
            },
        },
        RuntimeResult,
    },
    std::ops::Range,
};

/*
    loader
*/

/// Create a new journal
pub fn create_journal<J: RawJournalAdapter, Fs: FSInterface>(
    log_path: &str,
) -> RuntimeResult<RawJournalWriter<J, Fs>>
where
    J::Spec: FileSpecV1<EncodeArgs = ()>,
{
    let log = SdssFile::create::<Fs>(log_path)?;
    RawJournalWriter::new(
        JournalInitializer::new(<J::Spec as FileSpecV1>::SIZE as u64, SCrc64::new(), 0, 0),
        log,
    )
}

/// Open an existing journal
pub fn open_journal<J: RawJournalAdapter, Fs: FSInterface>(
    log_path: &str,
    gs: &J::GlobalState,
) -> RuntimeResult<RawJournalWriter<J, Fs>>
where
    J::Spec: FileSpecV1<DecodeArgs = ()>,
{
    let log = SdssFile::<_, J::Spec>::open::<Fs>(log_path)?;
    let (initializer, file) = RawJournalReader::<J, Fs>::scroll(log, gs)?;
    RawJournalWriter::new(initializer, file)
}

#[derive(Debug)]
pub struct JournalInitializer {
    cursor: u64,
    checksum: SCrc64,
    last_txn_id: u64,
    last_offset: u64,
}

impl JournalInitializer {
    pub fn new(cursor: u64, checksum: SCrc64, txn_id: u64, last_offset: u64) -> Self {
        Self {
            cursor,
            checksum,
            last_txn_id: txn_id,
            last_offset,
        }
    }
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
    pub fn checksum(&self) -> SCrc64 {
        self.checksum.clone()
    }
    pub fn txn_id(&self) -> u64 {
        self.last_txn_id
    }
    pub fn last_txn_id(&self) -> u64 {
        self.txn_id().saturating_sub(1)
    }
    pub fn is_new(&self) -> bool {
        self.last_offset == 0
    }
    pub fn last_offset(&self) -> u64 {
        self.last_offset
    }
}

/*
    tracing
*/

#[cfg(test)]
pub fn obtain_trace() -> Vec<JournalTraceEvent> {
    local_mut!(TRACE, |t| core::mem::take(t))
}

#[derive(Debug, PartialEq)]
pub enum JournalTraceEvent {
    Writer(JournalWriterTraceEvent),
    Reader(JournalReaderTraceEvent),
}

direct_from! {
    JournalTraceEvent => {
        JournalWriterTraceEvent as Writer,
        JournalReaderTraceEvent as Reader,
    }
}

#[derive(Debug, PartialEq)]
pub enum JournalReaderTraceEvent {
    Initialized,
    Completed,
    ClosedAndReachedEof,
    ReopenSuccess,
    // event
    AttemptingEvent(u64),
    DetectedServerEvent,
    ServerEventMetadataParsed,
    ServerEventParsed,
    ServerEventAppliedSuccess,
    // drv events
    DriverEventExpectingClose,
    DriverEventCompletedBlockRead,
    DriverEventExpectedCloseGotClose,
    DriverEventExpectingReopenBlock,
    DriverEventExpectingReopenGotReopen,
    // errors
    ErrTxnIdMismatch { expected: u64, current: u64 },
    DriverEventInvalidMetadata,
    ErrInvalidReopenMetadata,
    ErrExpectedCloseGotReopen,
}

#[derive(Debug, PartialEq)]
pub(super) enum JournalWriterTraceEvent {
    Initialized,
    ReinitializeAttempt,
    ReinitializeComplete,
    // server event
    CommitAttemptForEvent(u64),
    CommitServerEventWroteMetadata,
    CommitServerEventAdapterCompleted,
    CommitCommitServerEventSyncCompleted,
    // driver event
    DriverEventAttemptCommit {
        event: DriverEventKind,
        event_id: u64,
        prev_id: u64,
    },
    DriverEventPresyncCompleted,
    DriverEventCompleted,
    DriverClosed,
}

local! {
    #[cfg(test)]
    static TRACE: Vec<JournalTraceEvent> = Vec::new();
}

macro_rules! jtrace {
    ($expr:expr) => {
        #[cfg(test)]
        {
            local_mut!(TRACE, |traces| traces.push($expr.into()))
        }
    };
}

macro_rules! jtrace_writer {
    ($var:ident) => { jtrace!(JournalWriterTraceEvent::$var) };
    ($var:ident $($tt:tt)*) => { jtrace!(JournalWriterTraceEvent::$var$($tt)*) };
}

macro_rules! jtrace_reader {
    ($var:ident) => { jtrace!(JournalReaderTraceEvent::$var) };
    ($var:ident $($tt:tt)*) => { jtrace!(JournalReaderTraceEvent::$var$($tt)*) };
}

/*
    impls
*/

/// An adapter defining the low-level structure of a log file
pub trait RawJournalAdapter {
    /// event size buffer
    const EVENT_SIZE_BUFFER: usize = 128;
    /// Set to true if the journal writer should automatically flush the buffer and fsync after writing an event
    const AUTO_SYNC_ON_EVENT_COMMIT: bool = true;
    /// set the commit preference
    const COMMIT_PREFERENCE: CommitPreference;
    /// the journal's file spec
    type Spec: FileSpecV1;
    /// the global state that is used by this journal
    type GlobalState;
    /// a journal event
    type Event<'a>;
    /// the decoded event
    type DecodedEvent;
    /// a type representing the event kind
    type EventMeta: Copy;
    /// initialize this adapter
    fn initialize(j_: &JournalInitializer) -> Self;
    /// parse event metadata
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta>;
    /// get event metadata as an [`u64`]
    fn get_event_md<'a>(&self, event: &Self::Event<'a>) -> u64;
    /// commit event (direct preference)
    fn commit_direct<'a, Fs: FSInterface>(
        &mut self,
        _: &mut TrackedWriter<Fs::File, Self::Spec>,
        _: Self::Event<'a>,
    ) -> RuntimeResult<()> {
        unimplemented!()
    }
    /// commit event (buffered)
    fn commit_buffered<'a>(&mut self, _: &mut Vec<u8>, _: Self::Event<'a>) {
        unimplemented!()
    }
    /// parse the event
    fn parse_event<'a, Fs: FSInterface>(
        file: &mut TrackedReader<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
        meta: Self::EventMeta,
    ) -> RuntimeResult<Self::DecodedEvent>;
    /// apply the event
    fn apply_event<'a>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        event: Self::DecodedEvent,
    ) -> RuntimeResult<()>;
}

#[derive(Debug, PartialEq)]
pub enum CommitPreference {
    Buffered,
    Direct,
}

#[derive(Debug, PartialEq)]
/**
A driver event
---
Structured as:
+------------------+----------+--------------+------------------+-------------------+-----------------+-----------------+
|   16B: Event ID  | 8B: Meta | 8B: Checksum | 8B: Payload size | 8B: prev checksum | 8B: prev offset | 8B: prev txn id |
+------------------+----------+--------------+------------------+-------------------+-----------------+-----------------+
*/

struct DriverEvent {
    txn_id: u128,
    event: DriverEventKind,
    checksum: u64,
    payload_len: u64,
    last_checksum: u64,
    last_offset: u64,
    last_txn_id: u64,
}

impl DriverEvent {
    const FULL_EVENT_SIZE: usize = Self::OFFSET_6_LAST_TXN_ID.end - Self::OFFSET_0_TXN_ID.start;
    /// currently fixed to 24B: last checksum + last offset + last txn id
    const PAYLOAD_LEN: u64 = 3;
    const OFFSET_0_TXN_ID: Range<usize> = 0..sizeof!(u128);
    const OFFSET_1_EVENT_KIND: Range<usize> =
        Self::OFFSET_0_TXN_ID.end..Self::OFFSET_0_TXN_ID.end + sizeof!(u64);
    const OFFSET_2_CHECKSUM: Range<usize> =
        Self::OFFSET_1_EVENT_KIND.end..Self::OFFSET_1_EVENT_KIND.end + sizeof!(u64);
    const OFFSET_3_PAYLOAD_LEN: Range<usize> =
        Self::OFFSET_2_CHECKSUM.end..Self::OFFSET_2_CHECKSUM.end + sizeof!(u64);
    const OFFSET_4_LAST_CHECKSUM: Range<usize> =
        Self::OFFSET_3_PAYLOAD_LEN.end..Self::OFFSET_3_PAYLOAD_LEN.end + sizeof!(u64);
    const OFFSET_5_LAST_OFFSET: Range<usize> =
        Self::OFFSET_4_LAST_CHECKSUM.end..Self::OFFSET_4_LAST_CHECKSUM.end + sizeof!(u64);
    const OFFSET_6_LAST_TXN_ID: Range<usize> =
        Self::OFFSET_5_LAST_OFFSET.end..Self::OFFSET_5_LAST_OFFSET.end + sizeof!(u64);
    /// Create a new driver event (checksum auto-computed)
    fn new(
        txn_id: u128,
        driver_event: DriverEventKind,
        last_checksum: u64,
        last_offset: u64,
        last_txn_id: u64,
    ) -> Self {
        let mut checksum = SCrc64::new();
        checksum.update(&Self::PAYLOAD_LEN.to_le_bytes());
        checksum.update(&last_checksum.to_le_bytes());
        checksum.update(&last_offset.to_le_bytes());
        checksum.update(&last_txn_id.to_le_bytes());
        Self::with_checksum(
            txn_id,
            driver_event,
            checksum.finish(),
            last_checksum,
            last_offset,
            last_txn_id,
        )
    }
    /// Create a new driver event with the given checksum
    fn with_checksum(
        txn_id: u128,
        driver_event: DriverEventKind,
        checksum: u64,
        last_checksum: u64,
        last_offset: u64,
        last_txn_id: u64,
    ) -> Self {
        Self {
            txn_id,
            event: driver_event,
            checksum,
            payload_len: Self::PAYLOAD_LEN as u64,
            last_checksum,
            last_offset,
            last_txn_id,
        }
    }
    /// Encode the current driver event
    fn encode_self(&self) -> [u8; 64] {
        Self::encode(
            self.txn_id,
            self.event,
            self.last_checksum,
            self.last_offset,
            self.last_txn_id,
        )
    }
    /// Encode a new driver event
    ///
    /// Notes:
    /// - The payload length is harcoded to 3
    /// - The checksum is automatically computed
    fn encode(
        txn_id: u128,
        driver_event: DriverEventKind,
        last_checksum: u64,
        last_offset: u64,
        last_txn_id: u64,
    ) -> [u8; 64] {
        const _: () = assert!(DriverEvent::OFFSET_6_LAST_TXN_ID.end == 64);
        let mut block = [0; 64];
        block[Self::OFFSET_0_TXN_ID].copy_from_slice(&txn_id.to_le_bytes());
        block[Self::OFFSET_1_EVENT_KIND]
            .copy_from_slice(&(driver_event.value_u8() as u64).to_le_bytes());
        // the below is a part of the payload
        let mut checksum = SCrc64::new();
        block[Self::OFFSET_3_PAYLOAD_LEN].copy_from_slice(&Self::PAYLOAD_LEN.to_le_bytes());
        block[Self::OFFSET_4_LAST_CHECKSUM].copy_from_slice(&last_checksum.to_le_bytes());
        block[Self::OFFSET_5_LAST_OFFSET].copy_from_slice(&last_offset.to_le_bytes());
        block[Self::OFFSET_6_LAST_TXN_ID].copy_from_slice(&last_txn_id.to_le_bytes());
        checksum.update(&block[Self::OFFSET_3_PAYLOAD_LEN.start..Self::OFFSET_6_LAST_TXN_ID.end]);
        // now update the checksum
        block[Self::OFFSET_2_CHECKSUM].copy_from_slice(&checksum.finish().to_le_bytes());
        block
    }
    fn decode(block: [u8; 64]) -> Option<Self> {
        var!(
            let txn_id, driver_event, checksum, payload_len, last_checksum, last_offset, last_txn_id
        );
        unsafe {
            /*
                UNSAFE(@ohsayan): we've ensured that the block size is exactly 64 and we use the offsets
                correctly
            */
            macro_rules! cpblk {
                ($target:path) => {
                    cpblk!($target as u64)
                };
                ($target:path as $ty:ty) => {
                    <$ty>::from_le_bytes(memcpy(&block[$target]))
                };
            }
            txn_id = cpblk!(Self::OFFSET_0_TXN_ID as u128);
            let driver_event_ = cpblk!(Self::OFFSET_1_EVENT_KIND);
            checksum = cpblk!(Self::OFFSET_2_CHECKSUM);
            payload_len = cpblk!(Self::OFFSET_3_PAYLOAD_LEN);
            last_checksum = cpblk!(Self::OFFSET_4_LAST_CHECKSUM);
            last_offset = cpblk!(Self::OFFSET_5_LAST_OFFSET);
            last_txn_id = cpblk!(Self::OFFSET_6_LAST_TXN_ID);
            // now validate checksum
            let mut checksum_ = SCrc64::new();
            checksum_
                .update(&block[Self::OFFSET_3_PAYLOAD_LEN.start..Self::OFFSET_6_LAST_TXN_ID.end]);
            let target_checksum = checksum_.finish();
            let invalid_ev_dscr = driver_event_ > DriverEventKind::MAX as u64;
            let invalid_ck = checksum != target_checksum;
            let invalid_pl_size = payload_len != 3;
            if invalid_ev_dscr | invalid_ck | invalid_pl_size {
                return None;
            }
            driver_event = core::mem::transmute(driver_event_ as u8);
            Some(Self::with_checksum(
                txn_id,
                driver_event,
                checksum,
                last_checksum,
                last_offset,
                last_txn_id,
            ))
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, sky_macros::EnumMethods)]
#[repr(u8)]
pub(super) enum DriverEventKind {
    Reopened = 0,
    Closed = 1,
}

/*
    +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    Journal writer implementation
    ---
    Quick notes:
    - This is a low level writer and only handles driver events. Higher level impls must account for

    +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/

/// A low-level journal writer
pub struct RawJournalWriter<J: RawJournalAdapter, Fs: FSInterface> {
    j: J,
    log_file: TrackedWriter<<Fs as FSInterface>::File, <J as RawJournalAdapter>::Spec>,
    txn_id: u64,
    known_txn_id: u64,
    known_txn_offset: u64, // if offset is 0, txn id is unset
}

const SERVER_EV_MASK: u64 = 1 << (u64::BITS - 1);

impl<J: RawJournalAdapter, Fs: FSInterface> RawJournalWriter<J, Fs> {
    /// Initialize a new [`RawJournalWriter`] using a [`JournalInitializer`]
    pub fn new(j_: JournalInitializer, file: SdssFile<Fs::File, J::Spec>) -> RuntimeResult<Self> {
        let mut me = Self {
            log_file: TrackedWriter::with_cursor_and_checksum(file, j_.cursor(), j_.checksum()),
            known_txn_id: j_.last_txn_id(),
            known_txn_offset: j_.last_offset(),
            txn_id: j_.txn_id(),
            j: J::initialize(&j_),
        };
        if j_.is_new() {
            jtrace_writer!(Initialized);
        } else {
            // not a new instance, so we must update the journal with a re-open event
            jtrace_writer!(ReinitializeAttempt);
            Self::reopen_driver(&mut me)?;
            jtrace_writer!(ReinitializeComplete);
        }
        Ok(me)
    }
    /// Commit a new event to the journal
    ///
    /// This will auto-flush the buffer and sync metadata as soon as the [`RawJournalAdapter::commit`] method returns,
    /// unless otherwise configured
    pub fn commit_event<'a>(&mut self, event: J::Event<'a>) -> RuntimeResult<()> {
        self.txn_context(|me, txn_id| {
            let ev_md = me.j.get_event_md(&event);
            jtrace_writer!(CommitAttemptForEvent(txn_id as u64));
            // MSB must be unused; set msb
            debug_assert!(ev_md & SERVER_EV_MASK != 1, "MSB must be unset");
            let ev_md = ev_md | SERVER_EV_MASK;
            // commit event
            let Self { j, log_file, .. } = me;
            match J::COMMIT_PREFERENCE {
                CommitPreference::Buffered => {
                    // explicitly buffer and then directly write to the file (without buffering)
                    let mut buf = Vec::with_capacity(J::EVENT_SIZE_BUFFER);
                    buf.extend(&txn_id.to_le_bytes());
                    buf.extend(&ev_md.to_le_bytes());
                    jtrace_writer!(CommitServerEventWroteMetadata);
                    j.commit_buffered(&mut buf, event);
                    log_file.tracked_write_through_buffer(&buf)?;
                }
                CommitPreference::Direct => {
                    // use the underlying buffer
                    // these writes won't actually reach disk
                    log_file.tracked_write(&txn_id.to_le_bytes())?;
                    log_file.tracked_write(&ev_md.to_le_bytes())?;
                    jtrace_writer!(CommitServerEventWroteMetadata);
                    // now hand over control to adapter impl
                    J::commit_direct::<Fs>(j, log_file, event)?;
                }
            }
            jtrace_writer!(CommitServerEventAdapterCompleted);
            if J::AUTO_SYNC_ON_EVENT_COMMIT {
                // should fsync after event
                log_file.flush_sync()?;
                jtrace_writer!(CommitCommitServerEventSyncCompleted);
            }
            Ok(())
        })
    }
}

impl<J: RawJournalAdapter, Fs: FSInterface> RawJournalWriter<J, Fs> {
    fn txn_context<T>(
        &mut self,
        f: impl FnOnce(&mut Self, u128) -> RuntimeResult<T>,
    ) -> RuntimeResult<T> {
        let id = self.txn_id;
        self.txn_id += 1;
        let ret = f(self, id as u128);
        if ret.is_ok() {
            self.known_txn_id = id;
            self.known_txn_offset = self.log_file.cursor();
        }
        ret
    }
    /// Commit a new driver event
    fn _commit_driver_event(me: &mut Self, kind: DriverEventKind) -> RuntimeResult<()> {
        jtrace_writer!(DriverEventAttemptCommit {
            event: kind,
            event_id: me.txn_id,
            prev_id: me.known_txn_id
        });
        me.txn_context(|me, txn_id| {
            let block = DriverEvent::encode(
                txn_id,
                kind,
                me.log_file.current_checksum(),
                me.known_txn_offset,
                me.known_txn_id,
            );
            if !J::AUTO_SYNC_ON_EVENT_COMMIT {
                // the log might still not be fully flushed, so flush it now; NB: flush does not affect checksum state;
                // this is guaranteed by the impl of the tracked writer
                me.log_file.flush_sync()?;
            }
            me.log_file.tracked_write_through_buffer(&block)?;
            jtrace_writer!(DriverEventCompleted);
            Ok(())
        })
    }
    /// Close driver
    pub fn close_driver(me: &mut Self) -> RuntimeResult<()> {
        Self::_commit_driver_event(me, DriverEventKind::Closed)?;
        jtrace_writer!(DriverClosed);
        Ok(())
    }
    /// Reopen driver
    pub fn reopen_driver(me: &mut Self) -> RuntimeResult<()> {
        Self::_commit_driver_event(me, DriverEventKind::Reopened)?;
        Ok(())
    }
}

pub struct RawJournalReader<J: RawJournalAdapter, Fs: FSInterface> {
    tr: TrackedReader<
        <<Fs as FSInterface>::File as FileInterface>::BufReader,
        <J as RawJournalAdapter>::Spec,
    >,
    txn_id: u64,
    last_txn_id: u64,
    last_txn_offset: u64,
    last_txn_checksum: u64,
}

impl<J: RawJournalAdapter, Fs: FSInterface> RawJournalReader<J, Fs> {
    pub fn scroll(
        file: SdssFile<<Fs as FSInterface>::File, <J as RawJournalAdapter>::Spec>,
        gs: &J::GlobalState,
    ) -> RuntimeResult<(
        JournalInitializer,
        SdssFile<<Fs as FSInterface>::File, J::Spec>,
    )> {
        let reader = TrackedReader::with_cursor(
            file,
            <<J as RawJournalAdapter>::Spec as FileSpecV1>::SIZE as u64,
        )?;
        jtrace_reader!(Initialized);
        let mut me = Self::new(reader, 0, 0, 0, 0);
        loop {
            if me._next_event(gs)? {
                jtrace_reader!(Completed);
                let initializer = JournalInitializer::new(
                    me.tr.cursor(),
                    me.tr.checksum(),
                    me.txn_id,
                    // NB: the last txn offset is important because it indicates that the log is new
                    me.last_txn_offset,
                );
                let file = me.tr.into_inner::<Fs::File>()?;
                return Ok((initializer, file));
            }
        }
    }
    fn new(
        reader: TrackedReader<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            <J as RawJournalAdapter>::Spec,
        >,
        txn_id: u64,
        last_txn_id: u64,
        last_txn_offset: u64,
        last_txn_checksum: u64,
    ) -> Self {
        Self {
            tr: reader,
            txn_id,
            last_txn_id,
            last_txn_offset,
            last_txn_checksum,
        }
    }
    fn __refresh_known_txn(me: &mut Self) {
        me.last_txn_id = me.txn_id;
        me.last_txn_checksum = me.tr.current_checksum();
        me.last_txn_offset = me.tr.cursor();
        me.txn_id += 1;
    }
}

impl<J: RawJournalAdapter, Fs: FSInterface> RawJournalReader<J, Fs> {
    fn _next_event(&mut self, gs: &J::GlobalState) -> RuntimeResult<bool> {
        let txn_id = u128::from_le_bytes(self.tr.read_block()?);
        let meta = u64::from_le_bytes(self.tr.read_block()?);
        if txn_id != self.txn_id as u128 {
            jtrace_reader!(ErrTxnIdMismatch {
                expected: self.txn_id,
                current: txn_id as u64
            });
            return Err(StorageError::RawJournalEventCorruptedMetadata.into());
        }
        jtrace_reader!(AttemptingEvent(txn_id as u64));
        // check for a server event
        // is this a server event?
        if meta & SERVER_EV_MASK != 0 {
            jtrace_reader!(DetectedServerEvent);
            let meta = meta & !SERVER_EV_MASK;
            match J::parse_event_meta(meta) {
                Some(meta) => {
                    jtrace_reader!(ServerEventMetadataParsed);
                    // now parse the actual event
                    let Self { tr: reader, .. } = self;
                    let event = J::parse_event::<Fs>(reader, meta)?;
                    jtrace_reader!(ServerEventParsed);
                    // we do not consider a parsed event a success signal; so we must actually apply it
                    match J::apply_event(gs, meta, event) {
                        Ok(()) => {
                            jtrace_reader!(ServerEventAppliedSuccess);
                            Self::__refresh_known_txn(self);
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    }
                }
                None => return Err(StorageError::RawJournalEventCorruptedMetadata.into()),
            }
        }
        return self.handle_close(txn_id, meta);
    }
    fn handle_close(
        &mut self,
        txn_id: u128,
        meta: u64,
    ) -> Result<bool, crate::engine::fractal::error::Error> {
        jtrace_reader!(DriverEventExpectingClose);
        // attempt to parse a driver close event
        let mut block = [0u8; DriverEvent::FULL_EVENT_SIZE];
        block[DriverEvent::OFFSET_0_TXN_ID].copy_from_slice(&txn_id.to_le_bytes());
        block[DriverEvent::OFFSET_1_EVENT_KIND].copy_from_slice(&meta.to_le_bytes());
        // now get remaining block
        self.tr
            .tracked_read(&mut block[DriverEvent::OFFSET_2_CHECKSUM.start..])?;
        jtrace_reader!(DriverEventCompletedBlockRead);
        // check the driver event
        let drv_close_event = match DriverEvent::decode(block) {
            Some(
                ev @ DriverEvent {
                    event: DriverEventKind::Closed,
                    ..
                },
            ) => ev,
            Some(DriverEvent {
                event: DriverEventKind::Reopened,
                ..
            }) => {
                jtrace_reader!(ErrExpectedCloseGotReopen);
                return Err(StorageError::RawJournalInvalidEvent.into());
            }
            None => return Err(StorageError::RawJournalEventCorrupted.into()),
        };
        jtrace_reader!(DriverEventExpectedCloseGotClose);
        // a driver closed event; we've checked integrity, but we must check the field values
        let valid_meta = okay! {
            self.last_txn_checksum == drv_close_event.last_checksum,
            self.last_txn_id == drv_close_event.last_txn_id,
            self.last_txn_offset == drv_close_event.last_offset,
        };
        if !valid_meta {
            jtrace_reader!(DriverEventInvalidMetadata);
            // either the block is corrupted or the data we read is corrupted; either way,
            // we're going to refuse to read this
            return Err(StorageError::RawJournalCorrupted.into());
        }
        // update
        Self::__refresh_known_txn(self);
        // full metadata validated; this is a valid close event but is it actually a close
        if self.tr.is_eof() {
            jtrace_reader!(ClosedAndReachedEof);
            // yes, we're done
            return Ok(true);
        }
        return self.handle_reopen();
    }
    fn handle_reopen(&mut self) -> RuntimeResult<bool> {
        jtrace_reader!(AttemptingEvent(self.txn_id as u64));
        jtrace_reader!(DriverEventExpectingReopenBlock);
        // now we must look for a reopen event
        let event_block = self.tr.read_block::<{ DriverEvent::FULL_EVENT_SIZE }>()?;
        let reopen_event = match DriverEvent::decode(event_block) {
            Some(ev) if ev.event == DriverEventKind::Reopened => ev,
            None | Some(_) => return Err(StorageError::RawJournalEventCorrupted.into()),
        };
        jtrace_reader!(DriverEventExpectingReopenGotReopen);
        let valid_meta = okay! {
            self.last_txn_checksum == reopen_event.last_checksum,
            self.last_txn_id == reopen_event.last_txn_id,
            self.last_txn_offset == reopen_event.last_offset,
            self.txn_id as u128 == reopen_event.txn_id,
        };
        if valid_meta {
            // valid meta, update all
            Self::__refresh_known_txn(self);
            jtrace_reader!(ReopenSuccess);
            Ok(false)
        } else {
            jtrace_reader!(ErrInvalidReopenMetadata);
            Err(StorageError::RawJournalCorrupted.into())
        }
    }
}
