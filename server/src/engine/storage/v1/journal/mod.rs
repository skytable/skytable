/*
 * Created on Sat Jul 29 2023
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

/*
  +----------------+------------------------------+------------------+------------------+--------------------+
  | EVENT ID (16B) | EVENT SOURCE + METADATA (8B) | EVENT CRC32 (4B) | PAYLOAD LEN (8B) | EVENT PAYLOAD (?B) |
  +----------------+------------------------------+------------------+------------------+--------------------+
  Event ID:
  - The atomically incrementing event ID (for future scale we have 16B; it's like the ZFS situation haha)
  - Event source (1B) + 7B padding (for future metadata)
  - Event CRC32
  - Payload len: the size of the pyload
  - Payload: the payload


  Notes on error tolerance:
  - FIXME(@ohsayan): we currently expect atleast 36 bytes of the signature to be present. this is not great
  - FIXME(@ohsayan): we will probably (naively) need to dynamically reposition the cursor in case the metadata is corrupted as well
*/

use {
    super::{
        rw::{RawFSInterface, SDSSFileIO},
        spec,
    },
    crate::{
        engine::error::{RuntimeResult, StorageError},
        util::{compiler, copy_a_into_b, copy_slice_to_array as memcpy},
    },
    std::marker::PhantomData,
};

const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

/*
    emulation and tracing
*/
#[macro_use]
pub mod emulation_tracing;

#[cfg(test)]
pub fn open_or_create_journal<TA: JournalAdapter, Fs: RawFSInterface, F: spec::FileSpec>(
    log_file_name: &str,
    gs: &TA::GlobalState,
) -> RuntimeResult<super::rw::FileOpen<JournalWriter<Fs, TA>>> {
    use super::rw::FileOpen;
    let file = match SDSSFileIO::<Fs>::open_or_create_perm_rw::<F>(log_file_name)? {
        FileOpen::Created(f) => {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalEventTrace::InitCreated,
            );
            return Ok(FileOpen::Created(JournalWriter::new(f, 0, true)?));
        }
        FileOpen::Existing((file, _header)) => {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalEventTrace::InitRestored,
            );
            file
        }
    };
    let (file, last_txn) = JournalReader::<TA, Fs>::scroll(file, gs)?;
    Ok(FileOpen::Existing(JournalWriter::new(
        file, last_txn, false,
    )?))
}

/*
    journal load
*/

/// Create a new journal
pub fn create_journal<TA: JournalAdapter, Fs: RawFSInterface, F: spec::FileSpec>(
    log_file_name: &str,
) -> RuntimeResult<JournalWriter<Fs, TA>> {
    JournalWriter::new(SDSSFileIO::create::<F>(log_file_name)?, 0, true)
}

/// Attempt to load a journal
pub fn load_journal<TA: JournalAdapter, Fs: RawFSInterface, F: spec::FileSpec>(
    log_file_name: &str,
    gs: &TA::GlobalState,
) -> RuntimeResult<JournalWriter<Fs, TA>> {
    let (file, _) = SDSSFileIO::<Fs>::open::<F>(log_file_name)?;
    let (file, last_txn_id) = JournalReader::<TA, Fs>::scroll(file, gs)?;
    JournalWriter::new(file, last_txn_id, false)
}

/// The journal adapter
pub trait JournalAdapter {
    /// deny any SDSS file level operations that require non-append mode writes (for example, updating the SDSS header's modify count)
    const DENY_NONAPPEND: bool = true;
    /// enable/disable automated recovery algorithms
    const RECOVERY_PLUGIN: bool;
    /// The journal event
    type JournalEvent;
    /// The global state, which we want to modify on decoding the event
    type GlobalState;
    /// The transactional impl that makes use of this journal, should define it's error type
    type Error;
    /// Encode a journal event into a blob
    fn encode(event: Self::JournalEvent) -> Box<[u8]>;
    /// Decode a journal event and apply it to the global state
    fn decode_and_update_state(payload: &[u8], gs: &Self::GlobalState) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct JournalEntryMetadata {
    event_id: u128,
    event_source_md: u64,
    event_crc: u32,
    event_payload_len: u64,
}

impl JournalEntryMetadata {
    const SIZE: usize = sizeof!(u128) + sizeof!(u64) + sizeof!(u32) + sizeof!(u64);
    const P0: usize = 0;
    const P1: usize = sizeof!(u128);
    const P2: usize = Self::P1 + sizeof!(u64);
    const P3: usize = Self::P2 + sizeof!(u32);
    pub const fn new(
        event_id: u128,
        event_source_md: u64,
        event_crc: u32,
        event_payload_len: u64,
    ) -> Self {
        Self {
            event_id,
            event_source_md,
            event_crc,
            event_payload_len,
        }
    }
    /// Encodes the log entry metadata
    pub const fn encoded(&self) -> [u8; JournalEntryMetadata::SIZE] {
        let mut encoded = [0u8; JournalEntryMetadata::SIZE];
        encoded = copy_a_into_b(self.event_id.to_le_bytes(), encoded, Self::P0);
        encoded = copy_a_into_b(self.event_source_md.to_le_bytes(), encoded, Self::P1);
        encoded = copy_a_into_b(self.event_crc.to_le_bytes(), encoded, Self::P2);
        encoded = copy_a_into_b(self.event_payload_len.to_le_bytes(), encoded, Self::P3);
        encoded
    }
    /// Decodes the log entry metadata (essentially a simply type transmutation)
    pub fn decode(data: [u8; JournalEntryMetadata::SIZE]) -> Self {
        Self::new(
            u128::from_le_bytes(memcpy(&data[..Self::P1])),
            u64::from_le_bytes(memcpy(&data[Self::P1..Self::P2])),
            u32::from_le_bytes(memcpy(&data[Self::P2..Self::P3])),
            u64::from_le_bytes(memcpy(&data[Self::P3..])),
        )
    }
}

/*
    Event source:
    * * * * _ * * * *

    b1 (s+d): event source (unset -> driver, set -> server)
    b* -> unused. MUST be unset
    b7 (d):
        - set: [recovery] reverse journal event
    b8 (d):
        - unset: closed log
        - set: reopened log
*/

pub enum EventSourceMarker {
    ServerStandard,
    DriverClosed,
    RecoveryReverseLastJournal,
    DriverReopened,
}

impl EventSourceMarker {
    const SERVER_STD: u64 = 1 << 63;
    const DRIVER_CLOSED: u64 = 0;
    const DRIVER_REOPENED: u64 = 1;
    const RECOVERY_REVERSE_LAST_JOURNAL: u64 = 2;
    pub const __ILLEGAL: u64 = 0xFFFFFFFFFFFFFFFF;
}

impl JournalEntryMetadata {
    pub const fn event_source_marker(&self) -> Option<EventSourceMarker> {
        Some(match self.event_source_md {
            EventSourceMarker::SERVER_STD => EventSourceMarker::ServerStandard,
            EventSourceMarker::DRIVER_CLOSED => EventSourceMarker::DriverClosed,
            EventSourceMarker::DRIVER_REOPENED => EventSourceMarker::DriverReopened,
            EventSourceMarker::RECOVERY_REVERSE_LAST_JOURNAL => {
                EventSourceMarker::RecoveryReverseLastJournal
            }
            _ => return None,
        })
    }
}

pub struct JournalReader<TA, Fs: RawFSInterface> {
    log_file: SDSSFileIO<Fs>,
    evid: u64,
    closed: bool,
    remaining_bytes: u64,
    _m: PhantomData<TA>,
}

impl<TA: JournalAdapter, Fs: RawFSInterface> JournalReader<TA, Fs> {
    pub fn new(log_file: SDSSFileIO<Fs>) -> RuntimeResult<Self> {
        let log_size = log_file.file_length()? - spec::SDSSStaticHeaderV1Compact::SIZE as u64;
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalReaderTraceEvent::Initialized,
        );
        Ok(Self {
            log_file,
            evid: 0,
            closed: false,
            remaining_bytes: log_size,
            _m: PhantomData,
        })
    }
    /// Read the next event and apply it to the global state
    pub fn rapply_next_event(&mut self, gs: &TA::GlobalState) -> RuntimeResult<()> {
        // read metadata
        let mut en_jrnl_md = [0u8; JournalEntryMetadata::SIZE];
        self.logfile_read_into_buffer(&mut en_jrnl_md)?; // FIXME(@ohsayan): increase tolerance to not just payload
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalReaderTraceEvent::EntryReadRawMetadata,
        );
        let entry_metadata = JournalEntryMetadata::decode(en_jrnl_md);
        /*
            validate metadata:
            - evid
            - sourcemd
            - sum
            - len < alloc cap; FIXME(@ohsayan): more sensitive via alloc?
        */
        if self.evid != entry_metadata.event_id as u64 {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::IffyEventIDMismatch(
                    entry_metadata.event_id,
                ),
            );
            // the only case when this happens is when the journal faults at runtime with a write zero (or some other error when no bytes were written)
            self.remaining_bytes += JournalEntryMetadata::SIZE as u64;
            // move back cursor to see if we have a recovery block
            let new_cursor = self.log_file.retrieve_cursor()? - JournalEntryMetadata::SIZE as u64;
            self.log_file.seek_from_start(new_cursor)?;
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceRecovery::InitialCursorRestoredForRecoveryBlockCheck,
            );
            return self.try_recover_journal_strategy_simple_reverse(true);
        }
        match entry_metadata
            .event_source_marker()
            .ok_or(StorageError::JournalLogEntryCorrupted)?
        {
            EventSourceMarker::ServerStandard => emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::EventKindStandard(
                    entry_metadata.event_id,
                ),
            ),
            EventSourceMarker::DriverClosed => {
                emulation_tracing::__journal_evtrace(
                    emulation_tracing::_JournalReaderTraceEvent::HitClose(entry_metadata.event_id),
                );
                self._incr_evid(); // a close is also an event!
                                   // is this a real close?
                if self.end_of_file() {
                    emulation_tracing::__journal_evtrace(
                        emulation_tracing::_JournalReaderTraceEvent::EOF,
                    );
                    self.closed = true;
                    return Ok(());
                } else {
                    emulation_tracing::__journal_evtrace(
                        emulation_tracing::_JournalReaderTraceEvent::IffyReopen,
                    );
                    return self.handle_driver_reopen();
                }
            }
            EventSourceMarker::DriverReopened | EventSourceMarker::RecoveryReverseLastJournal => {
                emulation_tracing::__journal_evtrace(
                    emulation_tracing::_JournalReaderTraceEvent::ErrorUnexpectedEvent,
                );
                // these two are only taken in close and error paths (respectively) so we shouldn't see them here; this is bad
                // two special directives in the middle of nowhere? incredible
                return Err(StorageError::JournalCorrupted.into());
            }
        }
        // read payload
        if compiler::unlikely(!self.has_remaining_bytes(entry_metadata.event_payload_len)) {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::ErrorExpectedPayloadButEOF,
            );
            return compiler::cold_call(|| self.try_recover_journal_strategy_simple_reverse(true));
        }
        let mut payload = vec![0; entry_metadata.event_payload_len as usize];
        self.logfile_read_into_buffer(&mut payload)?; // exit jump -> we checked if enough data is there, but the read failed so this is not our business
        if compiler::unlikely(CRC.checksum(&payload) != entry_metadata.event_crc) {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::ErrorChecksumMismatch,
            );
            return compiler::cold_call(|| self.try_recover_journal_strategy_simple_reverse(true));
        }
        if compiler::unlikely(TA::decode_and_update_state(&payload, gs).is_err()) {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::ErrorFailedToApplyEvent,
            );
            return compiler::cold_call(|| self.try_recover_journal_strategy_simple_reverse(true));
        }
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalReaderTraceEvent::CompletedEvent,
        );
        self._incr_evid();
        Ok(())
    }
    /// handle a driver reopen (IMPORTANT: every event is unique so this must be called BEFORE the ID is incremented)
    fn handle_driver_reopen(&mut self) -> RuntimeResult<()> {
        if self.has_remaining_bytes(JournalEntryMetadata::SIZE as _) {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::ReopenCheck,
            );
            let mut reopen_block = [0u8; JournalEntryMetadata::SIZE];
            self.logfile_read_into_buffer(&mut reopen_block)?; // exit jump -> not our business since we have checked flen and if it changes due to user intervention, that's a you problem
            let md = JournalEntryMetadata::decode(reopen_block);
            if (md.event_id as u64 == self.evid)
                & (md.event_crc == 0)
                & (md.event_payload_len == 0)
                & (md.event_source_md == EventSourceMarker::DRIVER_REOPENED)
            {
                emulation_tracing::__journal_evtrace(
                    emulation_tracing::_JournalReaderTraceEvent::ReopenSuccess(self.evid),
                );
                self._incr_evid();
                Ok(())
            } else {
                // FIXME(@ohsayan): tolerate loss in this directive too
                emulation_tracing::__journal_evtrace(
                    emulation_tracing::_JournalReaderTraceEvent::ErrorReopenFailedBadBlock,
                );
                Err(StorageError::JournalCorrupted.into())
            }
        } else {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::ErrorExpectedReopenGotEOF,
            );
            Err(StorageError::JournalCorrupted.into())
        }
    }
    #[cold] // FIXME(@ohsayan): how bad can prod systems be? (clue: pretty bad, so look for possible changes)
    #[inline(never)]
    /// attempt to recover the journal using the reverse directive (simple strategy)
    /// IMPORTANT: every event is unique so this must be called BEFORE the ID is incremented (remember that we only increment
    /// once we **sucessfully** finish processing a normal (aka server event origin) event and not a non-normal branch)
    fn try_recover_journal_strategy_simple_reverse(
        &mut self,
        increment: bool,
    ) -> RuntimeResult<()> {
        debug_assert!(TA::RECOVERY_PLUGIN, "recovery plugin not enabled");
        if increment {
            self._incr_evid();
        }
        self.__record_read_bytes(JournalEntryMetadata::SIZE); // FIXME(@ohsayan): don't assume read length?
        let mut entry_buf = [0u8; JournalEntryMetadata::SIZE];
        if self.log_file.read_to_buffer(&mut entry_buf).is_err() {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceRecovery::ExitWithFailedToReadBlock,
            );
            return Err(StorageError::JournalCorrupted.into());
        }
        let entry = JournalEntryMetadata::decode(entry_buf);
        let okay = (entry.event_id == self.evid as u128)
            & (entry.event_crc == 0)
            & (entry.event_payload_len == 0)
            & (entry.event_source_md == EventSourceMarker::RECOVERY_REVERSE_LAST_JOURNAL);
        self._incr_evid();
        if okay {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceRecovery::Success(entry.event_id as _),
            );
            return Ok(());
        } else {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceRecovery::ExitWithInvalidBlock,
            );
            Err(StorageError::JournalCorrupted.into())
        }
    }
    /// Read and apply all events in the given log file to the global state, returning the (open file, last event ID)
    pub fn scroll(
        file: SDSSFileIO<Fs>,
        gs: &TA::GlobalState,
    ) -> RuntimeResult<(SDSSFileIO<Fs>, u64)> {
        let mut slf = Self::new(file)?;
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalReaderTraceEvent::BeginEventsScan,
        );
        while !slf.end_of_file() {
            slf.rapply_next_event(gs)?;
        }
        if slf.closed {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::Success,
            );
            Ok((slf.log_file, slf.evid))
        } else {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalReaderTraceEvent::ErrorUnclosed,
            );
            Err(StorageError::JournalCorrupted.into())
        }
    }
}

impl<TA, Fs: RawFSInterface> JournalReader<TA, Fs> {
    fn _incr_evid(&mut self) {
        self.evid += 1;
    }
    fn __record_read_bytes(&mut self, cnt: usize) {
        self.remaining_bytes -= cnt as u64;
    }
    fn has_remaining_bytes(&self, size: u64) -> bool {
        self.remaining_bytes >= size
    }
    fn end_of_file(&self) -> bool {
        self.remaining_bytes == 0
    }
}

impl<TA, Fs: RawFSInterface> JournalReader<TA, Fs> {
    fn logfile_read_into_buffer(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
        if !self.has_remaining_bytes(buf.len() as _) {
            // do this right here to avoid another syscall
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        self.log_file.read_to_buffer(buf)?;
        self.__record_read_bytes(buf.len());
        Ok(())
    }
}

pub struct JournalWriter<Fs: RawFSInterface, TA> {
    /// the txn log file
    log_file: SDSSFileIO<Fs>,
    /// the id of the **next** journal
    id: u64,
    _m: PhantomData<TA>,
    closed: bool,
}

impl<Fs: RawFSInterface, TA: JournalAdapter> JournalWriter<Fs, TA> {
    pub fn new(mut log_file: SDSSFileIO<Fs>, last_txn_id: u64, new: bool) -> RuntimeResult<Self> {
        let log_size = log_file.file_length()?;
        log_file.seek_from_start(log_size)?; // avoid jumbling with headers
        let mut slf = Self {
            log_file,
            id: last_txn_id,
            _m: PhantomData,
            closed: false,
        };
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalWriterTraceEvent::Initialized,
        );
        if !new {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalWriterTraceEvent::Reinitializing,
            );
            // IMPORTANT: don't forget this; otherwise the journal reader will report a false error!
            slf.append_journal_reopen()?;
        }
        Ok(slf)
    }
    pub fn append_event(&mut self, event: TA::JournalEvent) -> RuntimeResult<()> {
        let encoded = TA::encode(event);
        let next_id = self._incr_id();
        let md = JournalEntryMetadata::new(
            /*
                for all the emulated faults here, an individual section of the metadata is intentionally
                written to be invalid
            */
            __inject_during_test_or!(if let @EventIDCorrupted {
                emulation_tracing::__journal_evtrace(emulation_tracing::_JournalWriterInjectedWith::BadEventID);
                (next_id - 1) as u128
            } else {
                // REAL
                next_id as u128
            }),
            __inject_during_test_or!(if let @EventSourceCorrupted {
                emulation_tracing::__journal_evtrace(emulation_tracing::_JournalWriterInjectedWith::BadSource);
                EventSourceMarker::__ILLEGAL
            } else {
                // REAL
                EventSourceMarker::SERVER_STD
            }),
            __inject_during_test_or!(if let @EventChecksumCorrupted {
                emulation_tracing::__journal_evtrace(emulation_tracing::_JournalWriterInjectedWith::BadChecksum);
                0
            } else {
                // REAL
                CRC.checksum(&encoded)
            }),
            __inject_during_test_or!(if let @EventPayloadLenIsGreaterBy(gt_by) {
                emulation_tracing::__journal_evtrace(emulation_tracing::_JournalWriterInjectedWith::BadPayloadLen(gt_by + encoded.len() as u64));
                encoded.len() as u64 + gt_by
            } else {
                // REAL
                encoded.len() as u64
            }),
        )
        .encoded();
        self.log_file.unfsynced_write(&md)?;
        __inject_during_test_or!(if let @PayloadMissing {
            emulation_tracing::__journal_evtrace(emulation_tracing::_JournalWriterInjectedWith::MissingPayload);
            Ok(())
        } else {
            // REAL
            self.log_file.unfsynced_write(&encoded)
        })?;
        self.log_file.fsync_all()?;
        if emulation_tracing::__is_emulated_fault() {
            // a fault was emulated, so we should append a recovery block
            #[cfg(test)]
            if local_ref!(emulation_tracing::_EMULATE_ALLOW_RECOVER, |v| *v) {
                // yes, we are allowed to add a recovery block
                return self.appendrec_journal_reverse_entry();
            }
        }
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalWriterTraceEvent::CompletedEventAppend((self.id - 1) as _),
        );
        Ok(())
    }
    pub fn append_event_with_recovery_plugin(
        &mut self,
        event: TA::JournalEvent,
    ) -> RuntimeResult<()> {
        debug_assert!(TA::RECOVERY_PLUGIN);
        match self.append_event(event) {
            Ok(()) => Ok(()),
            Err(e) => compiler::cold_call(move || {
                emulation_tracing::__journal_evtrace(
                    emulation_tracing::_JournalWriterTraceEvent::ErrorAddingNewEvent,
                );
                // IMPORTANT: we still need to return an error so that the caller can retry if deemed appropriate
                self.appendrec_journal_reverse_entry()?;
                Err(e)
            }),
        }
    }
}

impl<Fs: RawFSInterface, TA> JournalWriter<Fs, TA> {
    pub fn appendrec_journal_reverse_entry(&mut self) -> RuntimeResult<()> {
        let mut entry =
            JournalEntryMetadata::new(0, EventSourceMarker::RECOVERY_REVERSE_LAST_JOURNAL, 0, 0);
        entry.event_id = self._incr_id() as u128;
        if self.log_file.fsynced_write(&entry.encoded()).is_ok() {
            emulation_tracing::__journal_evtrace(
                emulation_tracing::_JournalWriterTraceEvent::RecoveryEventAdded(
                    entry.event_id as _,
                ),
            );
            return Ok(());
        }
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalWriterTraceEvent::ErrorRecoveryFailed,
        );
        Err(StorageError::JournalWRecoveryStageOneFailCritical.into())
    }
    pub fn append_journal_reopen(&mut self) -> RuntimeResult<()> {
        let id = self._incr_id() as u128;
        emulation_tracing::__journal_evtrace(
            emulation_tracing::_JournalWriterTraceEvent::Reopened(id as _),
        );
        self.log_file.fsynced_write(
            &JournalEntryMetadata::new(id, EventSourceMarker::DRIVER_REOPENED, 0, 0).encoded(),
        )
    }
    pub fn __close_mut(&mut self) -> RuntimeResult<()> {
        self.closed = true;
        let id = self._incr_id() as u128;
        self.log_file.fsynced_write(
            &JournalEntryMetadata::new(id, EventSourceMarker::DRIVER_CLOSED, 0, 0).encoded(),
        )?;
        emulation_tracing::__journal_evtrace(emulation_tracing::_JournalWriterTraceEvent::Closed(
            id as u64,
        ));
        Ok(())
    }
    pub fn close(mut self) -> RuntimeResult<()> {
        self.__close_mut()
    }
}

impl<Fs: RawFSInterface, TA> JournalWriter<Fs, TA> {
    fn _incr_id(&mut self) -> u64 {
        let current = self.id;
        self.id += 1;
        current
    }
}

impl<Fs: RawFSInterface, TA> Drop for JournalWriter<Fs, TA> {
    fn drop(&mut self) {
        assert!(self.closed, "log not closed");
    }
}
