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
        header_impl::{FileSpecifierVersion, HostRunMode, SDSSHeaderRaw},
        rw::{FileOpen, RawFileIOInterface, SDSSFileIO},
        SDSSError, SDSSResult,
    },
    crate::{
        engine::storage::v1::header_impl::{FileScope, FileSpecifier},
        util::{compiler, copy_a_into_b, copy_slice_to_array as memcpy, Threshold},
    },
    std::marker::PhantomData,
};

const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
const RECOVERY_BLOCK_AUTO_THRESHOLD: usize = 5;

pub fn open_journal<
    TA: JournalAdapter + core::fmt::Debug,
    LF: RawFileIOInterface + core::fmt::Debug,
>(
    log_file_name: &str,
    log_kind: FileSpecifier,
    log_kind_version: FileSpecifierVersion,
    host_setting_version: u32,
    host_run_mode: HostRunMode,
    host_startup_counter: u64,
    gs: &TA::GlobalState,
) -> SDSSResult<JournalWriter<LF, TA>> {
    let f = SDSSFileIO::<LF>::open_or_create_perm_rw(
        log_file_name,
        FileScope::Journal,
        log_kind,
        log_kind_version,
        host_setting_version,
        host_run_mode,
        host_startup_counter,
    )?;
    let file = match f {
        FileOpen::Created(f) => return JournalWriter::new(f, 0, true),
        FileOpen::Existing(file, _) => file,
    };
    let (file, last_txn) = JournalReader::<TA, LF>::scroll(file, gs)?;
    JournalWriter::new(file, last_txn, false)
}

/// The journal adapter
pub trait JournalAdapter {
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
}

impl JournalEntryMetadata {
    pub const fn is_server_event(&self) -> bool {
        self.event_source_md == EventSourceMarker::SERVER_STD
    }
    pub const fn is_driver_event(&self) -> bool {
        self.event_source_md <= 1
    }
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

#[derive(Debug)]
pub struct JournalReader<TA, LF> {
    log_file: SDSSFileIO<LF>,
    log_size: u64,
    evid: u64,
    closed: bool,
    remaining_bytes: u64,
    _m: PhantomData<TA>,
}

impl<TA: JournalAdapter, LF: RawFileIOInterface> JournalReader<TA, LF> {
    pub fn new(log_file: SDSSFileIO<LF>) -> SDSSResult<Self> {
        let log_size = log_file.file_length()? - SDSSHeaderRaw::header_size() as u64;
        Ok(Self {
            log_file,
            log_size,
            evid: 0,
            closed: false,
            remaining_bytes: log_size,
            _m: PhantomData,
        })
    }
    /// Read the next event and apply it to the global state
    pub fn rapply_next_event(&mut self, gs: &TA::GlobalState) -> SDSSResult<()> {
        // read metadata
        let mut en_jrnl_md = [0u8; JournalEntryMetadata::SIZE];
        self.logfile_read_into_buffer(&mut en_jrnl_md)?; // FIXME(@ohsayan): increase tolerance to not just payload
        let entry_metadata = JournalEntryMetadata::decode(en_jrnl_md);
        /*
            validate metadata:
            - evid
            - sourcemd
            - sum
            - len < alloc cap; FIXME(@ohsayan): more sensitive via alloc?
        */
        if self.evid != entry_metadata.event_id as u64 {
            // the only case when this happens is when the journal faults at runtime with a write zero (or some other error when no bytes were written)
            self.remaining_bytes += JournalEntryMetadata::SIZE as u64;
            // move back cursor to see if we have a recovery block
            let new_cursor = self.log_file.retrieve_cursor()? - JournalEntryMetadata::SIZE as u64;
            self.log_file.seek_from_start(new_cursor)?;
            return self.try_recover_journal_strategy_simple_reverse();
        }
        match entry_metadata
            .event_source_marker()
            .ok_or(SDSSError::JournalLogEntryCorrupted)?
        {
            EventSourceMarker::ServerStandard => {}
            EventSourceMarker::DriverClosed => {
                // is this a real close?
                if self.end_of_file() {
                    self.closed = true;
                    return Ok(());
                } else {
                    return self.handle_driver_reopen();
                }
            }
            EventSourceMarker::DriverReopened | EventSourceMarker::RecoveryReverseLastJournal => {
                // these two are only taken in close and error paths (respectively) so we shouldn't see them here; this is bad
                // two special directives in the middle of nowhere? incredible
                return Err(SDSSError::JournalCorrupted);
            }
        }
        // read payload
        if compiler::unlikely(!self.has_remaining_bytes(entry_metadata.event_payload_len)) {
            return compiler::cold_call(|| self.try_recover_journal_strategy_simple_reverse());
        }
        let mut payload = vec![0; entry_metadata.event_payload_len as usize];
        self.logfile_read_into_buffer(&mut payload)?; // exit jump -> we checked if enough data is there, but the read failed so this is not our business
        if compiler::unlikely(CRC.checksum(&payload) != entry_metadata.event_crc) {
            return compiler::cold_call(|| self.try_recover_journal_strategy_simple_reverse());
        }
        if compiler::unlikely(TA::decode_and_update_state(&payload, gs).is_err()) {
            return compiler::cold_call(|| self.try_recover_journal_strategy_simple_reverse());
        }
        self._incr_evid();
        Ok(())
    }
    /// handle a driver reopen (IMPORTANT: every event is unique so this must be called BEFORE the ID is incremented)
    fn handle_driver_reopen(&mut self) -> SDSSResult<()> {
        if self.has_remaining_bytes(JournalEntryMetadata::SIZE as _) {
            let mut reopen_block = [0u8; JournalEntryMetadata::SIZE];
            self.logfile_read_into_buffer(&mut reopen_block)?; // exit jump -> not our business since we have checked flen and if it changes due to user intervention, that's a you problem
            let md = JournalEntryMetadata::decode(reopen_block);
            if (md.event_id as u64 == self.evid)
                & (md.event_crc == 0)
                & (md.event_payload_len == 0)
                & (md.event_source_md == EventSourceMarker::DRIVER_REOPENED)
            {
                self._incr_evid();
                Ok(())
            } else {
                // FIXME(@ohsayan): tolerate loss in this directive too
                Err(SDSSError::JournalCorrupted)
            }
        } else {
            Err(SDSSError::JournalCorrupted)
        }
    }
    #[cold] // FIXME(@ohsayan): how bad can prod systems be? (clue: pretty bad, so look for possible changes)
    #[inline(never)]
    /// attempt to recover the journal using the reverse directive (simple strategy)
    /// IMPORTANT: every event is unique so this must be called BEFORE the ID is incremented (remember that we only increment
    /// once we **sucessfully** finish processing a normal (aka server event origin) event and not a non-normal branch)
    fn try_recover_journal_strategy_simple_reverse(&mut self) -> SDSSResult<()> {
        debug_assert!(TA::RECOVERY_PLUGIN, "recovery plugin not enabled");
        let mut threshold = Threshold::<RECOVERY_BLOCK_AUTO_THRESHOLD>::new();
        while threshold.not_busted() & self.has_remaining_bytes(JournalEntryMetadata::SIZE as _) {
            self.__record_read_bytes(JournalEntryMetadata::SIZE); // FIXME(@ohsayan): don't assume read length?
            let mut entry_buf = [0u8; JournalEntryMetadata::SIZE];
            if self.log_file.read_to_buffer(&mut entry_buf).is_err() {
                threshold.bust_one();
                continue;
            }
            let entry = JournalEntryMetadata::decode(entry_buf);
            let okay = (entry.event_id == self.evid as u128)
                & (entry.event_crc == 0)
                & (entry.event_payload_len == 0)
                & (entry.event_source_md == EventSourceMarker::RECOVERY_REVERSE_LAST_JOURNAL);
            if okay {
                return Ok(());
            }
            self._incr_evid();
            threshold.bust_one();
        }
        Err(SDSSError::JournalCorrupted)
    }
    /// Read and apply all events in the given log file to the global state, returning the (open file, last event ID)
    pub fn scroll(file: SDSSFileIO<LF>, gs: &TA::GlobalState) -> SDSSResult<(SDSSFileIO<LF>, u64)> {
        let mut slf = Self::new(file)?;
        while !slf.end_of_file() {
            slf.rapply_next_event(gs)?;
        }
        if slf.closed {
            Ok((slf.log_file, slf.evid))
        } else {
            Err(SDSSError::JournalCorrupted)
        }
    }
}

impl<TA, LF> JournalReader<TA, LF> {
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

impl<TA, LF: RawFileIOInterface> JournalReader<TA, LF> {
    fn logfile_read_into_buffer(&mut self, buf: &mut [u8]) -> SDSSResult<()> {
        if !self.has_remaining_bytes(buf.len() as _) {
            // do this right here to avoid another syscall
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        self.log_file.read_to_buffer(buf)?;
        self.__record_read_bytes(buf.len());
        Ok(())
    }
}

#[derive(Debug)]
pub struct JournalWriter<LF, TA> {
    /// the txn log file
    log_file: SDSSFileIO<LF>,
    /// the id of the **next** journal
    id: u64,
    _m: PhantomData<TA>,
    closed: bool,
}

impl<LF: RawFileIOInterface, TA: JournalAdapter> JournalWriter<LF, TA> {
    pub fn new(mut log_file: SDSSFileIO<LF>, last_txn_id: u64, new: bool) -> SDSSResult<Self> {
        let log_size = log_file.file_length()?;
        log_file.seek_from_start(log_size)?; // avoid jumbling with headers
        let mut slf = Self {
            log_file,
            id: last_txn_id,
            _m: PhantomData,
            closed: false,
        };
        if !new {
            // IMPORTANT: don't forget this; otherwise the journal reader will report a false error!
            slf.append_journal_reopen()?;
        }
        Ok(slf)
    }
    pub fn append_event(&mut self, event: TA::JournalEvent) -> SDSSResult<()> {
        let encoded = TA::encode(event);
        let md = JournalEntryMetadata::new(
            self._incr_id() as u128,
            EventSourceMarker::SERVER_STD,
            CRC.checksum(&encoded),
            encoded.len() as u64,
        )
        .encoded();
        self.log_file.unfsynced_write(&md)?;
        self.log_file.unfsynced_write(&encoded)?;
        self.log_file.fsync_all()?;
        Ok(())
    }
    pub fn append_event_with_recovery_plugin(&mut self, event: TA::JournalEvent) -> SDSSResult<()> {
        debug_assert!(TA::RECOVERY_PLUGIN);
        match self.append_event(event) {
            Ok(()) => Ok(()),
            Err(_) => {
                return self.appendrec_journal_reverse_entry();
            }
        }
    }
}

impl<LF: RawFileIOInterface, TA> JournalWriter<LF, TA> {
    pub fn appendrec_journal_reverse_entry(&mut self) -> SDSSResult<()> {
        let mut threshold = Threshold::<RECOVERY_BLOCK_AUTO_THRESHOLD>::new();
        let mut entry =
            JournalEntryMetadata::new(0, EventSourceMarker::RECOVERY_REVERSE_LAST_JOURNAL, 0, 0);
        while threshold.not_busted() {
            entry.event_id = self._incr_id() as u128;
            if self.log_file.fsynced_write(&entry.encoded()).is_ok() {
                return Ok(());
            }
            threshold.bust_one();
        }
        Err(SDSSError::JournalWRecoveryStageOneFailCritical)
    }
    pub fn append_journal_reopen(&mut self) -> SDSSResult<()> {
        let id = self._incr_id() as u128;
        self.log_file.fsynced_write(
            &JournalEntryMetadata::new(id, EventSourceMarker::DRIVER_REOPENED, 0, 0).encoded(),
        )
    }
    pub fn append_journal_close_and_close(mut self) -> SDSSResult<()> {
        self.closed = true;
        let id = self._incr_id() as u128;
        self.log_file.fsynced_write(
            &JournalEntryMetadata::new(id, EventSourceMarker::DRIVER_CLOSED, 0, 0).encoded(),
        )?;
        Ok(())
    }
}

impl<LF, TA> JournalWriter<LF, TA> {
    fn _incr_id(&mut self) -> u64 {
        let current = self.id;
        self.id += 1;
        current
    }
}

impl<LF, TA> Drop for JournalWriter<LF, TA> {
    fn drop(&mut self) {
        assert!(self.closed, "log not closed");
    }
}
