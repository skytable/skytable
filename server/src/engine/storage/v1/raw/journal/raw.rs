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
    super::super::{rw::SDSSFileIO, spec::Header},
    crate::{
        engine::{
            error::{RuntimeResult, StorageError},
            storage::common::{
                interface::fs::{BufferedReader, File},
                sdss,
            },
        },
        util::{compiler, copy_a_into_b, copy_slice_to_array as memcpy},
    },
    std::marker::PhantomData,
};

const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

pub fn load_journal<TA: JournalAdapter, F: sdss::sdss_r1::FileSpecV1<DecodeArgs = ()>>(
    log_file_name: &str,
    gs: &TA::GlobalState,
) -> RuntimeResult<JournalWriter<TA>> {
    let (file, _) = SDSSFileIO::open::<F>(log_file_name)?;
    let (file, last_txn_id) = JournalReader::<TA>::scroll(file, gs)?;
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

pub struct JournalReader<TA> {
    log_file: SDSSFileIO<BufferedReader>,
    evid: u64,
    closed: bool,
    remaining_bytes: u64,
    _m: PhantomData<TA>,
}

impl<TA: JournalAdapter> JournalReader<TA> {
    pub fn new(log_file: SDSSFileIO<File>) -> RuntimeResult<Self> {
        let log_size = log_file.file_length()? - Header::SIZE as u64;
        Ok(Self {
            log_file: log_file.into_buffered_reader(),
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
            let new_cursor = self.log_file.file_cursor()? - JournalEntryMetadata::SIZE as u64;
            self.log_file.seek_from_start(new_cursor)?;
            return self.try_recover_journal_strategy_simple_reverse();
        }
        match entry_metadata
            .event_source_marker()
            .ok_or(StorageError::V1JournalDecodeLogEntryCorrupted)?
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
                return Err(StorageError::V1JournalDecodeCorrupted.into());
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
    fn handle_driver_reopen(&mut self) -> RuntimeResult<()> {
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
                Err(StorageError::V1JournalDecodeCorrupted.into())
            }
        } else {
            Err(StorageError::V1JournalDecodeCorrupted.into())
        }
    }
    #[cold] // FIXME(@ohsayan): how bad can prod systems be? (clue: pretty bad, so look for possible changes)
    #[inline(never)]
    /// attempt to recover the journal using the reverse directive (simple strategy)
    /// IMPORTANT: every event is unique so this must be called BEFORE the ID is incremented (remember that we only increment
    /// once we **sucessfully** finish processing a normal (aka server event origin) event and not a non-normal branch)
    fn try_recover_journal_strategy_simple_reverse(&mut self) -> RuntimeResult<()> {
        debug_assert!(TA::RECOVERY_PLUGIN, "recovery plugin not enabled");
        self.__record_read_bytes(JournalEntryMetadata::SIZE); // FIXME(@ohsayan): don't assume read length?
        let mut entry_buf = [0u8; JournalEntryMetadata::SIZE];
        if self.log_file.read_buffer(&mut entry_buf).is_err() {
            return Err(StorageError::V1JournalDecodeCorrupted.into());
        }
        let entry = JournalEntryMetadata::decode(entry_buf);
        let okay = (entry.event_id == self.evid as u128)
            & (entry.event_crc == 0)
            & (entry.event_payload_len == 0)
            & (entry.event_source_md == EventSourceMarker::RECOVERY_REVERSE_LAST_JOURNAL);
        self._incr_evid();
        if okay {
            return Ok(());
        } else {
            Err(StorageError::V1JournalDecodeCorrupted.into())
        }
    }
    /// Read and apply all events in the given log file to the global state, returning the (open file, last event ID)
    pub fn scroll(
        file: SDSSFileIO<File>,
        gs: &TA::GlobalState,
    ) -> RuntimeResult<(SDSSFileIO<File>, u64)> {
        let mut slf = Self::new(file)?;
        while !slf.end_of_file() {
            slf.rapply_next_event(gs)?;
        }
        if slf.closed {
            Ok((slf.log_file.downgrade_reader(), slf.evid))
        } else {
            Err(StorageError::V1JournalDecodeCorrupted.into())
        }
    }
}

impl<TA> JournalReader<TA> {
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

impl<TA> JournalReader<TA> {
    fn logfile_read_into_buffer(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
        if !self.has_remaining_bytes(buf.len() as _) {
            // do this right here to avoid another syscall
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        self.log_file.read_buffer(buf)?;
        self.__record_read_bytes(buf.len());
        Ok(())
    }
}

pub struct JournalWriter<TA> {
    /// the txn log file
    log_file: SDSSFileIO<File>,
    /// the id of the **next** journal
    id: u64,
    _m: PhantomData<TA>,
    closed: bool,
}

impl<TA: JournalAdapter> JournalWriter<TA> {
    pub fn new(mut log_file: SDSSFileIO<File>, last_txn_id: u64, new: bool) -> RuntimeResult<Self> {
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
}

impl<TA> JournalWriter<TA> {
    pub fn append_journal_reopen(&mut self) -> RuntimeResult<()> {
        let id = self._incr_id() as u128;
        e!(self.log_file.fsynced_write(
            &JournalEntryMetadata::new(id, EventSourceMarker::DRIVER_REOPENED, 0, 0).encoded(),
        ))
    }
    pub fn __close_mut(&mut self) -> RuntimeResult<()> {
        self.closed = true;
        let id = self._incr_id() as u128;
        self.log_file.fsynced_write(
            &JournalEntryMetadata::new(id, EventSourceMarker::DRIVER_CLOSED, 0, 0).encoded(),
        )?;
        Ok(())
    }
    pub fn close(mut self) -> RuntimeResult<()> {
        self.__close_mut()
    }
}

impl<TA> JournalWriter<TA> {
    fn _incr_id(&mut self) -> u64 {
        let current = self.id;
        self.id += 1;
        current
    }
}

impl<TA> Drop for JournalWriter<TA> {
    fn drop(&mut self) {
        assert!(self.closed, "log not closed");
    }
}
