/*
 * Created on Thu Jul 23 2023
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
*/

use {
    super::{
        rw::{RawFileIOInterface, SDSSFileIO},
        SDSSError, SDSSResult,
    },
    crate::util::{compiler, copy_a_into_b, copy_slice_to_array as memcpy},
    std::marker::PhantomData,
};

/// The transaction adapter
pub trait TransactionLogAdapter {
    /// The transaction event
    type TransactionEvent;
    /// The global state, which we want to modify on decoding the event
    type GlobalState;
    /// Encode a transaction event into a blob
    fn encode(event: Self::TransactionEvent) -> Box<[u8]>;
    /// Decode a transaction event and apply it to the global state
    fn decode_and_update_state(payload: &[u8], gs: &Self::GlobalState) -> SDSSResult<()>;
}

pub struct TxnLogEntryMetadata {
    event_id: u128,
    event_source_md: u64,
    event_crc: u32,
    event_payload_len: u64,
}

impl TxnLogEntryMetadata {
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
    pub const fn encoded(&self) -> [u8; TxnLogEntryMetadata::SIZE] {
        let mut encoded = [0u8; TxnLogEntryMetadata::SIZE];
        encoded = copy_a_into_b(self.event_id.to_le_bytes(), encoded, Self::P0);
        encoded = copy_a_into_b(self.event_source_md.to_le_bytes(), encoded, Self::P1);
        encoded = copy_a_into_b(self.event_crc.to_le_bytes(), encoded, Self::P2);
        encoded = copy_a_into_b(self.event_payload_len.to_le_bytes(), encoded, Self::P3);
        encoded
    }
    /// Decodes the log entry metadata (essentially a simply type transmutation)
    pub fn decode(data: [u8; TxnLogEntryMetadata::SIZE]) -> Self {
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
    b8 (d):
        - unset: closed log
*/
pub enum EventSourceMarker {
    ServerStandard,
    DriverClosed,
}

impl EventSourceMarker {
    const SERVER_STD: u64 = 1 << 63;
    const DRIVER_CLOSED: u64 = 0;
}

impl TxnLogEntryMetadata {
    pub const fn is_server_event(&self) -> bool {
        self.event_source_md == EventSourceMarker::SERVER_STD
    }
    pub const fn is_driver_event(&self) -> bool {
        self.event_source_md <= 1
    }
    pub const fn event_source_marker(&self) -> Option<EventSourceMarker> {
        Some(match self.event_source_md {
            EventSourceMarker::DRIVER_CLOSED => EventSourceMarker::DriverClosed,
            EventSourceMarker::SERVER_STD => EventSourceMarker::ServerStandard,
            _ => return None,
        })
    }
}

#[derive(Debug)]
pub struct TransactionLogReader<TA, LF> {
    log_file: SDSSFileIO<LF>,
    evid: u64,
    closed: bool,
    remaining_bytes: u64,
    _m: PhantomData<TA>,
}

impl<TA: TransactionLogAdapter, LF: RawFileIOInterface> TransactionLogReader<TA, LF> {
    pub fn new(log_file: SDSSFileIO<LF>) -> SDSSResult<Self> {
        let log_size = log_file.file_length()?;
        Ok(Self {
            log_file,
            evid: 0,
            closed: false,
            remaining_bytes: log_size,
            _m: PhantomData,
        })
    }
    /// Read the next event and apply it to the global state
    pub fn rapply_next_event(&mut self, gs: &TA::GlobalState) -> SDSSResult<()> {
        self._incr_evid();
        // read metadata
        let mut raw_txn_log_row_md = [0u8; TxnLogEntryMetadata::SIZE];
        self.log_file.read_to_buffer(&mut raw_txn_log_row_md)?;
        let event_metadata = TxnLogEntryMetadata::decode(raw_txn_log_row_md);
        /*
            verify metadata and read bytes into buffer, verify sum
        */
        // verify md
        let event_src_marker = event_metadata.event_source_marker();
        let okay = (self.evid == (event_metadata.event_id as _))
            & event_src_marker.is_some()
            & (event_metadata.event_payload_len < (isize::MAX as u64))
            & self.has_remaining_bytes(event_metadata.event_payload_len);
        if compiler::unlikely(!okay) {
            return Err(SDSSError::TransactionLogEntryCorrupted);
        }
        let event_is_zero =
            (event_metadata.event_crc == 0) & (event_metadata.event_payload_len == 0);
        let event_src_marker = event_src_marker.unwrap();
        match event_src_marker {
            EventSourceMarker::ServerStandard => {}
            EventSourceMarker::DriverClosed if event_is_zero => {
                // expect last entry
                if self.end_of_file() {
                    self.closed = true;
                    // good
                    return Ok(());
                } else {
                    return Err(SDSSError::TransactionLogCorrupted);
                }
            }
            _ => return Err(SDSSError::TransactionLogEntryCorrupted),
        }
        // read bytes
        let mut payload_data_block = vec![0u8; event_metadata.event_payload_len as usize];
        self.log_file.read_to_buffer(&mut payload_data_block)?;
        self._record_bytes_read(event_metadata.event_payload_len as _);
        // verify sum
        const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
        let actual_sum = CRC.checksum(&payload_data_block);
        if compiler::likely(actual_sum == event_metadata.event_crc) {
            // great, the sums match
            TA::decode_and_update_state(&payload_data_block, gs)?;
            Ok(())
        } else {
            Err(SDSSError::TransactionLogEntryCorrupted)
        }
    }
    /// Read and apply all events in the given log file to the global state, returning the open file
    pub fn scroll(file: SDSSFileIO<LF>, gs: &TA::GlobalState) -> SDSSResult<SDSSFileIO<LF>> {
        let mut slf = Self::new(file)?;
        while !slf.end_of_file() {
            slf.rapply_next_event(gs)?;
        }
        Ok(slf.log_file)
    }
}

impl<TA, LF> TransactionLogReader<TA, LF> {
    fn _incr_evid(&mut self) {
        self.evid += 1;
    }
    fn _record_bytes_read(&mut self, cnt: usize) {
        self.remaining_bytes -= cnt as u64;
    }
    fn has_remaining_bytes(&self, size: u64) -> bool {
        self.remaining_bytes >= size
    }
    fn end_of_file(&self) -> bool {
        self.remaining_bytes == 0
    }
}
