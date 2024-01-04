/*
 * Created on Tue Sep 05 2023
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

use {
    crate::{
        engine::{
            error::{RuntimeResult, StorageError},
            storage::v1::{
                journal::{
                    self,
                    emulation_tracing::{
                        _EmulateInjection, _JournalEventTrace, _JournalReaderTraceEvent,
                        _JournalReaderTraceRecovery, _JournalWriterInjectedWith,
                        _JournalWriterTraceEvent,
                    },
                    JournalAdapter, JournalWriter,
                },
                spec,
            },
        },
        util,
    },
    std::cell::RefCell,
};

pub struct Database {
    data: RefCell<[u8; 10]>,
}
impl Database {
    fn copy_data(&self) -> [u8; 10] {
        *self.data.borrow()
    }
    fn new() -> Self {
        Self {
            data: RefCell::new([0; 10]),
        }
    }
    fn reset(&self) {
        *self.data.borrow_mut() = [0; 10];
    }
    fn set(&self, pos: usize, val: u8) {
        self.data.borrow_mut()[pos] = val;
    }
    fn txn_set(
        &self,
        pos: usize,
        val: u8,
        txn_writer: &mut JournalWriter<super::VirtualFS, DatabaseTxnAdapter>,
    ) -> RuntimeResult<()> {
        self.set(pos, val);
        txn_writer.append_event_with_recovery_plugin(TxEvent::Set(pos, val))
    }
}

pub enum TxEvent {
    #[allow(unused)]
    Reset,
    Set(usize, u8),
}
#[derive(Debug)]
pub enum TxError {
    SDSS(StorageError),
}
direct_from! {
    TxError => {
        StorageError as SDSS
    }
}
#[derive(Debug)]
pub struct DatabaseTxnAdapter;
impl JournalAdapter for DatabaseTxnAdapter {
    const RECOVERY_PLUGIN: bool = true;
    type Error = TxError;
    type JournalEvent = TxEvent;
    type GlobalState = Database;

    fn encode(event: Self::JournalEvent) -> Box<[u8]> {
        /*
            [1B: opcode][8B:Index][1B: New value]
        */
        let opcode = match event {
            TxEvent::Reset => 0u8,
            TxEvent::Set(_, _) => 1u8,
        };
        let index = match event {
            TxEvent::Reset => 0u64,
            TxEvent::Set(index, _) => index as u64,
        };
        let new_value = match event {
            TxEvent::Reset => 0,
            TxEvent::Set(_, val) => val,
        };
        let mut ret = Vec::with_capacity(10);
        ret.push(opcode);
        ret.extend(index.to_le_bytes());
        ret.push(new_value);
        ret.into_boxed_slice()
    }

    fn decode_and_update_state(payload: &[u8], gs: &Self::GlobalState) -> Result<(), TxError> {
        assert!(payload.len() >= 10, "corrupt file");
        let opcode = payload[0];
        let index = u64::from_le_bytes(util::copy_slice_to_array(&payload[1..9]));
        let new_value = payload[9];
        match opcode {
            0 if index == 0 && new_value == 0 => gs.reset(),
            1 if index < 10 && index < isize::MAX as u64 => gs.set(index as usize, new_value),
            _ => return Err(TxError::SDSS(StorageError::JournalLogEntryCorrupted.into())),
        }
        Ok(())
    }
}

fn open_log(
    log_name: &str,
    db: &Database,
) -> RuntimeResult<JournalWriter<super::VirtualFS, DatabaseTxnAdapter>> {
    journal::open_or_create_journal::<DatabaseTxnAdapter, super::VirtualFS, spec::TestFile>(
        log_name, db,
    )
    .map(|v| v.into_inner())
}

#[test]
fn first_boot_second_readonly() {
    // create log
    let db1 = Database::new();
    let x = || -> RuntimeResult<()> {
        let mut log = open_log("testtxn.log", &db1)?;
        db1.txn_set(0, 20, &mut log)?;
        db1.txn_set(9, 21, &mut log)?;
        log.close()
    };
    x().unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            _JournalEventTrace::InitCreated,
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::CompletedEventAppend(0),
            _JournalWriterTraceEvent::CompletedEventAppend(1),
            _JournalWriterTraceEvent::Closed(2),
        ]
    );
    // backup original data
    let original_data = db1.copy_data();
    // restore log
    let empty_db2 = Database::new();
    open_log("testtxn.log", &empty_db2)
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            // restore
            _JournalEventTrace::InitRestored,
            // initialize
            _JournalReaderTraceEvent::Initialized,
            // read events
            _JournalReaderTraceEvent::BeginEventsScan,
            // event 1
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::EventKindStandard(0),
            _JournalReaderTraceEvent::CompletedEvent,
            // event 2
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::EventKindStandard(1),
            _JournalReaderTraceEvent::CompletedEvent,
            // journal close
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose(2),
            _JournalReaderTraceEvent::EOF,
            _JournalReaderTraceEvent::Success,
            // init writer
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::Reinitializing,
            _JournalWriterTraceEvent::Reopened(3),
            _JournalWriterTraceEvent::Closed(4),
        ]
    );
    assert_eq!(original_data, empty_db2.copy_data());
}

#[test]
fn oneboot_mod_twoboot_mod_thirdboot_read() {
    // first boot: set all to 1
    let db1 = Database::new();
    let x = || -> RuntimeResult<()> {
        let mut log = open_log("duatxn.db-tlog", &db1)?;
        for i in 0..10 {
            db1.txn_set(i, 1, &mut log)?;
        }
        log.close()
    };
    x().unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            _JournalEventTrace::InitCreated,
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::CompletedEventAppend(0), // 1
            _JournalWriterTraceEvent::CompletedEventAppend(1), // 2
            _JournalWriterTraceEvent::CompletedEventAppend(2), // 3
            _JournalWriterTraceEvent::CompletedEventAppend(3), // 4
            _JournalWriterTraceEvent::CompletedEventAppend(4), // 5
            _JournalWriterTraceEvent::CompletedEventAppend(5), // 6
            _JournalWriterTraceEvent::CompletedEventAppend(6), // 7
            _JournalWriterTraceEvent::CompletedEventAppend(7), // 8
            _JournalWriterTraceEvent::CompletedEventAppend(8), // 9
            _JournalWriterTraceEvent::CompletedEventAppend(9), // 10
            _JournalWriterTraceEvent::Closed(10),
        ]
    );
    let bkp_db1 = db1.copy_data();
    drop(db1);
    // second boot
    let db2 = Database::new();
    let x = || -> RuntimeResult<()> {
        let mut log = open_log("duatxn.db-tlog", &db2)?;
        assert_eq!(
            journal::emulation_tracing::__unwind_evtrace(),
            into_array![
                // restore
                _JournalEventTrace::InitRestored,
                // init reader
                _JournalReaderTraceEvent::Initialized,
                _JournalReaderTraceEvent::BeginEventsScan,
                // scan events (10)
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 1
                _JournalReaderTraceEvent::EventKindStandard(0),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 2
                _JournalReaderTraceEvent::EventKindStandard(1),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 3
                _JournalReaderTraceEvent::EventKindStandard(2),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 4
                _JournalReaderTraceEvent::EventKindStandard(3),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 5
                _JournalReaderTraceEvent::EventKindStandard(4),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 6
                _JournalReaderTraceEvent::EventKindStandard(5),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 7
                _JournalReaderTraceEvent::EventKindStandard(6),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 8
                _JournalReaderTraceEvent::EventKindStandard(7),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 9
                _JournalReaderTraceEvent::EventKindStandard(8),
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 10
                _JournalReaderTraceEvent::EventKindStandard(9),
                _JournalReaderTraceEvent::CompletedEvent,
                // reader: hit close
                _JournalReaderTraceEvent::EntryReadRawMetadata,
                _JournalReaderTraceEvent::HitClose(10),
                _JournalReaderTraceEvent::EOF,
                _JournalReaderTraceEvent::Success,
                // open writer
                _JournalWriterTraceEvent::Initialized,
                _JournalWriterTraceEvent::Reinitializing,
                _JournalWriterTraceEvent::Reopened(11),
            ]
        );
        assert_eq!(bkp_db1, db2.copy_data());
        for i in 0..10 {
            let current_val = db2.data.borrow()[i];
            db2.txn_set(i, current_val + i as u8, &mut log)?;
            assert_eq!(
                journal::emulation_tracing::__unwind_evtrace(),
                into_array![_JournalWriterTraceEvent::CompletedEventAppend(
                    12 + i as u64 // events start at 11 but i starts at 0
                )]
            );
        }
        log.close()
    };
    x().unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![_JournalWriterTraceEvent::Closed(22)]
    );
    let bkp_db2 = db2.copy_data();
    drop(db2);
    // third boot
    let db3 = Database::new();
    let log = open_log("duatxn.db-tlog", &db3).unwrap();
    log.close().unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            // init journal
            _JournalEventTrace::InitRestored,
            // init reader
            _JournalReaderTraceEvent::Initialized,
            _JournalReaderTraceEvent::BeginEventsScan,
            // scan events (10)
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 1
            _JournalReaderTraceEvent::EventKindStandard(0),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 2
            _JournalReaderTraceEvent::EventKindStandard(1),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 3
            _JournalReaderTraceEvent::EventKindStandard(2),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 4
            _JournalReaderTraceEvent::EventKindStandard(3),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 5
            _JournalReaderTraceEvent::EventKindStandard(4),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 6
            _JournalReaderTraceEvent::EventKindStandard(5),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 7
            _JournalReaderTraceEvent::EventKindStandard(6),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 8
            _JournalReaderTraceEvent::EventKindStandard(7),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 9
            _JournalReaderTraceEvent::EventKindStandard(8),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 10
            _JournalReaderTraceEvent::EventKindStandard(9),
            _JournalReaderTraceEvent::CompletedEvent,
            // close and reopen journal
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose(10),
            _JournalReaderTraceEvent::IffyReopen,
            _JournalReaderTraceEvent::ReopenCheck,
            _JournalReaderTraceEvent::ReopenSuccess(11),
            // scan events (10)
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 1
            _JournalReaderTraceEvent::EventKindStandard(12),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 2
            _JournalReaderTraceEvent::EventKindStandard(13),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 3
            _JournalReaderTraceEvent::EventKindStandard(14),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 4
            _JournalReaderTraceEvent::EventKindStandard(15),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 5
            _JournalReaderTraceEvent::EventKindStandard(16),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 6
            _JournalReaderTraceEvent::EventKindStandard(17),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 7
            _JournalReaderTraceEvent::EventKindStandard(18),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 8
            _JournalReaderTraceEvent::EventKindStandard(19),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 9
            _JournalReaderTraceEvent::EventKindStandard(20),
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 10
            _JournalReaderTraceEvent::EventKindStandard(21),
            _JournalReaderTraceEvent::CompletedEvent,
            // close reader
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose(22),
            _JournalReaderTraceEvent::EOF,
            _JournalReaderTraceEvent::Success,
            // open writer
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::Reinitializing,
            _JournalWriterTraceEvent::Reopened(23),
            _JournalWriterTraceEvent::Closed(24),
        ]
    );
    assert_eq!(bkp_db2, db3.copy_data());
    assert_eq!(
        db3.copy_data(),
        (1..=10)
            .into_iter()
            .map(u8::from)
            .collect::<Box<[u8]>>()
            .as_ref()
    );
}

#[test]
fn recovery_single_event_boot3_corrupted_checksum() {
    journal::emulation_tracing::__emulate_injection(_EmulateInjection::EventChecksumCorrupted);
    let x = || {
        let db = Database::new();
        let mut log = open_log("test_emulate.db-tlog", &db)?;
        db.txn_set(0, 100, &mut log)?;
        log.close()
    };
    x().unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            _JournalEventTrace::InitCreated,
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterInjectedWith::BadChecksum,
            _JournalWriterTraceEvent::RecoveryEventAdded(1),
            _JournalWriterTraceEvent::Closed(2)
        ]
    );
    let db = Database::new();
    open_log("test_emulate.db-tlog", &db)
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            // create
            _JournalEventTrace::InitRestored,
            // init reader
            _JournalReaderTraceEvent::Initialized,
            _JournalReaderTraceEvent::BeginEventsScan,
            // read event with bad checksum
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::EventKindStandard(0),
            _JournalReaderTraceEvent::ErrorChecksumMismatch,
            // succeed in recovering
            _JournalReaderTraceRecovery::Success(1),
            // read close event
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose(2),
            _JournalReaderTraceEvent::EOF,
            _JournalReaderTraceEvent::Success,
            // now open writer
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::Reinitializing,
            _JournalWriterTraceEvent::Reopened(3),
            // close writer
            _JournalWriterTraceEvent::Closed(4),
        ]
    );
    open_log("test_emulate.db-tlog", &db)
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(
        journal::emulation_tracing::__unwind_evtrace(),
        into_array![
            _JournalEventTrace::InitRestored,
            _JournalReaderTraceEvent::Initialized,
            _JournalReaderTraceEvent::BeginEventsScan,
            // read event with bad checksum
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::EventKindStandard(0),
            _JournalReaderTraceEvent::ErrorChecksumMismatch,
            // succeed in recovering
            _JournalReaderTraceRecovery::Success(1),
            // read close event and reopen
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose(2),
            _JournalReaderTraceEvent::IffyReopen,
            _JournalReaderTraceEvent::ReopenCheck,
            _JournalReaderTraceEvent::ReopenSuccess(3),
            // read close event
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose(4),
            _JournalReaderTraceEvent::EOF,
            _JournalReaderTraceEvent::Success,
            // init wrter
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::Reinitializing,
            _JournalWriterTraceEvent::Reopened(5),
            _JournalWriterTraceEvent::Closed(6),
        ]
    )
}
