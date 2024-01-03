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
                    self, JournalAdapter, JournalWriter, _JournalEventTrace,
                    _JournalReaderTraceEvent, _JournalWriterTraceEvent,
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
        txn_writer.append_event(TxEvent::Set(pos, val))
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
    const RECOVERY_PLUGIN: bool = false;
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
        journal::__unwind_evtrace(),
        into_array![
            _JournalEventTrace::InitCreated,
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::CompletedEventAppend,
            _JournalWriterTraceEvent::CompletedEventAppend,
            _JournalWriterTraceEvent::Closed,
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
        journal::__unwind_evtrace(),
        into_array![
            // restore
            _JournalEventTrace::InitRestored,
            // initialize
            _JournalReaderTraceEvent::Initialized,
            // read events
            _JournalReaderTraceEvent::BeginEventsScan,
            // event 1
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            // event 2
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            // journal close
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose,
            _JournalReaderTraceEvent::EOF,
            _JournalReaderTraceEvent::Closed,
            _JournalReaderTraceEvent::Success,
            // init writer
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::Reopened,
            _JournalWriterTraceEvent::Reinitialized,
            _JournalWriterTraceEvent::Closed,
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
        journal::__unwind_evtrace(),
        into_array![
            _JournalEventTrace::InitCreated,
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::CompletedEventAppend, // 1
            _JournalWriterTraceEvent::CompletedEventAppend, // 2
            _JournalWriterTraceEvent::CompletedEventAppend, // 3
            _JournalWriterTraceEvent::CompletedEventAppend, // 4
            _JournalWriterTraceEvent::CompletedEventAppend, // 5
            _JournalWriterTraceEvent::CompletedEventAppend, // 6
            _JournalWriterTraceEvent::CompletedEventAppend, // 7
            _JournalWriterTraceEvent::CompletedEventAppend, // 8
            _JournalWriterTraceEvent::CompletedEventAppend, // 9
            _JournalWriterTraceEvent::CompletedEventAppend, // 10
            _JournalWriterTraceEvent::Closed,
        ]
    );
    let bkp_db1 = db1.copy_data();
    drop(db1);
    // second boot
    let db2 = Database::new();
    let x = || -> RuntimeResult<()> {
        let mut log = open_log("duatxn.db-tlog", &db2)?;
        assert_eq!(
            journal::__unwind_evtrace(),
            into_array![
                // restore
                _JournalEventTrace::InitRestored,
                // init reader
                _JournalReaderTraceEvent::Initialized,
                _JournalReaderTraceEvent::BeginEventsScan,
                // scan events (10)
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 1
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 2
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 3
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 4
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 5
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 6
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 7
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 8
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 9
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                _JournalReaderTraceEvent::EntryReadRawMetadata, // 10
                _JournalReaderTraceEvent::EventKindStandard,
                _JournalReaderTraceEvent::CompletedEvent,
                // reader: hit close
                _JournalReaderTraceEvent::EntryReadRawMetadata,
                _JournalReaderTraceEvent::HitClose,
                _JournalReaderTraceEvent::EOF,
                _JournalReaderTraceEvent::Closed,
                _JournalReaderTraceEvent::Success,
                // open writer
                _JournalWriterTraceEvent::Initialized,
                _JournalWriterTraceEvent::Reopened,
                _JournalWriterTraceEvent::Reinitialized,
            ]
        );
        assert_eq!(bkp_db1, db2.copy_data());
        for i in 0..10 {
            let current_val = db2.data.borrow()[i];
            db2.txn_set(i, current_val + i as u8, &mut log)?;
            assert_eq!(
                journal::__unwind_evtrace(),
                into_array![_JournalWriterTraceEvent::CompletedEventAppend]
            );
        }
        log.close()
    };
    x().unwrap();
    assert_eq!(
        journal::__unwind_evtrace(),
        into_array![_JournalWriterTraceEvent::Closed]
    );
    let bkp_db2 = db2.copy_data();
    drop(db2);
    // third boot
    let db3 = Database::new();
    let log = open_log("duatxn.db-tlog", &db3).unwrap();
    log.close().unwrap();
    assert_eq!(
        journal::__unwind_evtrace(),
        into_array![
            // init journal
            _JournalEventTrace::InitRestored,
            // init reader
            _JournalReaderTraceEvent::Initialized,
            _JournalReaderTraceEvent::BeginEventsScan,
            // scan events (10)
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 1
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 2
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 3
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 4
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 5
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 6
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 7
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 8
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 9
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 10
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            // close and reopen journal
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose,
            _JournalReaderTraceEvent::IffyReopen,
            _JournalReaderTraceEvent::ReopenCheck,
            _JournalReaderTraceEvent::ReopenSuccess,
            // scan events (10)
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 1
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 2
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 3
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 4
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 5
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 6
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 7
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 8
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 9
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            _JournalReaderTraceEvent::EntryReadRawMetadata, // 10
            _JournalReaderTraceEvent::EventKindStandard,
            _JournalReaderTraceEvent::CompletedEvent,
            // close reader
            _JournalReaderTraceEvent::EntryReadRawMetadata,
            _JournalReaderTraceEvent::HitClose,
            _JournalReaderTraceEvent::EOF,
            _JournalReaderTraceEvent::Closed,
            _JournalReaderTraceEvent::Success,
            // open writer
            _JournalWriterTraceEvent::Initialized,
            _JournalWriterTraceEvent::Reopened,
            _JournalWriterTraceEvent::Reinitialized,
            _JournalWriterTraceEvent::Closed,
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
