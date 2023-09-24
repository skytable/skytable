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
        engine::storage::v1::{
            header_impl::{FileSpecifier, FileSpecifierVersion, HostRunMode},
            journal::{self, JournalAdapter, JournalWriter},
            SDSSError, SDSSResult,
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
    fn txn_reset(
        &self,
        txn_writer: &mut JournalWriter<super::VirtualFS, DatabaseTxnAdapter>,
    ) -> SDSSResult<()> {
        self.reset();
        txn_writer.append_event(TxEvent::Reset)
    }
    fn set(&self, pos: usize, val: u8) {
        self.data.borrow_mut()[pos] = val;
    }
    fn txn_set(
        &self,
        pos: usize,
        val: u8,
        txn_writer: &mut JournalWriter<super::VirtualFS, DatabaseTxnAdapter>,
    ) -> SDSSResult<()> {
        self.set(pos, val);
        txn_writer.append_event(TxEvent::Set(pos, val))
    }
}
pub enum TxEvent {
    Reset,
    Set(usize, u8),
}
#[derive(Debug)]
pub enum TxError {
    SDSS(SDSSError),
}
direct_from! {
    TxError => {
        SDSSError as SDSS
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
        if payload.len() != 10 {
            return Err(SDSSError::CorruptedFile("testtxn.log").into());
        }
        let opcode = payload[0];
        let index = u64::from_le_bytes(util::copy_slice_to_array(&payload[1..9]));
        let new_value = payload[9];
        match opcode {
            0 if index == 0 && new_value == 0 => gs.reset(),
            1 if index < 10 && index < isize::MAX as u64 => gs.set(index as usize, new_value),
            _ => return Err(SDSSError::JournalLogEntryCorrupted.into()),
        }
        Ok(())
    }
}

fn open_log(
    log_name: &str,
    db: &Database,
) -> SDSSResult<JournalWriter<super::VirtualFS, DatabaseTxnAdapter>> {
    journal::open_journal::<DatabaseTxnAdapter, super::VirtualFS>(
        log_name,
        FileSpecifier::TestTransactionLog,
        FileSpecifierVersion::__new(0),
        0,
        HostRunMode::Prod,
        1,
        &db,
    )
    .map(|v| v.into_inner())
}

#[test]
fn first_boot_second_readonly() {
    // create log
    let db1 = Database::new();
    let x = || -> SDSSResult<()> {
        let mut log = open_log("testtxn.log", &db1)?;
        db1.txn_set(0, 20, &mut log)?;
        db1.txn_set(9, 21, &mut log)?;
        log.append_journal_close_and_close()
    };
    x().unwrap();
    // backup original data
    let original_data = db1.copy_data();
    // restore log
    let empty_db2 = Database::new();
    open_log("testtxn.log", &empty_db2)
        .unwrap()
        .append_journal_close_and_close()
        .unwrap();
    assert_eq!(original_data, empty_db2.copy_data());
}
#[test]
fn oneboot_mod_twoboot_mod_thirdboot_read() {
    // first boot: set all to 1
    let db1 = Database::new();
    let x = || -> SDSSResult<()> {
        let mut log = open_log("duatxn.db-tlog", &db1)?;
        for i in 0..10 {
            db1.txn_set(i, 1, &mut log)?;
        }
        log.append_journal_close_and_close()
    };
    x().unwrap();
    let bkp_db1 = db1.copy_data();
    drop(db1);
    // second boot
    let db2 = Database::new();
    let x = || -> SDSSResult<()> {
        let mut log = open_log("duatxn.db-tlog", &db2)?;
        assert_eq!(bkp_db1, db2.copy_data());
        for i in 0..10 {
            let current_val = db2.data.borrow()[i];
            db2.txn_set(i, current_val + i as u8, &mut log)?;
        }
        log.append_journal_close_and_close()
    };
    x().unwrap();
    let bkp_db2 = db2.copy_data();
    drop(db2);
    // third boot
    let db3 = Database::new();
    let log = open_log("duatxn.db-tlog", &db3).unwrap();
    log.append_journal_close_and_close().unwrap();
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
