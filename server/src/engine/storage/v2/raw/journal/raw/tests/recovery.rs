/*
 * Created on Tue Mar 26 2024
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
    super::{SimpleDB, SimpleDBJournal},
    crate::engine::{
        error::ErrorKind,
        storage::{
            common::interface::fs::{File, FileExt, FileSystem, FileWriteExt},
            v2::raw::journal::{
                create_journal, open_journal,
                raw::{obtain_trace, DriverEvent, JournalReaderTraceEvent, RawJournalWriter},
                repair_journal, JournalRepairMode, JournalSettings, RepairResult,
            },
        },
    },
    std::io::ErrorKind as IoErrorKind,
};

#[test]
fn close_event_corruption() {
    let full_file_size;
    {
        // open and close a journal (and clear traces)
        let mut jrnl = create_journal::<SimpleDBJournal>("close_event_corruption.db").unwrap();
        RawJournalWriter::close_driver(&mut jrnl).unwrap();
        let _ = obtain_trace();
        full_file_size = {
            let f = File::open("close_event_corruption.db").unwrap();
            f.f_len().unwrap()
        };
    }
    for (trim_size, new_size) in (1..=DriverEvent::FULL_EVENT_SIZE)
        .rev()
        .map(|trim_size| (trim_size, full_file_size - trim_size as u64))
    {
        // create a copy of the "good" journal
        let trimmed_journal_path = format!("close_event_corruption-trimmed-{trim_size}.db");
        FileSystem::copy("close_event_corruption.db", &trimmed_journal_path).unwrap();
        let simple_db = SimpleDB::new();
        let open_journal_fn = || {
            open_journal::<SimpleDBJournal>(
                &trimmed_journal_path,
                &simple_db,
                JournalSettings::default(),
            )
        };
        // trim this journal to simulate loss of data
        let mut f = File::open(&trimmed_journal_path).unwrap();
        f.f_truncate(new_size).unwrap();
        // open the journal and validate failure
        let open_err = open_journal_fn().unwrap_err();
        let trace = obtain_trace();
        if trim_size > (DriverEvent::FULL_EVENT_SIZE - (sizeof!(u128) + sizeof!(u64))) {
            // the amount of trim from the end of the file causes us to lose valuable metadata
            assert_eq!(
                trace,
                intovec![JournalReaderTraceEvent::Initialized],
                "failed at trim_size {trim_size}"
            );
        } else {
            // the amount of trim still allows us to read some metadata
            assert_eq!(
                trace,
                intovec![
                    JournalReaderTraceEvent::Initialized,
                    JournalReaderTraceEvent::AttemptingEvent(0),
                    JournalReaderTraceEvent::DriverEventExpectingClose
                ],
                "failed at trim_size {trim_size}"
            );
        }
        assert_eq!(
            open_err.kind(),
            &ErrorKind::IoError(IoErrorKind::UnexpectedEof.into()),
            "failed at trim_size {trim_size}"
        );
        // now repair this log
        assert_eq!(
            repair_journal::<SimpleDBJournal>(
                &trimmed_journal_path,
                &simple_db,
                JournalSettings::default(),
                JournalRepairMode::Simple,
            )
            .unwrap(),
            RepairResult::UnspecifiedLoss((DriverEvent::FULL_EVENT_SIZE - trim_size) as _),
            "failed at trim_size {trim_size}"
        );
        // now reopen log and ensure it's repaired
        let mut jrnl = open_journal_fn().unwrap();
        RawJournalWriter::close_driver(&mut jrnl).unwrap();
        // clear trace
        let _ = obtain_trace();
    }
}
