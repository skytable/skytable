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
    crate::{
        engine::{
            error::ErrorKind,
            storage::{
                common::interface::fs::{File, FileExt, FileSystem, FileWriteExt},
                v2::raw::journal::{
                    create_journal, open_journal,
                    raw::{
                        obtain_offsets, obtain_trace, DriverEvent, JournalReaderTraceEvent,
                        RawJournalWriter,
                    },
                    repair_journal, JournalRepairMode, JournalSettings, RepairResult,
                },
            },
            RuntimeResult,
        },
        IoResult,
    },
    std::io::ErrorKind as IoErrorKind,
};

fn create_trimmed_file(from: &str, to: &str, trim_to: u64) -> IoResult<()> {
    FileSystem::copy(from, to)?;
    let mut f = File::open(to)?;
    f.f_truncate(trim_to)
}

#[test]
fn corruption_at_close() {
    let initializers: Vec<(&'static str, fn(&str) -> RuntimeResult<u64>)> = vec![
        // open and close
        ("close_event_corruption_empty.db", |jrnl_id| {
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            Ok(0)
        }),
        // open, apply mix of events, close
        ("close_event_corruption.db", |jrnl_id| {
            let mut operation_count = 0;
            let mut sdb = SimpleDB::new();
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            for num in 1..=100 {
                operation_count += 1;
                sdb.push(&mut jrnl, format!("key-{num}"))?;
                if num % 10 == 0 {
                    operation_count += 1;
                    sdb.pop(&mut jrnl)?;
                }
            }
            RawJournalWriter::close_driver(&mut jrnl)?;
            Ok(operation_count)
        }),
        // open, close, reinit, close
        (
            "close_event_corruption_open_close_open_close.db",
            |jrnl_id| {
                // open and close
                let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
                RawJournalWriter::close_driver(&mut jrnl)?;
                drop(jrnl);
                // reinit and close
                let mut jrnl = open_journal::<SimpleDBJournal>(
                    jrnl_id,
                    &SimpleDB::new(),
                    JournalSettings::default(),
                )?;
                RawJournalWriter::close_driver(&mut jrnl)?;
                Ok(2)
            },
        ),
    ];
    for (journal_id, initializer) in initializers {
        // initialize journal, get size and clear traces
        let close_event_id = match initializer(journal_id) {
            Ok(nid) => nid,
            Err(e) => panic!(
                "failed to initialize {journal_id} due to {e}. trace: {:?}, file_data={:?}",
                obtain_trace(),
                FileSystem::read(journal_id),
            ),
        };
        let journal_size = File::open(journal_id).unwrap().f_len().unwrap();
        let _ = obtain_trace();
        let _ = obtain_offsets();
        // now trim and repeat
        for (trim_size, new_size) in (1..=DriverEvent::FULL_EVENT_SIZE)
            .rev()
            .map(|trim_size| (trim_size, journal_size - trim_size as u64))
        {
            // create a copy of the "good" journal and trim to simulate data loss
            let trimmed_journal_path = format!("{journal_id}-trimmed-{trim_size}.db");
            create_trimmed_file(journal_id, &trimmed_journal_path, new_size).unwrap();
            // init misc
            let simple_db = SimpleDB::new();
            let open_journal_fn = || {
                open_journal::<SimpleDBJournal>(
                    &trimmed_journal_path,
                    &simple_db,
                    JournalSettings::default(),
                )
            };
            // open the journal and validate failure
            let open_err = open_journal_fn().unwrap_err();
            let trace = obtain_trace();
            if trim_size > (DriverEvent::FULL_EVENT_SIZE - (sizeof!(u128) + sizeof!(u64))) {
                // the amount of trim from the end of the file causes us to lose valuable metadata
                if close_event_id == 0 {
                    // empty log
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    )
                } else {
                    assert_eq!(
                        *trace.last().unwrap(),
                        JournalReaderTraceEvent::LookingForEvent.into(),
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    );
                }
            } else {
                // the amount of trim still allows us to read some metadata
                if close_event_id == 0 {
                    // empty log
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(close_event_id),
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    )
                } else {
                    assert_eq!(
                        &trace[trace.len() - 3..],
                        &into_array![
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(close_event_id),
                            JournalReaderTraceEvent::DriverEventExpectingClose
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    );
                }
            }
            assert_eq!(
                open_err.kind(),
                &ErrorKind::IoError(IoErrorKind::UnexpectedEof.into()),
                "failed at trim_size {trim_size} for journal {journal_id}"
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
                "failed at trim_size {trim_size} for journal {journal_id}"
            );
            // now reopen log and ensure it's repaired
            let mut jrnl = open_journal_fn().unwrap();
            RawJournalWriter::close_driver(&mut jrnl).unwrap();
            // clear trace
            let _ = obtain_trace();
            let _ = obtain_offsets();
        }
    }
}
