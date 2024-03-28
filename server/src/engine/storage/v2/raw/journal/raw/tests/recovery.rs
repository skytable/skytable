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
                        obtain_offsets, obtain_trace, DriverEvent, DriverEventKind,
                        JournalReaderTraceEvent, JournalWriterTraceEvent, RawJournalWriter,
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

type Initializer = (&'static str, fn(&str) -> RuntimeResult<u64>);

fn create_trimmed_file(from: &str, to: &str, trim_to: u64) -> IoResult<()> {
    FileSystem::copy(from, to)?;
    let mut f = File::open(to)?;
    f.f_truncate(trim_to)
}

fn emulate_corrupted_final_event(
    initializers: impl IntoIterator<Item = Initializer>,
    post_corruption_handler: impl Fn(&str, u64, usize, RuntimeResult<RawJournalWriter<SimpleDBJournal>>),
    post_repair_handler: impl Fn(
        &str,
        usize,
        RuntimeResult<RepairResult>,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
    ),
) {
    for (journal_id, initializer) in initializers {
        // initialize journal, get size and clear traces
        let repaired_last_event_id = match initializer(journal_id) {
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
            // now let the caller handle any post corruption work
            let open_journal_result = open_journal_fn();
            post_corruption_handler(
                journal_id,
                repaired_last_event_id,
                trim_size,
                open_journal_result,
            );
            // repair
            let repair_result = repair_journal::<SimpleDBJournal>(
                &trimmed_journal_path,
                &simple_db,
                JournalSettings::default(),
                JournalRepairMode::Simple,
            );
            let repaired_journal_reopen_result = open_journal_fn();
            // let caller handle any post repair work
            post_repair_handler(
                journal_id,
                trim_size,
                repair_result,
                repaired_journal_reopen_result,
            );
        }
    }
}

#[test]
fn corruption_before_close() {
    let initializers: Vec<Initializer> = vec![
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
    emulate_corrupted_final_event(
        initializers,
        |journal_id, repaired_last_event_id, trim_size, open_result| {
            // open the journal and validate failure
            let open_err = open_result.unwrap_err();
            let trace = obtain_trace();
            if trim_size > (DriverEvent::FULL_EVENT_SIZE - (sizeof!(u128) + sizeof!(u64))) {
                // the amount of trim from the end of the file causes us to lose valuable metadata
                if repaired_last_event_id == 0 {
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
                if repaired_last_event_id == 0 {
                    // empty log
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id),
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    )
                } else {
                    assert_eq!(
                        &trace[trace.len() - 3..],
                        &into_array![
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id),
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
        },
        |journal_id, trim_size, repair_result, reopen_result| {
            assert_eq!(
                repair_result.unwrap(),
                RepairResult::UnspecifiedLoss((DriverEvent::FULL_EVENT_SIZE - trim_size) as _),
                "failed at trim_size {trim_size} for journal {journal_id}"
            );
            let mut jrnl = reopen_result.unwrap();
            // now reopen log and ensure it's repaired
            RawJournalWriter::close_driver(&mut jrnl).unwrap();
            // clear trace
            let _ = obtain_trace();
            let _ = obtain_offsets();
        },
    )
}

#[test]
fn corruption_after_reopen() {
    let initializers: Vec<Initializer> = vec![("corruption_after_reopen.db", |jrnl_id| {
        let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
        RawJournalWriter::close_driver(&mut jrnl)?;
        drop(jrnl);
        // reopen, but don't close
        open_journal::<SimpleDBJournal>(jrnl_id, &SimpleDB::new(), JournalSettings::default())?;
        Ok(1)
    })];
    emulate_corrupted_final_event(
        initializers,
        |journal_id, repaired_last_event_id, trim_size, open_result| {
            let trace = obtain_trace();
            if trim_size == DriverEvent::FULL_EVENT_SIZE {
                /*
                    IMPORTANT IFFY SITUATION: undetectable error. if an entire "correct" part of the log vanishes, it's not going to be detected.
                    while possible in theory, it's going to have to be one heck of a coincidence for it to happen in practice. the only way to work
                    around this is to use a secondary checksum. I'm not a fan of that approach either (and I don't even consider it to be a good mitigation)
                    because it can potentially violate consistency, conflicting the source of truth. for example: if we have a database crash, should we trust
                    the checksum file or the log? guarding that further requires an enormous amount of effort and it will still have holes and ironically,
                    will potentially introduce more bugs due to increased complexity. Get a good filesystem and disk controller (that attaches checksums to sectors)!
                    -- @ohsayan
                */
                let mut jrnl =
                    open_result.expect(&format!("failed at {trim_size} for journal {journal_id}"));
                assert_eq!(
                    trace,
                    intovec![
                        JournalReaderTraceEvent::Initialized,
                        JournalReaderTraceEvent::LookingForEvent,
                        JournalReaderTraceEvent::AttemptingEvent(0),
                        JournalReaderTraceEvent::DriverEventExpectingClose,
                        JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                        JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                        JournalReaderTraceEvent::ClosedAndReachedEof,
                        JournalReaderTraceEvent::Completed,
                        JournalWriterTraceEvent::ReinitializeAttempt,
                        JournalWriterTraceEvent::DriverEventAttemptCommit {
                            event: DriverEventKind::Reopened,
                            event_id: repaired_last_event_id,
                            prev_id: 0
                        },
                        JournalWriterTraceEvent::DriverEventCompleted,
                        JournalWriterTraceEvent::ReinitializeComplete,
                    ],
                    "failed at {trim_size} for journal {journal_id}"
                );
                // now close this so that this works with the post repair handler
                RawJournalWriter::close_driver(&mut jrnl).unwrap();
                let _ = obtain_offsets();
                let _ = obtain_trace();
            } else {
                assert_eq!(
                    open_result.unwrap_err().kind(),
                    &ErrorKind::IoError(IoErrorKind::UnexpectedEof.into())
                );
                assert_eq!(
                    trace,
                    intovec![
                        JournalReaderTraceEvent::Initialized,
                        JournalReaderTraceEvent::LookingForEvent,
                        JournalReaderTraceEvent::AttemptingEvent(0),
                        JournalReaderTraceEvent::DriverEventExpectingClose,
                        JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                        JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                        JournalReaderTraceEvent::DriverEventExpectingReopenBlock,
                        JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id)
                    ]
                );
            }
        },
        |journal_id, trim_size, repair_result, reopen_result| {
            assert!(reopen_result.is_ok());
            if trim_size == DriverEvent::FULL_EVENT_SIZE {
                // see earlier comment
                assert_eq!(
                    repair_result.unwrap(),
                    RepairResult::NoErrors,
                    "failed at {trim_size} for journal {journal_id}"
                );
            } else {
                assert_eq!(
                    repair_result.unwrap(),
                    RepairResult::LostBytes((DriverEvent::FULL_EVENT_SIZE - trim_size) as u64)
                );
            }
            let _ = obtain_trace();
            let _ = obtain_offsets();
        },
    )
}
