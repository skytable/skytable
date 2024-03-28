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
    super::{
        super::{
            create_journal, obtain_trace, open_journal, DriverEventKind, JournalReaderTraceEvent,
            JournalSettings, JournalWriterTraceEvent, RawJournalWriter,
        },
        SimpleDB, SimpleDBJournal,
    },
    crate::engine::fractal::error::ErrorContext,
};

#[test]
fn journal_open_close() {
    const JOURNAL_NAME: &str = "journal_open_close";
    {
        // new boot
        let mut j = create_journal::<SimpleDBJournal>(JOURNAL_NAME).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![JournalWriterTraceEvent::Initialized]
        );
        RawJournalWriter::close_driver(&mut j).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Closed,
                    event_id: 0,
                    prev_id: 0
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::DriverClosed
            ]
        );
    }
    {
        // second boot
        let mut j = open_journal::<SimpleDBJournal>(
            JOURNAL_NAME,
            &SimpleDB::new(),
            JournalSettings::default(),
        )
        .unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                // init reader and read close event
                JournalReaderTraceEvent::Initialized,
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(0),
                JournalReaderTraceEvent::DriverEventExpectingClose,
                JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                JournalReaderTraceEvent::ClosedAndReachedEof,
                JournalReaderTraceEvent::Completed,
                // open writer and write reopen event
                JournalWriterTraceEvent::ReinitializeAttempt,
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Reopened,
                    event_id: 1,
                    prev_id: 0
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::ReinitializeComplete
            ]
        );
        RawJournalWriter::close_driver(&mut j).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Closed,
                    event_id: 2,
                    prev_id: 1
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::DriverClosed
            ]
        );
    }
    {
        // third boot
        let mut j = open_journal::<SimpleDBJournal>(
            JOURNAL_NAME,
            &SimpleDB::new(),
            JournalSettings::default(),
        )
        .unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                // init reader and read reopen event
                JournalReaderTraceEvent::Initialized,
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(0),
                JournalReaderTraceEvent::DriverEventExpectingClose,
                JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                JournalReaderTraceEvent::DriverEventExpectingReopenBlock,
                JournalReaderTraceEvent::AttemptingEvent(1),
                JournalReaderTraceEvent::DriverEventExpectingReopenGotReopen,
                JournalReaderTraceEvent::ReopenSuccess,
                // now read close event
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(2),
                JournalReaderTraceEvent::DriverEventExpectingClose,
                JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                JournalReaderTraceEvent::ClosedAndReachedEof,
                JournalReaderTraceEvent::Completed,
                // open writer and write reopen event
                JournalWriterTraceEvent::ReinitializeAttempt,
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Reopened,
                    event_id: 3,
                    prev_id: 2,
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::ReinitializeComplete
            ]
        );
        RawJournalWriter::close_driver(&mut j).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Closed,
                    event_id: 4,
                    prev_id: 3
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::DriverClosed
            ]
        );
    }
}

#[test]
fn journal_with_server_single_event() {
    const JOURNAL_NAME: &str = "journal_with_server_single_event";
    {
        let mut db = SimpleDB::new();
        // new boot
        let mut j = create_journal::<SimpleDBJournal>(JOURNAL_NAME).unwrap();
        db.push(&mut j, "hello world").unwrap();
        RawJournalWriter::close_driver(&mut j).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                JournalWriterTraceEvent::Initialized,
                JournalWriterTraceEvent::CommitAttemptForEvent(0),
                JournalWriterTraceEvent::CommitServerEventWroteMetadata,
                JournalWriterTraceEvent::CommitServerEventAdapterCompleted,
                JournalWriterTraceEvent::CommitCommitServerEventSyncCompleted,
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Closed,
                    event_id: 1,
                    prev_id: 0
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::DriverClosed
            ]
        );
    }
    {
        let db = SimpleDB::new();
        // second boot
        let mut j = open_journal::<SimpleDBJournal>(JOURNAL_NAME, &db, JournalSettings::default())
            .set_dmsg_fn(|| format!("{:?}", obtain_trace()))
            .unwrap();
        assert_eq!(db.data().len(), 1);
        assert_eq!(db.data()[0], "hello world");
        assert_eq!(
            obtain_trace(),
            intovec![
                // init reader and read server event
                JournalReaderTraceEvent::Initialized,
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(0),
                JournalReaderTraceEvent::DetectedServerEvent,
                JournalReaderTraceEvent::ServerEventMetadataParsed,
                JournalReaderTraceEvent::ServerEventAppliedSuccess,
                // now read close event
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(1),
                JournalReaderTraceEvent::DriverEventExpectingClose,
                JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                JournalReaderTraceEvent::ClosedAndReachedEof,
                JournalReaderTraceEvent::Completed,
                // now init writer
                JournalWriterTraceEvent::ReinitializeAttempt,
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Reopened,
                    event_id: 2,
                    prev_id: 1
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::ReinitializeComplete,
            ]
        );
        RawJournalWriter::close_driver(&mut j).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Closed,
                    event_id: 3,
                    prev_id: 2,
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::DriverClosed
            ]
        );
    }
    {
        // third boot
        let db = SimpleDB::new();
        let mut j =
            open_journal::<SimpleDBJournal>(JOURNAL_NAME, &db, JournalSettings::default()).unwrap();
        assert_eq!(db.data().len(), 1);
        assert_eq!(db.data()[0], "hello world");
        assert_eq!(
            obtain_trace(),
            intovec![
                // init reader and read server event
                JournalReaderTraceEvent::Initialized,
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(0),
                JournalReaderTraceEvent::DetectedServerEvent,
                JournalReaderTraceEvent::ServerEventMetadataParsed,
                JournalReaderTraceEvent::ServerEventAppliedSuccess,
                // now read close event
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(1),
                JournalReaderTraceEvent::DriverEventExpectingClose,
                JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                // now read reopen event
                JournalReaderTraceEvent::DriverEventExpectingReopenBlock,
                JournalReaderTraceEvent::AttemptingEvent(2),
                JournalReaderTraceEvent::DriverEventExpectingReopenGotReopen,
                JournalReaderTraceEvent::ReopenSuccess,
                // now read close event
                JournalReaderTraceEvent::LookingForEvent,
                JournalReaderTraceEvent::AttemptingEvent(3),
                JournalReaderTraceEvent::DriverEventExpectingClose,
                JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                JournalReaderTraceEvent::ClosedAndReachedEof,
                JournalReaderTraceEvent::Completed,
                // now open writer and reinitialize
                JournalWriterTraceEvent::ReinitializeAttempt,
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Reopened,
                    event_id: 4,
                    prev_id: 3,
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::ReinitializeComplete,
            ]
        );
        RawJournalWriter::close_driver(&mut j).unwrap();
        assert_eq!(
            obtain_trace(),
            intovec![
                JournalWriterTraceEvent::DriverEventAttemptCommit {
                    event: DriverEventKind::Closed,
                    event_id: 5,
                    prev_id: 4,
                },
                JournalWriterTraceEvent::DriverEventCompleted,
                JournalWriterTraceEvent::DriverClosed
            ]
        );
    }
}

#[test]
fn multi_boot() {
    {
        let mut j = create_journal::<SimpleDBJournal>("multiboot").unwrap();
        let mut db = SimpleDB::new();
        db.push(&mut j, "key_a").unwrap();
        db.push(&mut j, "key_b").unwrap();
        db.pop(&mut j).unwrap();
        RawJournalWriter::close_driver(&mut j).unwrap();
    }
    {
        let mut db = SimpleDB::new();
        let mut j =
            open_journal::<SimpleDBJournal>("multiboot", &db, JournalSettings::default()).unwrap();
        assert_eq!(db.data().as_ref(), vec!["key_a".to_string()]);
        db.clear(&mut j).unwrap();
        db.push(&mut j, "myfinkey").unwrap();
        RawJournalWriter::close_driver(&mut j).unwrap();
    }
    {
        let db = SimpleDB::new();
        let mut j =
            open_journal::<SimpleDBJournal>("multiboot", &db, JournalSettings::default()).unwrap();
        assert_eq!(db.data().as_ref(), vec!["myfinkey".to_string()]);
        RawJournalWriter::close_driver(&mut j).unwrap();
    }
}
