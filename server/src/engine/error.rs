/*
 * Created on Sat Feb 04 2023
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

use {super::config::ConfigError, crate::util::os::SysIOError, std::fmt};

pub type RuntimeResult<T> = Result<T, super::fractal::error::Error>;
pub type QueryResult<T> = Result<T, QueryError>;

/// an enumeration of 'flat' errors that the server actually responds to the client with, since we do not want to send specific information
/// about anything (as that will be a security hole). The variants correspond with their actual response codes
#[derive(Debug, Clone, Copy, PartialEq, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum QueryError {
    // system
    /// I/O error
    SysServerError = 0,
    /// out of memory
    SysOutOfMemory = 1,
    /// unknown server error
    SysUnknownError = 2,
    /// system auth error
    SysAuthError = 3,
    /// transactional error
    SysTransactionalError = 4,
    /// insufficient permissions error
    SysPermissionDenied = 5,
    SysNetworkSystemIllegalClientPacket = 6,
    // QL
    /// something like an integer that randomly has a character to attached to it like `1234q`
    LexInvalidInput = 25,
    /// unexpected byte
    LexUnexpectedByte = 26,
    /// expected a longer statement
    QLUnexpectedEndOfStatement = 27,
    /// incorrect syntax for "something"
    QLInvalidSyntax = 28,
    /// invalid collection definition definition
    QLInvalidCollectionSyntax = 29,
    /// invalid type definition syntax
    QLInvalidTypeDefinitionSyntax = 30,
    /// expected a full entity definition
    QLExpectedEntity = 31,
    /// expected a statement, found something else
    QLExpectedStatement = 32,
    /// unknown statement
    QLUnknownStatement = 33,
    // exec
    /// the object to be used as the "query container" is missing (for example, insert when the model was missing)
    QExecObjectNotFound = 100,
    /// an unknown field was attempted to be accessed/modified/...
    QExecUnknownField = 101,
    /// invalid property for an object
    QExecDdlInvalidProperties = 102,
    /// create space/model, but the object already exists
    QExecDdlObjectAlreadyExists = 103,
    /// an object that was attempted to be removed is non-empty, and for this object, removals require it to be empty
    QExecDdlNotEmpty = 104,
    /// invalid type definition
    QExecDdlInvalidTypeDefinition = 105,
    /// bad model definition
    QExecDdlModelBadDefinition = 106,
    /// illegal alter model query
    QExecDdlModelAlterIllegal = 107,
    // exec DML
    /// violated the uniqueness property
    QExecDmlDuplicate = 108,
    /// the data could not be validated for being accepted into a field/function/etc.
    QExecDmlValidationError = 109,
    /// the where expression has an unindexed column essentially implying that we can't run this query because of perf concerns
    QExecDmlWhereHasUnindexedColumn = 110,
    /// the row matching the given match expression was not found
    QExecDmlRowNotFound = 111,
    /// this query needs a lock for execution, but that wasn't explicitly allowed anywhere
    QExecNeedLock = 112,
}

direct_from! {
    QueryError[_] => {
        std::io::Error as SysServerError,
    }
}

impl From<super::fractal::error::Error> for QueryError {
    fn from(e: super::fractal::error::Error) -> Self {
        match e.kind() {
            ErrorKind::IoError(_) | ErrorKind::Storage(_) => QueryError::SysServerError,
            ErrorKind::Txn(_) => QueryError::SysTransactionalError,
            ErrorKind::Other(_) => QueryError::SysUnknownError,
            ErrorKind::Config(_) => unreachable!("config error cannot propagate here"),
        }
    }
}

macro_rules! enumerate_err {
    ($(#[$attr:meta])* $vis:vis enum $errname:ident { $($(#[$varattr:meta])* $variant:ident = $errstring:expr),* $(,)? }) => {
        $(#[$attr])*
        $vis enum $errname { $($(#[$varattr])* $variant),* }
        impl core::fmt::Display for $errname {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                match self {$( Self::$variant => write!(f, "{}", $errstring),)*}
            }
        }
        impl std::error::Error for $errname {}
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// A "master" error kind enumeration for all kinds of runtime errors
pub enum ErrorKind {
    /// An I/O error
    IoError(SysIOError),
    /// An SDSS error
    Storage(StorageError),
    /// A transactional error
    Txn(TransactionError),
    /// other errors
    Other(String),
    /// configuration errors
    Config(ConfigError),
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(io) => write!(f, "io error: {io}"),
            Self::Storage(se) => write!(f, "storage error: {se}"),
            Self::Txn(txe) => write!(f, "txn error: {txe}"),
            Self::Other(oe) => write!(f, "error: {oe}"),
            Self::Config(cfg) => write!(f, "config error: {cfg}"),
        }
    }
}

impl std::error::Error for ErrorKind {}

direct_from! {
    ErrorKind => {
        std::io::Error as IoError,
        SysIOError as IoError,
    }
}

enumerate_err! {
    #[derive(Debug, PartialEq)]
    /// Errors that occur when restoring transactional data
    pub enum TransactionError {
        /// corrupted txn payload. has more bytes than expected
        V1DecodeCorruptedPayloadMoreBytes = "txn-payload-unexpected-content",
        /// transaction payload is corrupted. has lesser bytes than expected
        V1DecodedUnexpectedEof = "txn-payload-unexpected-eof",
        /// unknown transaction operation. usually indicates a corrupted payload
        V1DecodeUnknownTxnOp = "txn-payload-unknown-payload",
        /// While restoring a certain item, a non-resolvable conflict was encountered in the global state, because the item was
        /// already present (when it was expected to not be present)
        OnRestoreDataConflictAlreadyExists = "txn-payload-conflict-already-exists",
        /// On restore, a certain item that was expected to be present was missing in the global state
        OnRestoreDataMissing = "txn-payload-conflict-missing",
        /// On restore, a certain item that was expected to match a certain value, has a different value
        OnRestoreDataConflictMismatch = "txn-payload-conflict-mismatch",
    }
}

enumerate_err! {
    #[derive(Debug, PartialEq, Clone, Copy)]
    /// SDSS based storage engine errors
    pub enum StorageError {
        /*
            ----
            SDSS Errors
            ----
            These errors are common across all versions
        */
        /// version mismatch
        FileDecodeHeaderVersionMismatch = "header-version-mismatch",
        /// The entire header is corrupted
        FileDecodeHeaderCorrupted = "header-corrupted",
        /*
            ----
            Common encoding errors
            ----
        */
        // internal file structures
        /// While attempting to decode a structure in an internal segment of a file, the storage engine ran into a possibly irrecoverable error
        InternalDecodeStructureCorrupted = "structure-decode-corrupted",
        /// the payload (non-static) part of a structure in an internal segment of a file is corrupted
        InternalDecodeStructureCorruptedPayload = "structure-decode-corrupted-payload",
        /// the data for an internal structure was decoded but is logically invalid
        InternalDecodeStructureIllegalData = "structure-decode-illegal-data",
        /*
            ----
            V1 Journal Errors
            ----
        */
        /// An entry in the journal is corrupted
        V1JournalDecodeLogEntryCorrupted = "journal-entry-corrupted",
        /// The structure of the journal is corrupted
        V1JournalDecodeCorrupted = "journal-corrupted",
        /// when attempting to restore a data batch from disk, the batch journal crashed and had a corruption, but it is irrecoverable
        V1DataBatchDecodeCorruptedBatch = "batch-corrupted-batch",
        /// when attempting to restore a data batch from disk, the driver encountered a corrupted entry
        V1DataBatchDecodeCorruptedEntry = "batch-corrupted-entry",
        /// the data batch file is corrupted
        V1DataBatchDecodeCorruptedBatchFile = "batch-corrupted-file",
        /// the system database is corrupted
        V1SysDBDecodeCorrupted = "sysdb-corrupted",
        /// we failed to close the data batch
        V1DataBatchRuntimeCloseError = "batch-persist-close-failed",
        /*
            ----
            V2 Journal Errors
            ----
        */
        /// Journal event metadata corrupted
        RawJournalDecodeEventCorruptedMetadata = "journal-event-metadata-corrupted",
        /// The event body is corrupted
        RawJournalDecodeEventCorruptedPayload = "journal-event-payload-corrupted",
        /// batch contents was unexpected (for example, we expected n events but got m events)
        RawJournalDecodeBatchContentsMismatch = "journal-batch-unexpected-termination",
        /// batch contents was validated and executed but the final integrity check failed
        RawJournalDecodeBatchIntegrityFailure = "journal-batch-integrity-check-failed",
        /// unexpected order of events
        RawJournalDecodeInvalidEvent = "journal-invalid-event-order",
        /// corrupted event within a batch
        RawJournalDecodeCorruptionInBatchMetadata = "journal-batch-corrupted-event-metadata",
        /*
            ----
            runtime errors
            ----
        */
        RawJournalRuntimeHeartbeatFail = "journal-lwt-heartbeat-failed",
        RawJournalRuntimeDirty = "journal-in-dirty-state",
    }
}
