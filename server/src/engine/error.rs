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
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum QueryError {
    /// I/O error
    SysServerError,
    /// out of memory
    SysOutOfMemory,
    /// unknown server error
    SysUnknownError,
    /// something like an integer that randomly has a character to attached to it like `1234q`
    LexInvalidLiteral,
    /// something like an invalid 'string" or a safe string with a bad length etc
    LexInvalidEscapedLiteral,
    /// unexpected byte
    LexUnexpectedByte,
    /// expected a longer statement
    QLUnexpectedEndOfStatement,
    /// incorrect syntax for "something"
    QLInvalidSyntax,
    /// invalid collection definition definition
    QLInvalidCollectionSyntax,
    /// invalid type definition syntax
    QLInvalidTypeDefinitionSyntax,
    /// expected a full entity definition
    QPExpectedEntity,
    /// expected a statement, found something else
    QPExpectedStatement,
    /// unknown statement
    QPUnknownStatement,
    /// this query needs a lock for execution, but that wasn't explicitly allowed anywhere
    QPNeedLock,
    /// the object to be used as the "query container" is missing (for example, insert when the model was missing)
    QPObjectNotFound,
    /// an unknown field was attempted to be accessed/modified/...
    QPUnknownField,
    /// invalid property for an object
    QPDdlInvalidProperties,
    /// create space/model, but the object already exists
    QPDdlObjectAlreadyExists,
    /// an object that was attempted to be removed is non-empty, and for this object, removals require it to be empty
    QPDdlNotEmpty,
    /// invalid type definition
    QPDdlInvalidTypeDefinition,
    /// bad model definition
    QPDdlModelBadDefinition,
    /// illegal alter model query
    QPDdlModelAlterIllegal,
    /// violated the uniqueness property
    QPDmlDuplicate,
    /// the data could not be validated for being accepted into a field/function/etc.
    QPDmlValidationError,
    /// the where expression has an unindexed column essentially implying that we can't run this query because of perf concerns
    QPDmlWhereHasUnindexedColumn,
    /// the row matching the given match expression was not found
    QPDmlRowNotFound,
    /// transactional error
    TransactionalError,
    SysAuthError,
}

impl From<super::fractal::error::Error> for QueryError {
    fn from(e: super::fractal::error::Error) -> Self {
        match e.kind() {
            ErrorKind::IoError(_) | ErrorKind::Storage(_) => QueryError::SysServerError,
            ErrorKind::Txn(_) => QueryError::TransactionalError,
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
        DecodeCorruptedPayloadMoreBytes = "txn-payload-unexpected-content",
        /// transaction payload is corrupted. has lesser bytes than expected
        DecodedUnexpectedEof = "txn-payload-unexpected-eof",
        /// unknown transaction operation. usually indicates a corrupted payload
        DecodeUnknownTxnOp = "txn-payload-unknown-payload",
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
    #[derive(Debug, PartialEq)]
    /// SDSS based storage engine errors
    pub enum StorageError {
        // header
        /// version mismatch
        HeaderDecodeVersionMismatch = "header-version-mismatch",
        /// The entire header is corrupted
        HeaderDecodeCorruptedHeader = "header-corrupted",
        /// Expected header values were not matched with the current header
        HeaderDecodeDataMismatch = "header-data-mismatch",
        // journal
        /// While attempting to handle a basic failure (such as adding a journal entry), the recovery engine ran into an exceptional
        /// situation where it failed to make a necessary repair the log
        JournalWRecoveryStageOneFailCritical = "journal-recovery-failure",
        /// An entry in the journal is corrupted
        JournalLogEntryCorrupted = "journal-entry-corrupted",
        /// The structure of the journal is corrupted
        JournalCorrupted = "journal-corrupted",
        // internal file structures
        /// While attempting to decode a structure in an internal segment of a file, the storage engine ran into a possibly irrecoverable error
        InternalDecodeStructureCorrupted = "structure-decode-corrupted",
        /// the payload (non-static) part of a structure in an internal segment of a file is corrupted
        InternalDecodeStructureCorruptedPayload = "structure-decode-corrupted-payload",
        /// the data for an internal structure was decoded but is logically invalid
        InternalDecodeStructureIllegalData = "structure-decode-illegal-data",
        /// when attempting to flush a data batch, the batch journal crashed and a recovery event was triggered. But even then,
        /// the data batch journal could not be fixed
        DataBatchRecoveryFailStageOne = "batch-recovery-failure",
        /// when attempting to restore a data batch from disk, the batch journal crashed and had a corruption, but it is irrecoverable
        DataBatchRestoreCorruptedBatch = "batch-corrupted-batch",
        /// when attempting to restore a data batch from disk, the driver encountered a corrupted entry
        DataBatchRestoreCorruptedEntry = "batch-corrupted-entry",
        /// we failed to close the data batch
        DataBatchCloseError = "batch-persist-close-failed",
        /// the data batch file is corrupted
        DataBatchRestoreCorruptedBatchFile = "batch-corrupted-file",
        /// the system database is corrupted
        SysDBCorrupted = "sysdb-corrupted",
    }
}
