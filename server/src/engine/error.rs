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

use super::{storage::v1::SDSSError, txn::TransactionError};
pub type QueryResult<T> = Result<T, Error>;

/// an enumeration of 'flat' errors that the server actually responds to the client with, since we do not want to send specific information
/// about anything (as that will be a security hole). The variants correspond with their actual response codes
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Error {
    /// I/O error
    SysIOError,
    /// out of memory
    SysOutOfMemory,
    /// unknown server error
    SysUnknownError,
    /// invalid protocol packet
    NetProtocolIllegalPacket,
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
    /// expected a statement keyword found something else
    QLExpectedStatement,
    /// invalid collection definition definition
    QLInvalidCollectionSyntax,
    /// invalid type definition syntax
    QLInvalidTypeDefinitionSyntax,
    /// invalid relational expression
    QLIllegalRelExp,
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
    /// storage subsystem error
    StorageSubsystemError,
}

direct_from! {
    Error[_] => {
        SDSSError as StorageSubsystemError,
        TransactionError as TransactionalError,
    }
}
