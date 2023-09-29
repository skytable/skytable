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

use {
    super::{
        storage::v1::{SDSSError, SDSSErrorKind},
        txn::TransactionError,
    },
    crate::util::os::SysIOError,
    std::fmt,
};

pub type QueryResult<T> = Result<T, Error>;
// stack
pub type CtxResult<T, E> = Result<T, CtxError<E>>;
pub type RuntimeResult<T> = CtxResult<T, RuntimeErrorKind>;
pub type RuntimeError = CtxError<RuntimeErrorKind>;

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
    SysAuthError,
}

direct_from! {
    Error[_] => {
        SDSSError as StorageSubsystemError,
        TransactionError as TransactionalError,
    }
}

/*
    contextual errors
*/

/// An error context
pub enum CtxErrorDescription {
    A(&'static str),
    B(Box<str>),
}

impl CtxErrorDescription {
    fn inner(&self) -> &str {
        match self {
            Self::A(a) => a,
            Self::B(b) => &b,
        }
    }
}

impl PartialEq for CtxErrorDescription {
    fn eq(&self, other: &Self) -> bool {
        self.inner() == other.inner()
    }
}

impl fmt::Display for CtxErrorDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.inner())
    }
}

impl fmt::Debug for CtxErrorDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.inner())
    }
}

direct_from! {
    CtxErrorDescription => {
        &'static str as A,
        String as B,
        Box<str> as B,
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// A contextual error
pub struct CtxError<E> {
    kind: E,
    ctx: Option<CtxErrorDescription>,
}

impl<E> CtxError<E> {
    fn _new(kind: E, ctx: Option<CtxErrorDescription>) -> Self {
        Self { kind, ctx }
    }
    pub fn new(kind: E) -> Self {
        Self::_new(kind, None)
    }
    pub fn with_ctx(kind: E, ctx: impl Into<CtxErrorDescription>) -> Self {
        Self::_new(kind, Some(ctx.into()))
    }
    pub fn add_ctx(self, ctx: impl Into<CtxErrorDescription>) -> Self {
        Self::with_ctx(self.kind, ctx)
    }
    pub fn into_result<T>(self) -> CtxResult<T, E> {
        Err(self)
    }
    pub fn result<T, F>(result: Result<T, F>) -> CtxResult<T, E>
    where
        E: From<F>,
    {
        result.map_err(|e| CtxError::new(e.into()))
    }
    pub fn result_ctx<T, F>(
        result: Result<T, F>,
        ctx: impl Into<CtxErrorDescription>,
    ) -> CtxResult<T, E>
    where
        E: From<F>,
    {
        result.map_err(|e| CtxError::with_ctx(e.into(), ctx))
    }
}

macro_rules! impl_from_hack {
    ($($ty:ty),*) => {
        $(impl<E> From<E> for CtxError<$ty> where E: Into<$ty> {fn from(e: E) -> Self { CtxError::new(e.into()) }})*
    }
}

/*
    Contextual error impls
*/

impl_from_hack!(RuntimeErrorKind, SDSSErrorKind);

#[derive(Debug)]
pub enum RuntimeErrorKind {
    StorageSubsytem(SDSSError),
    IoError(SysIOError),
    OSSLErrorMulti(openssl::error::ErrorStack),
    OSSLError(openssl::ssl::Error),
}

direct_from! {
    RuntimeErrorKind => {
        SDSSError as StorageSubsytem,
        std::io::Error as IoError,
        SysIOError as IoError,
        openssl::error::ErrorStack as OSSLErrorMulti,
        openssl::ssl::Error as OSSLError,
    }
}
