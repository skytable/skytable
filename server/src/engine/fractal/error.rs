/*
 * Created on Mon Oct 02 2023
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
    super::context::{self, Dmsg, Subsystem},
    crate::engine::{
        config::ConfigError,
        error::{ErrorKind, StorageError, TransactionError},
    },
    core::fmt,
};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// An error implementation with context tracing and propagation
///
/// - All errors that are classified in [`ErrorKind`] will automatically inherit all local context, unless explicitly orphaned,
/// or manually constructed (see [`IntoError::err_noinherit`])
/// - All other errors will generally take the context from parent
///
/// Error propagation and tracing relies on the fact that the first error that occurs will end the routine in question, entering
/// a new local context; if otherwise, it will fail. To manage such custom conditions, look at [`ErrorContext`] or manually
/// constructing [`Error`]s.
pub struct Error {
    kind: ErrorKind,
    origin: Option<Subsystem>,
    dmsg: Option<Dmsg>,
}

impl Error {
    /// Returns the error kind
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
    /// Replace the origin in self
    pub fn add_origin(self, origin: Subsystem) -> Self {
        Self::_new(self.kind, Some(origin), self.dmsg)
    }
    /// Replace the dmsg in self
    pub fn add_dmsg(self, dmsg: impl Into<Dmsg>) -> Self {
        Self::_new(self.kind, self.origin, Some(dmsg.into()))
    }
}

impl Error {
    /// ctor
    fn _new(kind: ErrorKind, origin: Option<Subsystem>, dmsg: Option<Dmsg>) -> Self {
        Self { kind, origin, dmsg }
    }
    /// new full error
    pub fn new(kind: ErrorKind, origin: Subsystem, dmsg: impl Into<Dmsg>) -> Self {
        Self::_new(kind, Some(origin), Some(dmsg.into()))
    }
    /// new error with kind and no ctx
    pub fn with_kind(kind: ErrorKind) -> Self {
        Self::_new(kind, None, None)
    }
    /// new error with kind and origin
    fn with_origin(kind: ErrorKind, origin: Subsystem) -> Self {
        Self::_new(kind, Some(origin), None)
    }
    /// remove the dmsg from self
    fn remove_dmsg(self) -> Self {
        Self::_new(self.kind, self.origin, None)
    }
    /// remove the origin from self
    fn remove_origin(self) -> Self {
        Self::_new(self.kind, None, self.dmsg)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.origin {
            Some(orig) => write!(f, "{} error: ", orig.as_str()),
            None => write!(f, "runtime error: "),
        }?;
        match self.dmsg.as_ref() {
            Some(dmsg) => write!(f, "{dmsg}; ")?,
            None => {}
        }
        write!(f, "{}", self.kind)
    }
}

impl std::error::Error for Error {}

/*
    generic error casts
*/

// for all other direct error casts, always inherit context
impl<E: Into<ErrorKind>> From<E> for Error {
    fn from(e: E) -> Self {
        Self::_new(e.into(), context::get_origin(), context::get_dmsg())
    }
}

/*
    error casts used during result private context mutation
*/

// only used when you're modifying context
pub trait IntoError {
    fn err_noinherit(self) -> Error;
    fn err_inherit_parent(self) -> Error;
}

// error kinds do not carry any context
impl<E: Into<ErrorKind>> IntoError for E {
    fn err_noinherit(self) -> Error {
        Error::with_kind(self.into())
    }
    fn err_inherit_parent(self) -> Error {
        Self::err_noinherit(self)
    }
}

impl IntoError for Error {
    fn err_noinherit(self) -> Error {
        Error::with_kind(self.kind)
    }
    fn err_inherit_parent(self) -> Error {
        self
    }
}

/*
    error context and tracing
*/

pub trait ErrorContext<T> {
    // no inherit
    /// set the origin (do not inherit parent or local)
    fn set_origin(self, origin: Subsystem) -> Result<T, Error>;
    /// set the dmsg (do not inherit parent or local)
    fn set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error>;
    fn set_dmsg_fn<F, M>(self, d: F) -> Result<T, Error>
    where
        F: Fn() -> M,
        M: Into<Dmsg>,
        Self: Sized;
    /// set the origin and dmsg (do not inherit)
    fn set_ctx(self, origin: Subsystem, dmsg: impl Into<Dmsg>) -> Result<T, Error>;
    // inherit parent
    /// set the origin (inherit rest from parent)
    fn ip_set_origin(self, origin: Subsystem) -> Result<T, Error>;
    /// set the dmsg (inherit rest from origin)
    fn ip_set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error>;
    // inherit local
    /// set the origin (inherit rest from local)
    fn il_set_origin(self, origin: Subsystem) -> Result<T, Error>;
    /// set the dmsg (inherit rest from local)
    fn il_set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error>;
    /// inherit everything from local (assuming this has no context)
    fn inherit_local(self) -> Result<T, Error>;
    // inherit any
    /// set the origin (inherit rest from either parent, then local)
    fn inherit_set_origin(self, origin: Subsystem) -> Result<T, Error>;
    /// set the dmsg (inherit rest from either parent, then local)
    fn inherit_set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error>;
    // orphan
    /// orphan the entire context (if any)
    fn orphan(self) -> Result<T, Error>;
    /// orphan the origin (if any)
    fn orphan_origin(self) -> Result<T, Error>;
    /// orphan the dmsg (if any)
    fn orphan_dmsg(self) -> Result<T, Error>;
}

impl<T, E> ErrorContext<T> for Result<T, E>
where
    E: IntoError,
{
    // no inherit
    fn set_origin(self, origin: Subsystem) -> Result<T, Error> {
        self.map_err(|e| e.err_noinherit().add_origin(origin))
    }
    fn set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error> {
        self.map_err(|e| e.err_noinherit().add_dmsg(dmsg))
    }
    fn set_dmsg_fn<F, M>(self, d: F) -> Result<T, Error>
    where
        F: Fn() -> M,
        M: Into<Dmsg>,
        Self: Sized,
    {
        self.map_err(|e| e.err_noinherit().add_dmsg(d().into()))
    }
    fn set_ctx(self, origin: Subsystem, dmsg: impl Into<Dmsg>) -> Result<T, Error> {
        self.map_err(|e| Error::new(e.err_noinherit().kind, origin, dmsg))
    }
    // inherit local
    fn il_set_origin(self, origin: Subsystem) -> Result<T, Error> {
        self.map_err(|e| Error::_new(e.err_noinherit().kind, Some(origin), context::pop_dmsg()))
    }
    fn il_set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error> {
        self.map_err(|e| {
            Error::_new(
                e.err_noinherit().kind,
                context::pop_origin(),
                Some(dmsg.into()),
            )
        })
    }
    fn inherit_local(self) -> Result<T, Error> {
        self.map_err(|e| {
            Error::_new(
                e.err_noinherit().kind,
                context::get_origin(),
                context::get_dmsg(),
            )
        })
    }
    // inherit parent
    fn ip_set_origin(self, origin: Subsystem) -> Result<T, Error> {
        self.map_err(|e| e.err_inherit_parent().add_origin(origin))
    }
    fn ip_set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error> {
        self.map_err(|e| e.err_inherit_parent().add_dmsg(dmsg))
    }
    // inherit any
    fn inherit_set_dmsg(self, dmsg: impl Into<Dmsg>) -> Result<T, Error> {
        self.map_err(|e| {
            // inherit from parent
            let mut e = e.err_inherit_parent();
            // inherit from local if parent has no ctx
            e.origin = e.origin.or_else(|| context::pop_origin());
            e.add_dmsg(dmsg)
        })
    }
    fn inherit_set_origin(self, origin: Subsystem) -> Result<T, Error> {
        self.map_err(|e| {
            // inherit from parent
            let mut e = e.err_inherit_parent();
            // inherit form local if parent has no ctx
            e.dmsg = e.dmsg.or_else(|| context::pop_dmsg());
            e.add_origin(origin)
        })
    }
    fn orphan(self) -> Result<T, Error> {
        self.map_err(|e| e.err_noinherit())
    }
    fn orphan_dmsg(self) -> Result<T, Error> {
        self.map_err(|e| e.err_inherit_parent().remove_dmsg())
    }
    fn orphan_origin(self) -> Result<T, Error> {
        self.map_err(|e| e.err_inherit_parent().remove_origin())
    }
}

/*
    foreign type casts
*/

macro_rules! impl_other_err_tostring {
    ($($ty:ty => $origin:ident),* $(,)?) => {
        $(
            impl From<$ty> for Error {
                fn from(e: $ty) -> Self { Self::_new(ErrorKind::Other(e.to_string()), Some(Subsystem::$origin), context::pop_dmsg()) }
            }
            impl IntoError for $ty {
                fn err_noinherit(self) -> Error { Error::with_kind(ErrorKind::Other(self.to_string())) }
                fn err_inherit_parent(self) -> Error { Self::err_noinherit(self) }
            }
        )*
    }
}

impl_other_err_tostring! {
    openssl::ssl::Error => Network,
    openssl::error::Error => Network,
    openssl::error::ErrorStack => Network,
}

impl From<StorageError> for Error {
    fn from(value: StorageError) -> Self {
        Self::_new(
            ErrorKind::Storage(value),
            context::pop_origin(),
            context::pop_dmsg(),
        )
    }
}

impl From<TransactionError> for Error {
    fn from(value: TransactionError) -> Self {
        Self::_new(
            ErrorKind::Txn(value),
            context::pop_origin(),
            context::pop_dmsg(),
        )
    }
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Self::with_origin(ErrorKind::Config(e), Subsystem::Init)
    }
}

impl IntoError for StorageError {
    fn err_noinherit(self) -> Error {
        Error::with_kind(ErrorKind::Storage(self))
    }
    fn err_inherit_parent(self) -> Error {
        self.into()
    }
}

impl IntoError for TransactionError {
    fn err_noinherit(self) -> Error {
        Error::with_kind(ErrorKind::Txn(self))
    }
    fn err_inherit_parent(self) -> Error {
        self.into()
    }
}
