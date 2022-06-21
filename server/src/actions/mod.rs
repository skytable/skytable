/*
 * Created on Wed Aug 19 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! # Actions
//!
//! Actions are like shell commands, you provide arguments -- they return output. This module contains a collection
//! of the actions supported by Skytable
//!

#[macro_use]
mod macros;
pub mod dbsize;
pub mod del;
pub mod exists;
pub mod flushdb;
pub mod get;
pub mod keylen;
pub mod lists;
pub mod lskeys;
pub mod mget;
pub mod mpop;
pub mod mset;
pub mod mupdate;
pub mod pop;
pub mod set;
pub mod strong;
pub mod update;
pub mod uset;
pub mod whereami;
use crate::corestore::memstore::DdlError;
use crate::protocol::interface::ProtocolSpec;
use crate::util;
use std::io::Error as IoError;

/// A generic result for actions
pub type ActionResult<T> = Result<T, ActionError>;

/// Errors that can occur while running actions
#[derive(Debug)]
pub enum ActionError {
    ActionError(&'static [u8]),
    IoError(std::io::Error),
}

impl PartialEq for ActionError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ActionError(a1), Self::ActionError(a2)) => a1 == a2,
            (Self::IoError(ioe1), Self::IoError(ioe2)) => ioe1.to_string() == ioe2.to_string(),
            _ => false,
        }
    }
}

impl From<&'static [u8]> for ActionError {
    fn from(e: &'static [u8]) -> Self {
        Self::ActionError(e)
    }
}

impl From<IoError> for ActionError {
    fn from(e: IoError) -> Self {
        Self::IoError(e)
    }
}

#[cold]
#[inline(never)]
fn map_ddl_error_to_status<P: ProtocolSpec>(e: DdlError) -> ActionError {
    let r = match e {
        DdlError::AlreadyExists => P::RSTRING_ALREADY_EXISTS,
        DdlError::DdlTransactionFailure => P::RSTRING_DDL_TRANSACTIONAL_FAILURE,
        DdlError::DefaultNotFound => P::RSTRING_DEFAULT_UNSET,
        DdlError::NotEmpty => P::RSTRING_KEYSPACE_NOT_EMPTY,
        DdlError::NotReady => P::RSTRING_NOT_READY,
        DdlError::ObjectNotFound => P::RSTRING_CONTAINER_NOT_FOUND,
        DdlError::ProtectedObject => P::RSTRING_PROTECTED_OBJECT,
        DdlError::StillInUse => P::RSTRING_STILL_IN_USE,
        DdlError::WrongModel => P::RSTRING_WRONG_MODEL,
    };
    ActionError::ActionError(r)
}

#[inline(always)]
pub fn translate_ddl_error<P: ProtocolSpec, T>(r: Result<T, DdlError>) -> Result<T, ActionError> {
    match r {
        Ok(r) => Ok(r),
        Err(e) => Err(map_ddl_error_to_status::<P>(e)),
    }
}

pub fn ensure_length<P: ProtocolSpec>(len: usize, is_valid: fn(usize) -> bool) -> ActionResult<()> {
    if util::compiler::likely(is_valid(len)) {
        Ok(())
    } else {
        util::err(P::RCODE_ACTION_ERR)
    }
}

pub fn ensure_boolean_or_aerr<P: ProtocolSpec>(boolean: bool) -> ActionResult<()> {
    if util::compiler::likely(boolean) {
        Ok(())
    } else {
        util::err(P::RCODE_ACTION_ERR)
    }
}

pub mod heya {
    //! Respond to `HEYA` queries
    use crate::dbnet::connection::prelude::*;
    action!(
        /// Returns a `HEY!` `Response`
        fn heya(_handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
            ensure_length::<P>(act.len(), |len| len == 0 || len == 1)?;
            if act.len() == 1 {
                let raw_byte = unsafe { act.next_unchecked() };
                con.write_mono_length_prefixed_with_tsymbol(raw_byte, b'+')
                    .await?;
            } else {
                con._write_raw(P::ELEMRESP_HEYA).await?;
            }
            Ok(())
        }
    );
}
