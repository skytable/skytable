/*
 * Created on Fri Jun 25 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

#[macro_use]
mod macros;
pub mod compiler;
pub mod os;
pub mod error;
use crate::actions::{ActionError, ActionResult};
use crate::protocol::responses::groups;
use core::fmt::Debug;
use core::future::Future;
use core::ops::Deref;
use core::pin::Pin;
use std::process;

const EXITCODE_ONE: i32 = 0x01;
pub type FutureResult<'s, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 's>>;

/// # Unsafe unwrapping
///
/// This trait provides a method `unsafe_unwrap` that is potentially unsafe and has
/// the ability to **violate multiple safety gurantees** that rust provides. So,
/// if you get `SIGILL`s or `SIGSEGV`s, by using this trait, blame yourself.
///
/// # Safety
/// Use this when you're absolutely sure that the error case is never reached
pub unsafe trait Unwrappable<T> {
    /// Unwrap a _nullable_ (almost) type to get its value while asserting that the value
    /// cannot ever be null
    ///
    /// ## Safety
    /// The trait is unsafe, and so is this function. You can wreck potential havoc if you
    /// use this heedlessly
    ///
    unsafe fn unsafe_unwrap(self) -> T;
}

unsafe impl<T, E> Unwrappable<T> for Result<T, E> {
    unsafe fn unsafe_unwrap(self) -> T {
        match self {
            Ok(t) => t,
            Err(_) => impossible!(),
        }
    }
}

unsafe impl<T> Unwrappable<T> for Option<T> {
    unsafe fn unsafe_unwrap(self) -> T {
        match self {
            Some(t) => t,
            None => impossible!(),
        }
    }
}

pub trait UnwrapActionError<T> {
    fn unwrap_or_custom_aerr(self, e: impl Into<ActionError>) -> ActionResult<T>;
    fn unwrap_or_aerr(self) -> ActionResult<T>;
}

impl<T> UnwrapActionError<T> for Option<T> {
    fn unwrap_or_custom_aerr(self, e: impl Into<ActionError>) -> ActionResult<T> {
        self.ok_or_else(|| e.into())
    }
    fn unwrap_or_aerr(self) -> ActionResult<T> {
        self.ok_or_else(|| groups::ACTION_ERR.into())
    }
}

pub fn exit_error() -> ! {
    process::exit(EXITCODE_ONE)
}

/// Returns a Result with the provided error
#[inline(never)]
#[cold]
pub fn err<T, E>(e: impl Into<E>) -> Result<T, E> {
    Err(e.into())
}

/// This is used to hack around multiple trait system boundaries
/// like deref coercion recursions
#[derive(Debug)]
pub struct Wrapper<T> {
    inner: T,
}

impl<T> Wrapper<T> {
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: Clone> Wrapper<T> {
    pub fn inner_clone(&self) -> T {
        self.inner.clone()
    }
}

impl<T> Deref for Wrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Clone> Clone for Wrapper<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
