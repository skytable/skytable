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
pub mod error;
pub mod os;
use {
    crate::{
        actions::{ActionError, ActionResult},
        protocol::interface::ProtocolSpec,
    },
    core::{fmt::Debug, future::Future, marker::PhantomData, ops::Deref, pin::Pin},
    std::process,
};

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
    fn unwrap_or_aerr<P: ProtocolSpec>(self) -> ActionResult<T>;
}

impl<T> UnwrapActionError<T> for Option<T> {
    fn unwrap_or_custom_aerr(self, e: impl Into<ActionError>) -> ActionResult<T> {
        self.ok_or_else(|| e.into())
    }
    fn unwrap_or_aerr<P: ProtocolSpec>(self) -> ActionResult<T> {
        self.ok_or_else(|| P::RCODE_ACTION_ERR.into())
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

#[derive(Debug, PartialEq)]
/// This is yet another compiler hack and has no "actual impact" in terms of memory alignment.
///
/// When it's hard to have a _split mutable borrow_, all across the source we use custom
/// fat pointers which are inherently unbounded in their lifetime; this is needed in cases where
/// it's **impossible** to do so. But when you can _somehow_ bind a lifetime without causing
/// a compiler error, it is always good to do so to avoid misuse of the previously mentioned
/// fat pointers. This is exactly what this type does. It binds a context-dependent lifetime
/// to some type which preferably has no other lifetime (something like an `UnsafeSlice`, for
/// example)
///
/// ## Important notes
/// - lifetimes are context captured by the compiler. so if this doesn't work, we'll need
/// to explicitly annotate bounds
/// - this type derefs to the base type
#[derive(Copy, Clone)]
pub struct Life<'a, T> {
    _lt: PhantomData<&'a T>,
    v: T,
}

impl<'a, T> Life<'a, T> {
    /// Ensure compile-time alignment (this is just a sanity check)
    const _ENSURE_COMPILETIME_ALIGN: () =
        assert!(std::mem::align_of::<Life<Vec<u8>>>() == std::mem::align_of::<Vec<u8>>());

    #[inline(always)]
    pub const fn new(v: T) -> Self {
        Life {
            v,
            _lt: PhantomData,
        }
    }
}

impl<'a, T> From<T> for Life<'a, T> {
    fn from(v: T) -> Self {
        Self::new(v)
    }
}

impl<'a, T> Deref for Life<'a, T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.v
    }
}

impl<'a, T> AsRef<T> for Life<'a, T> {
    #[inline(always)]
    fn as_ref(&self) -> &T {
        Deref::deref(self)
    }
}

impl<'a, T: PartialEq> PartialEq<T> for Life<'a, T> {
    #[inline(always)]
    fn eq(&self, other: &T) -> bool {
        PartialEq::eq(&self.v, other)
    }
}

unsafe impl<'a, T: Send> Send for Life<'a, T> {}
unsafe impl<'a, T: Sync> Sync for Life<'a, T> {}
