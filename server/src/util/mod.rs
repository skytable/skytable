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
#[cfg(test)]
pub mod test_utils;
use {
    core::{
        fmt::{self, Debug},
        marker::PhantomData,
        mem::{self, MaybeUninit},
        ops::Deref,
        slice,
    },
    std::process,
};

pub const IS_ON_CI: bool = option_env!("CI").is_some();

const EXITCODE_ONE: i32 = 0x01;

pub fn bx_to_vec<T>(bx: Box<[T]>) -> Vec<T> {
    Vec::from(bx)
}

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
#[repr(transparent)]
/// This is yet another compiler hack and has no "actual impact" in terms of memory alignment.
///
/// When it's hard to have a _split mutable borrow_, all across the source we use custom
/// fat pointers which are inherently unbounded in their lifetime; this is needed in cases where
/// it's **impossible** to do so. But when you can _somehow_ bind a lifetime without causing
/// a compiler error, it is always good to do so to avoid misuse of the previously mentioned
/// fat pointers. This is exactly what this type does. It binds a context-dependent lifetime
/// to some type which preferably has no other lifetime (something like an `UnsafeSlice`, for
/// example).
///
/// How do you access this? Always consider using [`AsRef::as_ref`] to get a ref to the inner
/// type and then do whatever you like. Move semantics to the inner type are prohibited (and
/// marked unsafe)
///
/// ## Important notes
/// - lifetimes are context captured by the compiler. so if this doesn't work, we'll need
/// to explicitly annotate bounds
/// - this type derefs to the base type
#[derive(Copy, Clone)]
pub struct Life<'a, T: 'a> {
    _lt: PhantomData<&'a T>,
    v: T,
}

impl<'a, T: 'a> Life<'a, T> {
    /// Ensure compile-time alignment (this is just a sanity check)
    const _ENSURE_COMPILETIME_ALIGN: () =
        assert!(std::mem::align_of::<Life<T>>() == std::mem::align_of::<T>());

    #[inline(always)]
    pub const fn new(v: T) -> Self {
        let _ = Self::_ENSURE_COMPILETIME_ALIGN;
        Life {
            v,
            _lt: PhantomData,
        }
    }
    /// Get the inner value
    /// # Safety
    /// The caller must ensure that the returned value outlives the proposed lifetime
    pub unsafe fn into_inner(self) -> T {
        self.v
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

/// [`MaybeInit`] is a structure that is like an [`Option`] in debug mode and like
/// [`MaybeUninit`] in release mode. This means that provided there are good enough test cases, most
/// incorrect `assume_init` calls should be detected in the test phase.
#[cfg_attr(not(test), repr(transparent))]
pub struct MaybeInit<T> {
    #[cfg(test)]
    is_init: bool,
    #[cfg(not(test))]
    is_init: (),
    base: MaybeUninit<T>,
}

impl<T> MaybeInit<T> {
    /// Initialize a new uninitialized variant
    #[inline(always)]
    pub const fn uninit() -> Self {
        Self {
            #[cfg(test)]
            is_init: false,
            #[cfg(not(test))]
            is_init: (),
            base: MaybeUninit::uninit(),
        }
    }
    /// Initialize with a value
    #[inline(always)]
    pub const fn new(val: T) -> Self {
        Self {
            #[cfg(test)]
            is_init: true,
            #[cfg(not(test))]
            is_init: (),
            base: MaybeUninit::new(val),
        }
    }
    const fn ensure_init(#[cfg(test)] is_init: bool, #[cfg(not(test))] is_init: ()) {
        #[cfg(test)]
        {
            if !is_init {
                panic!("Tried to `assume_init` on uninitialized data");
            }
        }
        let _ = is_init;
    }
    /// Assume that `self` is initialized and return the inner value
    ///
    /// ## Safety
    ///
    /// Caller needs to ensure that the data is actually initialized
    #[inline(always)]
    pub const unsafe fn assume_init(self) -> T {
        Self::ensure_init(self.is_init);
        self.base.assume_init()
    }
    /// Assume that `self` is initialized and return a reference
    ///
    /// ## Safety
    ///
    /// Caller needs to ensure that the data is actually initialized
    #[inline(always)]
    pub const unsafe fn assume_init_ref(&self) -> &T {
        Self::ensure_init(self.is_init);
        self.base.assume_init_ref()
    }
    /// Assumes `self` is initialized, replaces `self` with an uninit state, returning
    /// the older value
    ///
    /// ## Safety
    pub unsafe fn take(&mut self) -> T {
        Self::ensure_init(self.is_init);
        let mut r = MaybeUninit::uninit();
        mem::swap(&mut r, &mut self.base);
        #[cfg(test)]
        {
            self.is_init = false;
        }
        r.assume_init()
    }
}

#[cfg(test)]
impl<T: fmt::Debug> fmt::Debug for MaybeInit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dat_fmt = if self.is_init {
            unsafe { format!("{:?}", self.base.assume_init_ref()) }
        } else {
            "MaybeUninit {..}".to_string()
        };
        f.debug_struct("MaybeInit")
            .field("is_init", &self.is_init)
            .field("base", &dat_fmt)
            .finish()
    }
}

#[cfg(not(test))]
impl<T: fmt::Debug> fmt::Debug for MaybeInit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaybeInit")
            .field("base", &self.base)
            .finish()
    }
}

pub unsafe trait ByteRepr {
    fn repr(&self) -> &[u8];
}

unsafe impl ByteRepr for [u8] {
    fn repr(&self) -> &[u8] {
        self
    }
}
unsafe impl ByteRepr for str {
    fn repr(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub trait NumericRepr: ByteRepr {
    fn be(&self) -> Self;
    fn le(&self) -> Self;
}

macro_rules! byte_repr_impls {
    ($($ty:ty),*) => {
        $(
            unsafe impl ByteRepr for $ty { fn repr(&self) -> &[u8] { unsafe { slice::from_raw_parts(self as *const $ty as *const u8, mem::size_of::<Self>()) } } }
            impl NumericRepr for $ty { fn be(&self) -> $ty { <$ty>::to_be(*self) } fn le(&self) -> $ty { <$ty>::to_le(*self) } }
        )*
    };
}

byte_repr_impls!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, usize, isize);

pub const fn copy_slice_to_array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    assert!(bytes.len() <= N);
    let mut data = [0u8; N];
    let mut i = 0;
    while i < bytes.len() {
        data[i] = bytes[i];
        i += 1;
    }
    data
}
pub const fn copy_str_to_array<const N: usize>(str: &str) -> [u8; N] {
    copy_slice_to_array(str.as_bytes())
}
/// Copy the elements of a into b, beginning the copy at `pos`
pub const fn copy_a_into_b<const M: usize, const N: usize>(
    from: [u8; M],
    mut to: [u8; N],
    mut pos: usize,
) -> [u8; N] {
    assert!(M <= N);
    assert!(pos < N);
    let mut i = 0;
    while i < M {
        to[pos] = from[i];
        i += 1;
        pos += 1;
    }
    to
}

pub struct Threshold<const N: usize> {
    now: usize,
}

impl<const N: usize> Threshold<N> {
    pub const fn new() -> Self {
        Self { now: 0 }
    }
    pub fn bust_one(&mut self) {
        self.now += 1;
    }
    pub fn not_busted(&self) -> bool {
        self.now < N
    }
}

pub trait EndianQW {
    fn u64_bytes_le(&self) -> [u8; 8];
    fn u64_bytes_be(&self) -> [u8; 8];
    fn u64_bytes_ne(&self) -> [u8; 8] {
        if cfg!(target_endian = "big") {
            self.u64_bytes_be()
        } else {
            self.u64_bytes_le()
        }
    }
}

pub trait EndianDW {
    fn u32_bytes_le(&self) -> [u8; 8];
    fn u32_bytes_be(&self) -> [u8; 8];
    fn u32_bytes_ne(&self) -> [u8; 8] {
        if cfg!(target_endian = "big") {
            self.u32_bytes_be()
        } else {
            self.u32_bytes_le()
        }
    }
}

macro_rules! impl_endian {
    ($($ty:ty),*) => {
        $(impl EndianQW for $ty {
            fn u64_bytes_le(&self) -> [u8; 8] { (*self as u64).to_le_bytes() }
            fn u64_bytes_be(&self) -> [u8; 8] { (*self as u64).to_le_bytes() }
        })*
    }
}

impl_endian!(u8, i8, u16, i16, u32, i32, u64, i64, usize, isize);

pub fn time_now_string() -> String {
    chrono::Local::now().format("%Y%m%d_%H%M%S").to_string()
}

pub fn time_now_with_postfix(post_fix: &str) -> String {
    // Concatenate the formatted date and time with the postfix
    format!("{}-{}", time_now_string(), post_fix)
}
