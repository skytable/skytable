#![feature(prelude_import)]
/*
 * Created on Thu Jul 02 2020
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

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]
#![deny(unused_must_use)]

//! # Skytable
//!
//! The `skyd` crate (or the `server` folder) is Skytable's database server and maybe
//! is the most important part of the project. There are several modules within this crate; see
//! the modules for their respective documentation.
#[prelude_import]
use std::prelude::rust_2021::*;
#[macro_use]
extern crate std;

use {
    crate::{
        config::ConfigurationSet, diskstore::flock::FileLock, util::exit_error,
    },
    env_logger::Builder, libsky::{URL, VERSION},
    std::{env, process},
};

#[macro_use]
pub mod util {







    // Start the server which asynchronously waits for a CTRL+C signal
    // which will safely shut down the server
    // check if any other process is using the data directory and lock it if not (else error)
    // important: create the pid_file just here and nowhere else because check_args can also
    // involve passing --help or wrong arguments which can falsely create a PID file
    // Make sure all background workers terminate
    // uh oh, something happened while starting up
    // remove this file in debug builds for harness to pick it up

    // print warnings if any

    #[macro_use]
    mod macros {
        #[macro_export]
        macro_rules! impossible {
            () => { core :: hint :: unreachable_unchecked() } ;
        }
        #[macro_export]
        macro_rules! consts {
            ($($(#[$attr : meta]) * $ident : ident : $ty : ty = $expr : expr
            ;) *) => { $($(#[$attr]) * const $ident : $ty = $expr ;) * } ;
            ($($(#[$attr : meta]) * $vis : vis $ident : ident : $ty : ty =
            $expr : expr ;) *) =>
            { $($(#[$attr]) * $vis const $ident : $ty = $expr ;) * } ;
        }
        #[macro_export]
        macro_rules! typedef {
            ($($(#[$attr : meta]) * $ident : ident = $ty : ty ;) *) =>
            { $($(#[$attr]) * type $ident = $ty ;) * } ;
            ($($(#[$attr : meta]) * $vis : vis $ident : ident = $ty : ty ;) *)
            => { $($(#[$attr]) * $vis type $ident = $ty ;) * } ;
        }
        #[macro_export]
        macro_rules! cfg_test {
            ($block : block) => { #[cfg(test)] $block } ; ($($item : item) *)
            => { $(#[cfg(test)] $item) * } ;
        }
        #[macro_export]
        /// Compare two vectors irrespective of their elements' position
        macro_rules! veceq {
            ($v1 : expr, $v2 : expr) =>
            {
                $v1.len() == $v2.len() &&
                $v1.iter().all(| v | $v2.contains(v))
            } ;
        }
        #[macro_export]
        macro_rules! assert_veceq {
            ($v1 : expr, $v2 : expr) => { assert! (veceq! ($v1, $v2)) } ;
        }
        #[macro_export]
        macro_rules! hmeq {
            ($h1 : expr, $h2 : expr) =>
            {
                $h1.len() == $h2.len() &&
                $h1.iter().all(| (k, v) | $h2.get(k).unwrap().eq(v))
            } ;
        }
        #[macro_export]
        macro_rules! assert_hmeq {
            ($h1 : expr, $h2 : expr) => { assert! (hmeq! ($h1, $h2)) } ;
        }
        #[macro_export]
        /// ## The action macro
        ///
        /// A macro for adding all the _fuss_ to an action. Implementing actions should be simple
        /// and should not require us to repeatedly specify generic paramters and/or trait bounds.
        /// This is exactly what this macro does: does all the _magic_ behind the scenes for you,
        /// including adding generic parameters, handling docs (if any), adding the correct
        /// trait bounds and finally making your function async. Rest knowing that all your
        /// action requirements have been happily addressed with this macro and that you don't have
        /// to write a lot of code to do the exact same thing
        ///
        ///
        /// ## Limitations
        ///
        /// This macro can only handle mutable parameters for a fixed number of arguments (three)
        ///
        macro_rules! action {
            ($($(#[$attr : meta]) * fn $fname :
            ident($($argname : ident : $argty : ty), *) $block : block) *) =>
            {
                $($(#[$attr]) * pub async fn $fname < 'a, T : 'a + $crate ::
                dbnet :: connection :: ClientConnection < P, Strm >, Strm :
                $crate :: dbnet :: connection :: Stream, P : $crate ::
                protocol :: interface :: ProtocolSpec >
                ($($argname : $argty,) *) -> $crate :: actions :: ActionResult
                < () > $block) *
            } ;
            ($($(#[$attr : meta]) * fn $fname :
            ident($argone : ident : $argonety : ty, $argtwo : ident :
            $argtwoty : ty, mut $argthree : ident : $argthreety : ty) $block :
            block) *) =>
            {
                $($(#[$attr]) * pub async fn $fname < 'a, T : 'a + $crate ::
                dbnet :: connection :: ClientConnection < P, Strm >, Strm :
                $crate :: dbnet :: connection :: Stream, P : $crate ::
                protocol :: interface :: ProtocolSpec >
                ($argone : $argonety, $argtwo : $argtwoty, mut $argthree :
                $argthreety) -> $crate :: actions :: ActionResult < () >
                $block) *
            } ;
            ($($(#[$attr : meta]) * fn $fname :
            ident($argone : ident : $argonety : ty, $argtwo : ident :
            $argtwoty : ty, $argthree : ident : $argthreety : ty) $block :
            block) *) =>
            {
                $($(#[$attr]) * pub async fn $fname < 'a, T : 'a + $crate ::
                dbnet :: connection :: ClientConnection < P, Strm >, Strm :
                $crate :: dbnet :: connection :: Stream, P : $crate ::
                protocol :: interface :: ProtocolSpec >
                ($argone : $argonety, $argtwo : $argtwoty, $argthree :
                $argthreety) -> $crate :: actions :: ActionResult < () >
                $block) *
            } ;
        }
        #[macro_export]
        macro_rules! byt { ($f : expr) => { bytes :: Bytes :: from($f) } ; }
        #[macro_export]
        macro_rules! bi {
            ($($x : expr), + $(,) ?) =>
            { { vec! [$(bytes :: Bytes :: from($x),) *].into_iter() } } ;
        }
        #[macro_export]
        macro_rules! do_sleep {
            ($dur : literal s) =>
            {
                {
                    std :: thread ::
                    sleep(std :: time :: Duration :: from_secs($dur)) ;
                }
            } ;
        }
        macro_rules! ucidx {
            ($base : expr, $idx : expr) =>
            { * ($base.as_ptr().add($idx as usize)) } ;
        }
        /// If you provide: [T; N] with M initialized elements, then you are given
        /// [MaybeUninit<T>; N] with M initialized elements and N-M uninit elements
        macro_rules! uninit_array {
            ($($vis : vis const $id : ident : [$ty : ty ; $len : expr] =
            [$($init_element : expr), *] ;) *) =>
            {
                $($vis const $id :
                [:: core :: mem :: MaybeUninit < $ty > ; $len] =
                {
                    let mut ret =
                    [:: core :: mem :: MaybeUninit :: uninit() ; $len] ; let mut
                    idx = 0 ;
                    $(idx += 1 ; ret [idx - 1] = :: core :: mem :: MaybeUninit
                    :: new($init_element) ;) * ret
                } ;) *
            } ;
        }
        #[macro_export]
        macro_rules! def {
            ($(#[$attr : meta]) * $vis : vis struct $ident : ident
            {
                $($(#[$fattr : meta]) * $field : ident : $ty : ty = $defexpr :
                expr), * $(,) ?
            }) =>
            {
                $(#[$attr]) * $vis struct $ident
                { $($(#[$fattr]) * $field : $ty,) * } impl :: core :: default
                :: Default for $ident
                { fn default() -> Self { Self { $($field : $defexpr,) * } } }
            } ;
        }
        #[macro_export]
        macro_rules! bench {
            ($vis : vis mod $modname : ident ;) =>
            { #[cfg(all(feature = "nightly", test))] $vis mod $modname ; } ;
        }
    }
    pub mod compiler {
        //! Dark compiler arts and hackery to defy the normal. Use at your own
        //! risk
        use core::mem;
        #[cold]
        #[inline(never)]
        pub const fn cold() {}
        pub const fn likely(b: bool) -> bool { if !b { cold() } b }
        pub const fn unlikely(b: bool) -> bool { if b { cold() } b }
        #[cold]
        #[inline(never)]
        pub const fn cold_err<T>(v: T) -> T { v }
        #[inline(always)]
        #[allow(unused)]
        pub const fn hot<T>(v: T) -> T { if false { cold() } v }
        /// # Safety
        /// The caller is responsible for ensuring lifetime validity
        pub const unsafe fn extend_lifetime<'a, 'b, T>(inp: &'a T) -> &'b T {
            mem::transmute(inp)
        }
        /// # Safety
        /// The caller is responsible for ensuring lifetime validity
        pub unsafe fn extend_lifetime_mut<'a, 'b, T>(inp: &'a mut T)
            -> &'b mut T {
            mem::transmute(inp)
        }
    }
    pub mod error {
        use crate::storage::v1::{
            error::StorageEngineError, sengine::SnapshotEngineError,
        };
        use openssl::{
            error::ErrorStack as SslErrorStack, ssl::Error as SslError,
        };
        use std::{fmt, io::Error as IoError};
        pub type SkyResult<T> = Result<T, Error>;
        pub enum Error {
            Storage(StorageEngineError),
            IoError(IoError),
            IoErrorExtra(IoError, String),
            OtherError(String),
            TlsError(SslError),
            SnapshotEngineError(SnapshotEngineError),
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Error {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&Error::Storage(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "Storage");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&Error::IoError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "IoError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&Error::IoErrorExtra(ref __self_0, ref __self_1),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "IoErrorExtra");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_1));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&Error::OtherError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "OtherError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&Error::TlsError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "TlsError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&Error::SnapshotEngineError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f,
                                    "SnapshotEngineError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl Error {
            pub fn ioerror_extra(ioe: IoError, extra: impl ToString) -> Self {
                Self::IoErrorExtra(ioe, extra.to_string())
            }
        }
        impl fmt::Display for Error {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Self::Storage(serr) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Storage engine error: "],
                                &[::core::fmt::ArgumentV1::new_display(&serr)])),
                    Self::IoError(nerr) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["I/O error: "],
                                &[::core::fmt::ArgumentV1::new_display(&nerr)])),
                    Self::IoErrorExtra(ioe, extra) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["I/O error while ",
                                            ": "],
                                &[::core::fmt::ArgumentV1::new_display(&extra),
                                            ::core::fmt::ArgumentV1::new_display(&ioe)])),
                    Self::OtherError(oerr) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Error: "],
                                &[::core::fmt::ArgumentV1::new_display(&oerr)])),
                    Self::TlsError(terr) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["TLS error: "],
                                &[::core::fmt::ArgumentV1::new_display(&terr)])),
                    Self::SnapshotEngineError(snaperr) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Snapshot engine error: "],
                                &[::core::fmt::ArgumentV1::new_display(&snaperr)])),
                }
            }
        }
        impl From<IoError> for Error {
            fn from(ioe: IoError) -> Self { Self::IoError(ioe) }
        }
        impl From<StorageEngineError> for Error {
            fn from(see: StorageEngineError) -> Self { Self::Storage(see) }
        }
        impl From<SslError> for Error {
            fn from(sslerr: SslError) -> Self { Self::TlsError(sslerr) }
        }
        impl From<SslErrorStack> for Error {
            fn from(estack: SslErrorStack) -> Self {
                Self::TlsError(estack.into())
            }
        }
        impl From<SnapshotEngineError> for Error {
            fn from(snaperr: SnapshotEngineError) -> Self {
                Self::SnapshotEngineError(snaperr)
            }
        }
    }
    pub mod os {
        #[cfg(unix)]
        pub use unix::*;
        use std::ffi::OsStr;
        #[cfg(unix)]
        mod unix {
            use libc::{rlimit, RLIMIT_NOFILE};
            use std::io::Error as IoError;
            pub struct ResourceLimit {
                cur: u64,
                max: u64,
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for ResourceLimit {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match *self {
                        ResourceLimit { cur: ref __self_0_0, max: ref __self_0_1 }
                            => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_struct(f,
                                        "ResourceLimit");
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder, "cur",
                                    &&(*__self_0_0));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder, "max",
                                    &&(*__self_0_1));
                            ::core::fmt::DebugStruct::finish(debug_trait_builder)
                        }
                    }
                }
            }
            impl ResourceLimit {
                const fn new(cur: u64, max: u64) -> Self { Self { cur, max } }
                pub const fn is_over_limit(&self, expected: usize) -> bool {
                    expected as u64 > self.cur
                }
                /// Returns the maximum number of open files
                pub fn get() -> Result<Self, IoError> {
                    unsafe {
                        let rlim = rlimit { rlim_cur: 0, rlim_max: 0 };
                        let ret =
                            libc::getrlimit(RLIMIT_NOFILE, &rlim as *const _ as *mut _);
                        if ret != 0 {
                                Err(IoError::last_os_error())
                            } else {

                               #[allow(clippy :: useless_conversion)]
                               Ok(ResourceLimit::new(rlim.rlim_cur.into(),
                                       rlim.rlim_max.into()))
                           }
                    }
                }
                /// Returns the current limit
                pub const fn current(&self) -> u64 { self.cur }
                /// Returns the max limit
                pub const fn max(&self) -> u64 { self.max }
            }
            use std::future::Future;
            use std::pin::Pin;
            use std::task::{Context, Poll};
            use tokio::signal::unix::{signal, Signal, SignalKind};
            pub struct TerminationSignal {
                sigint: Signal,
                sigterm: Signal,
            }
            impl TerminationSignal {
                pub fn init() -> crate::IoResult<Self> {
                    let sigint = signal(SignalKind::interrupt())?;
                    let sigterm = signal(SignalKind::terminate())?;
                    Ok(Self { sigint, sigterm })
                }
            }
            impl Future for TerminationSignal {
                type Output = Option<()>;
                fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>)
                    -> Poll<Self::Output> {
                    let int = self.sigint.poll_recv(ctx);
                    let term = self.sigterm.poll_recv(ctx);
                    match (int, term) {
                        (Poll::Ready(p), _) | (_, Poll::Ready(p)) => Poll::Ready(p),
                        _ => Poll::Pending,
                    }
                }
            }
        }
        use crate::IoResult;
        use std::fs;
        use std::path::Path;
        /// Recursively copy files from the given `src` to the provided `dest`
        pub fn recursive_copy(src: impl AsRef<Path>, dst: impl AsRef<Path>)
            -> IoResult<()> {
            fs::create_dir_all(&dst)?;
            for entry in fs::read_dir(src)? {
                let entry = entry?;
                match entry.file_type()? {
                    ft if ft.is_dir() => {
                        recursive_copy(entry.path(),
                                dst.as_ref().join(entry.file_name()))?;
                    }
                    _ => {
                        fs::copy(entry.path(),
                                dst.as_ref().join(entry.file_name()))?;
                    }
                }
            }
            Ok(())
        }
        pub enum EntryKind { Directory(String), File(String), }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for EntryKind {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&EntryKind::Directory(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "Directory");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&EntryKind::File(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "File");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for EntryKind {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for EntryKind {
            #[inline]
            fn eq(&self, other: &EntryKind) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&EntryKind::Directory(ref __self_0),
                                    &EntryKind::Directory(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                (&EntryKind::File(ref __self_0),
                                    &EntryKind::File(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { false }
                }
            }
            #[inline]
            fn ne(&self, other: &EntryKind) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&EntryKind::Directory(ref __self_0),
                                    &EntryKind::Directory(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                (&EntryKind::File(ref __self_0),
                                    &EntryKind::File(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { true }
                }
            }
        }
        impl EntryKind {
            pub fn into_inner(self) -> String {
                match self {
                    Self::Directory(path) | Self::File(path) => path,
                }
            }
            pub fn get_inner(&self) -> &str {
                match self { Self::Directory(rf) | Self::File(rf) => rf, }
            }
        }
        impl ToString for EntryKind {
            fn to_string(&self) -> String { self.get_inner().to_owned() }
        }
        impl AsRef<str> for EntryKind {
            fn as_ref(&self) -> &str { self.get_inner() }
        }
        impl AsRef<OsStr> for EntryKind {
            fn as_ref(&self) -> &OsStr { OsStr::new(self.get_inner()) }
        }
        /// Returns a vector with a complete list of entries (both directories and files)
        /// in the given path (recursive extraction)
        pub fn rlistdir(path: impl AsRef<Path>)
            -> crate::IoResult<Vec<EntryKind>> {
            let mut ret = Vec::new();
            rlistdir_inner(path.as_ref(), &mut ret)?;
            Ok(ret)
        }
        fn rlistdir_inner(path: &Path, paths: &mut Vec<EntryKind>)
            -> crate::IoResult<()> {
            let dir = fs::read_dir(path)?;
            for entry in dir {
                let entry = entry?;
                let path = entry.path();
                let path_str = path.to_string_lossy().to_string();
                if path.is_dir() {
                        paths.push(EntryKind::Directory(path_str));
                        rlistdir_inner(&path, paths)?;
                    } else { paths.push(EntryKind::File(path_str)); }
            }
            Ok(())
        }
        fn dir_size_inner(dir: fs::ReadDir) -> IoResult<u64> {
            let mut ret = 0;
            for entry in dir {
                let entry = entry?;
                let size =
                    match entry.metadata()? {
                        meta if meta.is_dir() =>
                            dir_size_inner(fs::read_dir(entry.path())?)?,
                        meta => meta.len(),
                    };
                ret += size;
            }
            Ok(ret)
        }
        /// Returns the size of a directory by recursively scanning it
        pub fn dirsize(path: impl AsRef<Path>) -> IoResult<u64> {
            dir_size_inner(fs::read_dir(path.as_ref())?)
        }
    }
    use {
        crate::{
            actions::{ActionError, ActionResult},
            protocol::interface::ProtocolSpec,
        },
        core::{
            fmt::Debug, future::Future, marker::PhantomData, ops::Deref,
            pin::Pin,
        },
        std::process,
    };
    const EXITCODE_ONE: i32 = 0x01;
    pub type FutureResult<'s, T> =
        Pin<Box<dyn Future<Output = T> + Send + Sync + 's>>;
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
        unsafe fn unsafe_unwrap(self)
        -> T;
    }
    unsafe impl<T, E> Unwrappable<T> for Result<T, E> {
        unsafe fn unsafe_unwrap(self) -> T {
            match self {
                Ok(t) => t,
                Err(_) => core::hint::unreachable_unchecked(),
            }
        }
    }
    unsafe impl<T> Unwrappable<T> for Option<T> {
        unsafe fn unsafe_unwrap(self) -> T {
            match self {
                Some(t) => t,
                None => core::hint::unreachable_unchecked(),
            }
        }
    }
    pub trait UnwrapActionError<T> {
        fn unwrap_or_custom_aerr(self, e: impl Into<ActionError>)
        -> ActionResult<T>;
        fn unwrap_or_aerr<P: ProtocolSpec>(self)
        -> ActionResult<T>;
    }
    impl<T> UnwrapActionError<T> for Option<T> {
        fn unwrap_or_custom_aerr(self, e: impl Into<ActionError>)
            -> ActionResult<T> {
            self.ok_or_else(|| e.into())
        }
        fn unwrap_or_aerr<P: ProtocolSpec>(self) -> ActionResult<T> {
            self.ok_or_else(|| P::RCODE_ACTION_ERR.into())
        }
    }
    pub fn exit_error() -> ! { process::exit(EXITCODE_ONE) }
    /// Returns a Result with the provided error
    #[inline(never)]
    #[cold]
    pub fn err<T, E>(e: impl Into<E>) -> Result<T, E> { Err(e.into()) }
    /// This is used to hack around multiple trait system boundaries
    /// like deref coercion recursions
    pub struct Wrapper<T> {
        inner: T,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<T: ::core::fmt::Debug> ::core::fmt::Debug for Wrapper<T> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                Wrapper { inner: ref __self_0_0 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "Wrapper");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "inner", &&(*__self_0_0));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl<T> Wrapper<T> {
        pub const fn new(inner: T) -> Self { Self { inner } }
    }
    impl<T: Clone> Wrapper<T> {
        pub fn inner_clone(&self) -> T { self.inner.clone() }
    }
    impl<T> Deref for Wrapper<T> {
        type Target = T;
        fn deref(&self) -> &Self::Target { &self.inner }
    }
    impl<T: Clone> Clone for Wrapper<T> {
        fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
    }
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
    pub struct Life<'a, T> {
        _lt: PhantomData<&'a T>,
        v: T,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a, T: ::core::marker::Copy> ::core::marker::Copy for Life<'a, T> { }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a, T: ::core::clone::Clone> ::core::clone::Clone for Life<'a, T> {
        #[inline]
        fn clone(&self) -> Life<'a, T> {
            match *self {
                Life { _lt: ref __self_0_0, v: ref __self_0_1 } =>
                    Life {
                        _lt: ::core::clone::Clone::clone(&(*__self_0_0)),
                        v: ::core::clone::Clone::clone(&(*__self_0_1)),
                    },
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a, T: ::core::fmt::Debug> ::core::fmt::Debug for Life<'a, T> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                Life { _lt: ref __self_0_0, v: ref __self_0_1 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "Life");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "_lt",
                            &&(*__self_0_0));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "v",
                            &&(*__self_0_1));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl<'a, T> ::core::marker::StructuralPartialEq for Life<'a, T> {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a, T: ::core::cmp::PartialEq> ::core::cmp::PartialEq for Life<'a, T>
        {
        #[inline]
        fn eq(&self, other: &Life<'a, T>) -> bool {
            match *other {
                Life { _lt: ref __self_1_0, v: ref __self_1_1 } =>
                    match *self {
                        Life { _lt: ref __self_0_0, v: ref __self_0_1 } =>
                            (*__self_0_0) == (*__self_1_0) &&
                                (*__self_0_1) == (*__self_1_1),
                    },
            }
        }
        #[inline]
        fn ne(&self, other: &Life<'a, T>) -> bool {
            match *other {
                Life { _lt: ref __self_1_0, v: ref __self_1_1 } =>
                    match *self {
                        Life { _lt: ref __self_0_0, v: ref __self_0_1 } =>
                            (*__self_0_0) != (*__self_1_0) ||
                                (*__self_0_1) != (*__self_1_1),
                    },
            }
        }
    }
    impl<'a, T> Life<'a, T> {
        /// Ensure compile-time alignment (this is just a sanity check)
        const _ENSURE_COMPILETIME_ALIGN: () =
            if !(std::mem::align_of::<Life<Vec<u8>>>() ==
                            std::mem::align_of::<Vec<u8>>()) {
                    ::core::panicking::panic("assertion failed: std::mem::align_of::<Life<Vec<u8>>>() == std::mem::align_of::<Vec<u8>>()")
                };
        #[inline(always)]
        pub const fn new(v: T) -> Self { Life { v, _lt: PhantomData } }
    }
    impl<'a, T> From<T> for Life<'a, T> {
        fn from(v: T) -> Self { Self::new(v) }
    }
    impl<'a, T> Deref for Life<'a, T> {
        type Target = T;
        #[inline(always)]
        fn deref(&self) -> &Self::Target { &self.v }
    }
    impl<'a, T> AsRef<T> for Life<'a, T> {
        #[inline(always)]
        fn as_ref(&self) -> &T { Deref::deref(self) }
    }
    impl<'a, T: PartialEq> PartialEq<T> for Life<'a, T> {
        #[inline(always)]
        fn eq(&self, other: &T) -> bool { PartialEq::eq(&self.v, other) }
    }
    unsafe impl<'a, T: Send> Send for Life<'a, T> {}
    unsafe impl<'a, T: Sync> Sync for Life<'a, T> {}
}
mod actions {
    //! # Actions
    //!
    //! Actions are like shell commands, you provide arguments -- they return output. This module contains a collection
    //! of the actions supported by Skytable
    //!
    #[macro_use]
    mod macros {
        #[macro_export]
        /// endian independent check to see if the lowbit is set or not. Returns true if the lowbit
        /// is set. this is undefined to be applied on signed values on one's complement targets
        macro_rules! is_lowbit_set { ($v : expr) => { $v & 1 == 1 } ; }
        #[macro_export]
        /// endian independent check to see if the lowbit is unset or not. Returns true if the lowbit
        /// is unset. this is undefined to be applied on signed values on one's complement targets
        macro_rules! is_lowbit_unset { ($v : expr) => { $v & 1 == 0 } ; }
        #[macro_export]
        macro_rules! get_tbl {
            ($entity : expr, $store : expr, $con : expr) =>
            {
                {
                    $crate :: actions :: translate_ddl_error :: < P, :: std ::
                    sync :: Arc < $crate :: corestore :: table :: Table >>
                    ($store.get_table($entity),) ?
                }
            } ; ($store : expr, $con : expr) =>
            {
                {
                    match $store.get_ctable()
                    {
                        Some(tbl) => tbl, None => return $crate :: util ::
                        err(P :: RSTRING_DEFAULT_UNSET),
                    }
                }
            } ;
        }
        #[macro_export]
        macro_rules! get_tbl_ref {
            ($store : expr, $con : expr) =>
            {
                {
                    match $store.get_ctable_ref()
                    {
                        Some(tbl) => tbl, None => return $crate :: util ::
                        err(P :: RSTRING_DEFAULT_UNSET),
                    }
                }
            } ;
        }
        #[macro_export]
        macro_rules! handle_entity {
            ($con : expr, $ident : expr) =>
            {
                {
                    match $crate :: queryengine :: parser :: Entity ::
                    from_slice :: < P > (& $ident)
                    { Ok(e) => e, Err(e) => return Err(e.into()), }
                }
            } ;
        }
    }
    pub mod dbsize {
        use crate::dbnet::connection::prelude::*;
        #[doc = r" Returns the number of keys in the database"]
        pub async fn dbsize<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len < 2)?;
            if act.is_empty() {
                    let len =
                        {
                                match handle.get_ctable_ref() {
                                    Some(tbl) => tbl,
                                    None => return crate::util::err(P::RSTRING_DEFAULT_UNSET),
                                }
                            }.count();
                    con.write_usize(len).await?;
                } else {
                   let raw_entity = unsafe { act.next().unsafe_unwrap() };
                   let entity =
                       {
                           match crate::queryengine::parser::Entity::from_slice::<P>(&raw_entity)
                               {
                               Ok(e) => e,
                               Err(e) => return Err(e.into()),
                           }
                       };
                   con.write_usize({
                                       crate::actions::translate_ddl_error::<P,
                                                   ::std::sync::Arc<crate::corestore::table::Table>>(handle.get_table(entity))?
                                   }.count()).await?;
               }
            Ok(())
        }
    }
    pub mod del {
        //! # `DEL` queries
        //! This module provides functions to work with `DEL` queries
        use crate::corestore::table::DataModel;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER;
        use crate::util::compiler;
        #[doc = r" Run a `DEL` query"]
        #[doc = r""]
        #[doc =
        r" Do note that this function is blocking since it acquires a write lock."]
        #[doc = r" It will write an entire datagroup, for this `del` action"]
        pub async fn del<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size != 0)?;
            let table =
                {
                    match handle.get_ctable_ref() {
                        Some(tbl) => tbl,
                        None => return crate::util::err(P::RSTRING_DEFAULT_UNSET),
                    }
                };
            macro_rules! remove {
                ($engine : expr) =>
                {
                    {
                        let encoding_is_okay = ENCODING_LUT_ITER
                        [$engine.is_key_encoded()] (act.as_ref()) ; if compiler ::
                        likely(encoding_is_okay)
                        {
                            let done_howmany : Option < usize > ;
                            {
                                if registry :: state_okay()
                                {
                                    let mut many = 0 ;
                                    act.for_each(| key |
                                    { many += $engine.remove_unchecked(key) as usize ; }) ;
                                    done_howmany = Some(many) ;
                                } else { done_howmany = None ; }
                            } if let Some(done_howmany) = done_howmany
                            { con.write_usize(done_howmany).await ? ; } else
                            { con._write_raw(P :: RCODE_SERVER_ERR).await ? ; }
                        } else { return util :: err(P :: RCODE_ENCODING_ERROR) ; }
                    }
                } ;
            }
            match table.get_model_ref() {
                DataModel::KV(kve) => {
                    {
                        let encoding_is_okay =
                            ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
                        if compiler::likely(encoding_is_okay) {
                                let done_howmany: Option<usize>;
                                {
                                    if registry::state_okay() {
                                            let mut many = 0;
                                            act.for_each(|key|
                                                    { many += kve.remove_unchecked(key) as usize; });
                                            done_howmany = Some(many);
                                        } else { done_howmany = None; }
                                }
                                if let Some(done_howmany) = done_howmany {
                                        con.write_usize(done_howmany).await?;
                                    } else { con._write_raw(P::RCODE_SERVER_ERR).await?; }
                            } else { return util::err(P::RCODE_ENCODING_ERROR); }
                    }
                }
                DataModel::KVExtListmap(kvlmap) => {
                    {
                        let encoding_is_okay =
                            ENCODING_LUT_ITER[kvlmap.is_key_encoded()](act.as_ref());
                        if compiler::likely(encoding_is_okay) {
                                let done_howmany: Option<usize>;
                                {
                                    if registry::state_okay() {
                                            let mut many = 0;
                                            act.for_each(|key|
                                                    { many += kvlmap.remove_unchecked(key) as usize; });
                                            done_howmany = Some(many);
                                        } else { done_howmany = None; }
                                }
                                if let Some(done_howmany) = done_howmany {
                                        con.write_usize(done_howmany).await?;
                                    } else { con._write_raw(P::RCODE_SERVER_ERR).await?; }
                            } else { return util::err(P::RCODE_ENCODING_ERROR); }
                    }
                }
                    #[allow(unreachable_patterns)]
                    _ => return util::err(P::RSTRING_WRONG_MODEL),
            }
            Ok(())
        }
    }
    pub mod exists {
        //! # `EXISTS` queries
        //! This module provides functions to work with `EXISTS` queries
        use crate::corestore::table::DataModel;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER;
        use crate::queryengine::ActionIter;
        use crate::util::compiler;
        #[doc = r" Run an `EXISTS` query"]
        pub async fn exists<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len != 0)?;
            let mut how_many_of_them_exist = 0usize;
            macro_rules! exists {
                ($engine : expr) =>
                {
                    {
                        let encoding_is_okay = ENCODING_LUT_ITER
                        [$engine.is_key_encoded()] (act.as_ref()) ; if compiler ::
                        likely(encoding_is_okay)
                        {
                            act.for_each(| key |
                            {
                                how_many_of_them_exist += $engine.exists_unchecked(key) as
                                usize ;
                            }) ; con.write_usize(how_many_of_them_exist).await ? ;
                        } else { return util :: err(P :: RCODE_ENCODING_ERROR) ; }
                    }
                } ;
            }
            let tbl =
                {
                    match handle.get_ctable_ref() {
                        Some(tbl) => tbl,
                        None => return crate::util::err(P::RSTRING_DEFAULT_UNSET),
                    }
                };
            match tbl.get_model_ref() {
                DataModel::KV(kve) => {
                    let encoding_is_okay =
                        ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
                    if compiler::likely(encoding_is_okay) {
                            act.for_each(|key|
                                    {
                                        how_many_of_them_exist +=
                                            kve.exists_unchecked(key) as usize;
                                    });
                            con.write_usize(how_many_of_them_exist).await?;
                        } else { return util::err(P::RCODE_ENCODING_ERROR); }
                }
                DataModel::KVExtListmap(kve) => {
                    let encoding_is_okay =
                        ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
                    if compiler::likely(encoding_is_okay) {
                            act.for_each(|key|
                                    {
                                        how_many_of_them_exist +=
                                            kve.exists_unchecked(key) as usize;
                                    });
                            con.write_usize(how_many_of_them_exist).await?;
                        } else { return util::err(P::RCODE_ENCODING_ERROR); }
                }
                    #[allow(unreachable_patterns)]
                    _ => return util::err(P::RSTRING_WRONG_MODEL),
            }
            Ok(())
        }
    }
    pub mod flushdb {
        use crate::dbnet::connection::prelude::*;
        use crate::queryengine::ActionIter;
        #[doc = r" Delete all the keys in the database"]
        pub async fn flushdb<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len < 2)?;
            if registry::state_okay() {
                    if act.is_empty() {
                            {
                                    match handle.get_ctable_ref() {
                                        Some(tbl) => tbl,
                                        None => return crate::util::err(P::RSTRING_DEFAULT_UNSET),
                                    }
                                }.truncate_table();
                        } else {
                           let raw_entity = unsafe { act.next_unchecked() };
                           let entity =
                               {
                                   match crate::queryengine::parser::Entity::from_slice::<P>(&raw_entity)
                                       {
                                       Ok(e) => e,
                                       Err(e) => return Err(e.into()),
                                   }
                               };
                           {
                                   crate::actions::translate_ddl_error::<P,
                                               ::std::sync::Arc<crate::corestore::table::Table>>(handle.get_table(entity))?
                               }.truncate_table();
                       }
                    con._write_raw(P::RCODE_OKAY).await?;
                } else { con._write_raw(P::RCODE_SERVER_ERR).await?; }
            Ok(())
        }
    }
    pub mod get {
        //! # `GET` queries
        //! This module provides functions to work with `GET` queries
        use crate::dbnet::connection::prelude::*;
        use crate::util::compiler;
        #[doc = r" Run a `GET` query"]
        pub async fn get<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 1)?;
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            unsafe {
                match kve.get_cloned(act.next_unchecked()) {
                    Ok(Some(val)) => {
                        con.write_mono_length_prefixed_with_tsymbol(&val,
                                    kve.get_value_tsymbol()).await?
                    }
                    Err(_) =>
                        compiler::cold_err(con._write_raw(P::RCODE_ENCODING_ERROR)).await?,
                    Ok(_) => con._write_raw(P::RCODE_NIL).await?,
                }
            }
            Ok(())
        }
    }
    pub mod keylen {
        use crate::dbnet::connection::prelude::*;
        #[doc = r" Run a `KEYLEN` query"]
        #[doc = r""]
        #[doc = r" At this moment, `keylen` only supports a single key"]
        pub async fn keylen<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 1)?;
            let res: Option<usize> =
                {
                    let reader = handle.get_table_with::<P, KVEBlob>()?;
                    unsafe {
                        match reader.get(act.next_unchecked()) {
                            Ok(v) => v.map(|b| b.len()),
                            Err(_) => None,
                        }
                    }
                };
            if let Some(value) = res {
                    con.write_usize(value).await?;
                } else { con._write_raw(P::RCODE_NIL).await?; }
            Ok(())
        }
    }
    pub mod lists {
        #[macro_use]
        mod macros {
            macro_rules! writelist {
                ($con : expr, $listmap : expr, $items : expr) =>
                {
                    {
                        $con.write_typed_non_null_array_header($items.len(),
                        $listmap.get_value_tsymbol()).await ? ; for item in $items
                        {
                            $con.write_typed_non_null_array_element(& item).await ? ;
                        }
                    }
                } ;
            }
        }
        pub mod lget {
            use crate::corestore::Data;
            use crate::dbnet::connection::prelude::*;
            const LEN: &[u8] = "LEN".as_bytes();
            const LIMIT: &[u8] = "LIMIT".as_bytes();
            const VALUEAT: &[u8] = "VALUEAT".as_bytes();
            const LAST: &[u8] = "LAST".as_bytes();
            const FIRST: &[u8] = "FIRST".as_bytes();
            const RANGE: &[u8] = "RANGE".as_bytes();
            struct Range {
                start: usize,
                stop: Option<usize>,
            }
            impl Range {
                pub fn new(start: usize) -> Self {
                    Self { start, stop: None }
                }
                pub fn set_stop(&mut self, stop: usize) {
                    self.stop = Some(stop);
                }
                pub fn into_vec(self, slice: &[Data]) -> Option<Vec<Data>> {
                    slice.get(self.start..self.stop.unwrap_or(slice.len())).map(|slc|
                            slc.to_vec())
                }
            }
            #[doc = r" Handle an `LGET` query for the list model (KVExt)"]
            #[doc = r" ## Syntax"]
            #[doc = r" - `LGET <mylist>` will return the full list"]
            #[doc =
            r" - `LGET <mylist> LEN` will return the length of the list"]
            #[doc =
            r" - `LGET <mylist> LIMIT <limit>` will return a maximum of `limit` elements"]
            #[doc =
            r" - `LGET <mylist> VALUEAT <index>` will return the value at the provided index"]
            #[doc = r" - `LGET <mylist> FIRST` will return the first item"]
            #[doc = r" - `LGET <mylist> LAST` will return the last item"]
            #[doc = r" if it exists"]
            pub async fn lget<'a, T: 'a +
                crate::dbnet::connection::ClientConnection<P, Strm>,
                Strm: crate::dbnet::connection::Stream,
                P: crate::protocol::interface::ProtocolSpec>(handle:
                    &Corestore, con: &mut T, mut act: ActionIter<'a>)
                -> crate::actions::ActionResult<()> {
                ensure_length::<P>(act.len(), |len| len != 0)?;
                let listmap = handle.get_table_with::<P, KVEList>()?;
                let listname = unsafe { act.next_unchecked() };
                macro_rules! get_numeric_count {
                    () =>
                    {
                        match unsafe
                        { String :: from_utf8_lossy(act.next_unchecked()) }.parse ::
                        < usize > ()
                        {
                            Ok(int) => int, Err(_) => return util ::
                            err(P :: RCODE_WRONGTYPE_ERR),
                        }
                    } ;
                }
                match act.next_uppercase().as_ref() {
                    None => {
                        let items =
                            match listmap.list_cloned_full(listname) {
                                Ok(Some(list)) => list,
                                Ok(None) => return Err(P::RCODE_NIL.into()),
                                Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                            };
                        {
                            con.write_typed_non_null_array_header(items.len(),
                                        listmap.get_value_tsymbol()).await?;
                            for item in items {
                                con.write_typed_non_null_array_element(&item).await?;
                            }
                        };
                    }
                    Some(subaction) => {
                        match subaction.as_ref() {
                            LEN => {
                                ensure_length::<P>(act.len(), |len| len == 0)?;
                                match listmap.list_len(listname) {
                                    Ok(Some(len)) => con.write_usize(len).await?,
                                    Ok(None) => return Err(P::RCODE_NIL.into()),
                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                }
                            }
                            LIMIT => {
                                ensure_length::<P>(act.len(), |len| len == 1)?;
                                let count =
                                    match unsafe {
                                                String::from_utf8_lossy(act.next_unchecked())
                                            }.parse::<usize>() {
                                        Ok(int) => int,
                                        Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                                    };
                                match listmap.list_cloned(listname, count) {
                                    Ok(Some(items)) => {
                                        con.write_typed_non_null_array_header(items.len(),
                                                    listmap.get_value_tsymbol()).await?;
                                        for item in items {
                                            con.write_typed_non_null_array_element(&item).await?;
                                        }
                                    }
                                    Ok(None) => return Err(P::RCODE_NIL.into()),
                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                }
                            }
                            VALUEAT => {
                                ensure_length::<P>(act.len(), |len| len == 1)?;
                                let idx =
                                    match unsafe {
                                                String::from_utf8_lossy(act.next_unchecked())
                                            }.parse::<usize>() {
                                        Ok(int) => int,
                                        Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                                    };
                                let maybe_value =
                                    listmap.get(listname).map(|list|
                                            { list.map(|lst| lst.read().get(idx).cloned()) });
                                match maybe_value {
                                    Ok(v) =>
                                        match v {
                                            Some(Some(value)) => {
                                                con.write_mono_length_prefixed_with_tsymbol(&value,
                                                            listmap.get_value_tsymbol()).await?;
                                            }
                                            Some(None) => {
                                                return Err(P::RSTRING_LISTMAP_BAD_INDEX.into());
                                            }
                                            None => { return Err(P::RCODE_NIL.into()); }
                                        },
                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                }
                            }
                            LAST => {
                                ensure_length::<P>(act.len(), |len| len == 0)?;
                                let maybe_value =
                                    listmap.get(listname).map(|list|
                                            { list.map(|lst| lst.read().last().cloned()) });
                                match maybe_value {
                                    Ok(v) =>
                                        match v {
                                            Some(Some(value)) => {
                                                con.write_mono_length_prefixed_with_tsymbol(&value,
                                                            listmap.get_value_tsymbol()).await?;
                                            }
                                            Some(None) =>
                                                return Err(P::RSTRING_LISTMAP_LIST_IS_EMPTY.into()),
                                            None => return Err(P::RCODE_NIL.into()),
                                        },
                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                }
                            }
                            FIRST => {
                                ensure_length::<P>(act.len(), |len| len == 0)?;
                                let maybe_value =
                                    listmap.get(listname).map(|list|
                                            { list.map(|lst| lst.read().first().cloned()) });
                                match maybe_value {
                                    Ok(v) =>
                                        match v {
                                            Some(Some(value)) => {
                                                con.write_mono_length_prefixed_with_tsymbol(&value,
                                                            listmap.get_value_tsymbol()).await?;
                                            }
                                            Some(None) =>
                                                return Err(P::RSTRING_LISTMAP_LIST_IS_EMPTY.into()),
                                            None => return Err(P::RCODE_NIL.into()),
                                        },
                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                }
                            }
                            RANGE => {
                                match act.next_string_owned() {
                                    Some(start) => {
                                        let start: usize =
                                            match start.parse() {
                                                Ok(v) => v,
                                                Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                                            };
                                        let mut range = Range::new(start);
                                        if let Some(stop) = act.next_string_owned() {
                                                let stop: usize =
                                                    match stop.parse() {
                                                        Ok(v) => v,
                                                        Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                                                    };
                                                range.set_stop(stop);
                                            };
                                        match listmap.get(listname) {
                                            Ok(Some(list)) => {
                                                let ret = range.into_vec(&list.read());
                                                match ret {
                                                    Some(ret) => {
                                                        {
                                                            con.write_typed_non_null_array_header(ret.len(),
                                                                        listmap.get_value_tsymbol()).await?;
                                                            for item in ret {
                                                                con.write_typed_non_null_array_element(&item).await?;
                                                            }
                                                        };
                                                    }
                                                    None => return Err(P::RSTRING_LISTMAP_BAD_INDEX.into()),
                                                }
                                            }
                                            Ok(None) => return Err(P::RCODE_NIL.into()),
                                            Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                        }
                                    }
                                    None => return Err(P::RCODE_ACTION_ERR.into()),
                                }
                            }
                            _ => return Err(P::RCODE_UNKNOWN_ACTION.into()),
                        }
                    }
                }
                Ok(())
            }
        }
        pub mod lmod {
            use crate::corestore::Data;
            use crate::dbnet::connection::prelude::*;
            use crate::util::compiler;
            const CLEAR: &[u8] = "CLEAR".as_bytes();
            const PUSH: &[u8] = "PUSH".as_bytes();
            const REMOVE: &[u8] = "REMOVE".as_bytes();
            const INSERT: &[u8] = "INSERT".as_bytes();
            const POP: &[u8] = "POP".as_bytes();
            #[doc = r" Handle `LMOD` queries"]
            #[doc = r" ## Syntax"]
            #[doc = r" - `LMOD <mylist> push <value>`"]
            #[doc = r" - `LMOD <mylist> pop <optional idx>`"]
            #[doc = r" - `LMOD <mylist> insert <index> <value>`"]
            #[doc = r" - `LMOD <mylist> remove <index>`"]
            #[doc = r" - `LMOD <mylist> clear`"]
            pub async fn lmod<'a, T: 'a +
                crate::dbnet::connection::ClientConnection<P, Strm>,
                Strm: crate::dbnet::connection::Stream,
                P: crate::protocol::interface::ProtocolSpec>(handle:
                    &Corestore, con: &mut T, mut act: ActionIter<'a>)
                -> crate::actions::ActionResult<()> {
                ensure_length::<P>(act.len(), |len| len > 1)?;
                let listmap = handle.get_table_with::<P, KVEList>()?;
                let listname = unsafe { act.next_unchecked() };
                macro_rules! get_numeric_count {
                    () =>
                    {
                        match unsafe
                        { String :: from_utf8_lossy(act.next_unchecked()) }.parse ::
                        < usize > ()
                        {
                            Ok(int) => int, Err(_) => return
                            Err(P :: RCODE_WRONGTYPE_ERR.into()),
                        }
                    } ;
                }
                match unsafe { act.next_uppercase_unchecked() }.as_ref() {
                    CLEAR => {
                        ensure_length::<P>(act.len(), |len| len == 0)?;
                        let list =
                            match listmap.get_inner_ref().get(listname) {
                                Some(l) => l,
                                _ => return Err(P::RCODE_NIL.into()),
                            };
                        let okay =
                            if registry::state_okay() {
                                    list.write().clear();
                                    P::RCODE_OKAY
                                } else { P::RCODE_SERVER_ERR };
                        con._write_raw(okay).await?
                    }
                    PUSH => {
                        ensure_boolean_or_aerr::<P>(!act.is_empty())?;
                        let list =
                            match listmap.get_inner_ref().get(listname) {
                                Some(l) => l,
                                _ => return Err(P::RCODE_NIL.into()),
                            };
                        let venc_ok = listmap.get_val_encoder();
                        let ret =
                            if compiler::likely(act.as_ref().all(venc_ok)) {
                                    if registry::state_okay() {
                                            list.write().extend(act.map(Data::copy_from_slice));
                                            P::RCODE_OKAY
                                        } else { P::RCODE_SERVER_ERR }
                                } else { P::RCODE_ENCODING_ERROR };
                        con._write_raw(ret).await?
                    }
                    REMOVE => {
                        ensure_length::<P>(act.len(), |len| len == 1)?;
                        let idx_to_remove =
                            match unsafe {
                                        String::from_utf8_lossy(act.next_unchecked())
                                    }.parse::<usize>() {
                                Ok(int) => int,
                                Err(_) => return Err(P::RCODE_WRONGTYPE_ERR.into()),
                            };
                        if registry::state_okay() {
                                let maybe_value =
                                    listmap.get_inner_ref().get(listname).map(|list|
                                            {
                                                let mut wlock = list.write();
                                                if idx_to_remove < wlock.len() {
                                                        wlock.remove(idx_to_remove);
                                                        true
                                                    } else { false }
                                            });
                                con._write_raw(P::OKAY_BADIDX_NIL_NLUT[maybe_value]).await?
                            } else { return Err(P::RCODE_SERVER_ERR.into()); }
                    }
                    INSERT => {
                        ensure_length::<P>(act.len(), |len| len == 2)?;
                        let idx_to_insert_at =
                            match unsafe {
                                        String::from_utf8_lossy(act.next_unchecked())
                                    }.parse::<usize>() {
                                Ok(int) => int,
                                Err(_) => return Err(P::RCODE_WRONGTYPE_ERR.into()),
                            };
                        let bts = unsafe { act.next_unchecked() };
                        let ret =
                            if compiler::likely(listmap.is_val_ok(bts)) {
                                    if registry::state_okay() {
                                            let maybe_insert =
                                                match listmap.get(listname) {
                                                    Ok(lst) =>
                                                        lst.map(|list|
                                                                {
                                                                    let mut wlock = list.write();
                                                                    if idx_to_insert_at < wlock.len() {
                                                                            wlock.insert(idx_to_insert_at, Data::copy_from_slice(bts));
                                                                            true
                                                                        } else { false }
                                                                }),
                                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                                };
                                            P::OKAY_BADIDX_NIL_NLUT[maybe_insert]
                                        } else { P::RCODE_SERVER_ERR }
                                } else { P::RCODE_ENCODING_ERROR };
                        con._write_raw(ret).await?
                    }
                    POP => {
                        ensure_length::<P>(act.len(), |len| len < 2)?;
                        let idx =
                            if act.len() == 1 {
                                    Some(match unsafe {
                                                    String::from_utf8_lossy(act.next_unchecked())
                                                }.parse::<usize>() {
                                            Ok(int) => int,
                                            Err(_) => return Err(P::RCODE_WRONGTYPE_ERR.into()),
                                        })
                                } else { None };
                        if registry::state_okay() {
                                let maybe_pop =
                                    match listmap.get(listname) {
                                        Ok(lst) =>
                                            lst.map(|list|
                                                    {
                                                        let mut wlock = list.write();
                                                        if let Some(idx) = idx {
                                                                if idx < wlock.len() {
                                                                        Some(wlock.remove(idx))
                                                                    } else { None }
                                                            } else { wlock.pop() }
                                                    }),
                                        Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                    };
                                match maybe_pop {
                                    Some(Some(val)) => {
                                        con.write_mono_length_prefixed_with_tsymbol(&val,
                                                    listmap.get_value_tsymbol()).await?;
                                    }
                                    Some(None) => {
                                        con._write_raw(P::RSTRING_LISTMAP_BAD_INDEX).await?;
                                    }
                                    None => con._write_raw(P::RCODE_NIL).await?,
                                }
                            } else { con._write_raw(P::RCODE_SERVER_ERR).await? }
                    }
                    _ => con._write_raw(P::RCODE_UNKNOWN_ACTION).await?,
                }
                Ok(())
            }
        }
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::LockedVec;
        #[doc = r" Handle an `LSET` query for the list model"]
        #[doc = r" Syntax: `LSET <listname> <values ...>`"]
        pub async fn lset<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len > 0)?;
            let listmap = handle.get_table_with::<P, KVEList>()?;
            let listname = unsafe { act.next_unchecked_bytes() };
            let list = listmap.get_inner_ref();
            if registry::state_okay() {
                    let did =
                        if let Some(entry) = list.fresh_entry(listname.into()) {
                                let v: Vec<Data> = act.map(Data::copy_from_slice).collect();
                                entry.insert(LockedVec::new(v));
                                true
                            } else { false };
                    con._write_raw(P::OKAY_OVW_BLUT[did]).await?
                } else { con._write_raw(P::RCODE_SERVER_ERR).await? }
            Ok(())
        }
    }
    pub mod lskeys {
        use crate::corestore::table::DataModel;
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::*;
        const DEFAULT_COUNT: usize = 10;
        #[doc = r" Run an `LSKEYS` query"]
        pub async fn lskeys<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size < 4)?;
            let (table, count) =
                if act.is_empty() {
                        ({
                                match handle.get_ctable() {
                                    Some(tbl) => tbl,
                                    None => return crate::util::err(P::RSTRING_DEFAULT_UNSET),
                                }
                            }, DEFAULT_COUNT)
                    } else if act.len() == 1 {
                       let nextret = unsafe { act.next_unchecked() };
                       if unsafe {
                                       *(nextret.as_ptr().add(0 as usize))
                                   }.is_ascii_digit() {
                               let count =
                                   if let Ok(cnt) =
                                               String::from_utf8_lossy(nextret).parse::<usize>() {
                                           cnt
                                       } else { return util::err(P::RCODE_WRONGTYPE_ERR); };
                               ({
                                       match handle.get_ctable() {
                                           Some(tbl) => tbl,
                                           None => return crate::util::err(P::RSTRING_DEFAULT_UNSET),
                                       }
                                   }, count)
                           } else {
                              let entity =
                                  {
                                      match crate::queryengine::parser::Entity::from_slice::<P>(&nextret)
                                          {
                                          Ok(e) => e,
                                          Err(e) => return Err(e.into()),
                                      }
                                  };
                              ({
                                      crate::actions::translate_ddl_error::<P,
                                                  ::std::sync::Arc<crate::corestore::table::Table>>(handle.get_table(entity))?
                                  }, DEFAULT_COUNT)
                          }
                   } else {
                       let entity_ret = unsafe { act.next().unsafe_unwrap() };
                       let count_ret = unsafe { act.next().unsafe_unwrap() };
                       let entity =
                           {
                               match crate::queryengine::parser::Entity::from_slice::<P>(&entity_ret)
                                   {
                                   Ok(e) => e,
                                   Err(e) => return Err(e.into()),
                               }
                           };
                       let count =
                           if let Ok(cnt) =
                                       String::from_utf8_lossy(count_ret).parse::<usize>() {
                                   cnt
                               } else { return util::err(P::RCODE_WRONGTYPE_ERR); };
                       ({
                               crate::actions::translate_ddl_error::<P,
                                           ::std::sync::Arc<crate::corestore::table::Table>>(handle.get_table(entity))?
                           }, count)
                   };
            let tsymbol =
                match table.get_model_ref() {
                    DataModel::KV(kv) => kv.get_value_tsymbol(),
                    DataModel::KVExtListmap(kv) => kv.get_value_tsymbol(),
                };
            let items: Vec<Data> =
                match table.get_model_ref() {
                    DataModel::KV(kv) => kv.get_inner_ref().get_keys(count),
                    DataModel::KVExtListmap(kv) =>
                        kv.get_inner_ref().get_keys(count),
                };
            con.write_typed_non_null_array_header(items.len(),
                        tsymbol).await?;
            for key in items {
                con.write_typed_non_null_array_element(&key).await?;
            }
            Ok(())
        }
    }
    pub mod mget {
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER;
        use crate::queryengine::ActionIter;
        use crate::util::compiler;
        #[doc = r" Run an `MGET` query"]
        #[doc = r""]
        pub async fn mget<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size != 0)?;
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            let encoding_is_okay =
                ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
            if compiler::likely(encoding_is_okay) {
                    con.write_typed_array_header(act.len(),
                                kve.get_value_tsymbol()).await?;
                    for key in act {
                        match kve.get_cloned_unchecked(key) {
                            Some(v) => con.write_typed_array_element(&v).await?,
                            None => con.write_typed_array_element_null().await?,
                        }
                    }
                } else { return util::err(P::RCODE_ENCODING_ERROR); }
            Ok(())
        }
    }
    pub mod mpop {
        use crate::corestore;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER;
        use crate::queryengine::ActionIter;
        use crate::util::compiler;
        #[doc = r" Run an MPOP action"]
        pub async fn mpop<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &corestore::Corestore, con: &mut T, act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len != 0)?;
            if registry::state_okay() {
                    let kve = handle.get_table_with::<P, KVEBlob>()?;
                    let encoding_is_okay =
                        ENCODING_LUT_ITER[kve.is_key_encoded()](act.as_ref());
                    if compiler::likely(encoding_is_okay) {
                            con.write_typed_array_header(act.len(),
                                        kve.get_value_tsymbol()).await?;
                            for key in act {
                                match kve.pop_unchecked(key) {
                                    Some(val) => con.write_typed_array_element(&val).await?,
                                    None => con.write_typed_array_element_null().await?,
                                }
                            }
                        } else { return util::err(P::RCODE_ENCODING_ERROR); }
                } else { return util::err(P::RCODE_SERVER_ERR); }
            Ok(())
        }
    }
    pub mod mset {
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER_PAIR;
        use crate::util::compiler;
        #[doc = r" Run an `MSET` query"]
        pub async fn mset<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            let howmany = act.len();
            ensure_length::<P>(howmany, |size| size & 1 == 0 && size != 0)?;
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            let encoding_is_okay =
                ENCODING_LUT_ITER_PAIR[kve.get_encoding_tuple()](&act);
            if compiler::likely(encoding_is_okay) {
                    let done_howmany: Option<usize> =
                        if registry::state_okay() {
                                let mut didmany = 0;
                                while let (Some(key), Some(val)) = (act.next(), act.next())
                                    {
                                    didmany +=
                                        kve.set_unchecked(Data::copy_from_slice(key),
                                                Data::copy_from_slice(val)) as usize;
                                }
                                Some(didmany)
                            } else { None };
                    if let Some(done_howmany) = done_howmany {
                            con.write_usize(done_howmany).await?;
                        } else { return util::err(P::RCODE_SERVER_ERR); }
                } else { return util::err(P::RCODE_ENCODING_ERROR); }
            Ok(())
        }
    }
    pub mod mupdate {
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER_PAIR;
        use crate::util::compiler;
        #[doc = r" Run an `MUPDATE` query"]
        pub async fn mupdate<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            let howmany = act.len();
            ensure_length::<P>(howmany, |size| size & 1 == 0 && size != 0)?;
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            let encoding_is_okay =
                ENCODING_LUT_ITER_PAIR[kve.get_encoding_tuple()](&act);
            let done_howmany: Option<usize>;
            if compiler::likely(encoding_is_okay) {
                    if registry::state_okay() {
                            let mut didmany = 0;
                            while let (Some(key), Some(val)) = (act.next(), act.next())
                                {
                                didmany +=
                                    kve.update_unchecked(Data::copy_from_slice(key),
                                            Data::copy_from_slice(val)) as usize;
                            }
                            done_howmany = Some(didmany);
                        } else { done_howmany = None; }
                    if let Some(done_howmany) = done_howmany {
                            con.write_usize(done_howmany).await?;
                        } else { return util::err(P::RCODE_SERVER_ERR); }
                } else { return util::err(P::RCODE_ENCODING_ERROR); }
            Ok(())
        }
    }
    pub mod pop {
        use crate::dbnet::connection::prelude::*;
        pub async fn pop<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 1)?;
            let key = unsafe { act.next_unchecked() };
            if registry::state_okay() {
                    let kve = handle.get_table_with::<P, KVEBlob>()?;
                    match kve.pop(key) {
                        Ok(Some(val)) =>
                            con.write_mono_length_prefixed_with_tsymbol(&val,
                                        kve.get_value_tsymbol()).await?,
                        Ok(None) => return util::err(P::RCODE_NIL),
                        Err(()) => return util::err(P::RCODE_ENCODING_ERROR),
                    }
                } else { return util::err(P::RCODE_SERVER_ERR); }
            Ok(())
        }
    }
    pub mod set {
        //! # `SET` queries
        //! This module provides functions to work with `SET` queries
        use crate::corestore;
        use crate::dbnet::connection::prelude::*;
        use crate::queryengine::ActionIter;
        use corestore::Data;
        #[doc = r" Run a `SET` query"]
        pub async fn set<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 2)?;
            if registry::state_okay() {
                    let did_we =
                        {
                            let writer = handle.get_table_with::<P, KVEBlob>()?;
                            match unsafe {
                                    writer.set(Data::copy_from_slice(act.next().unsafe_unwrap()),
                                        Data::copy_from_slice(act.next().unsafe_unwrap()))
                                } {
                                Ok(true) => Some(true),
                                Ok(false) => Some(false),
                                Err(()) => None,
                            }
                        };
                    con._write_raw(P::SET_NLUT[did_we]).await?;
                } else { con._write_raw(P::RCODE_SERVER_ERR).await?; }
            Ok(())
        }
    }
    pub mod strong {
        //! # Strong Actions
        //! Strong actions are like "do all" or "fail all" actions, built specifically for
        //! multiple keys. So let's say you used `SSET` instead of `MSET` for setting keys:
        //! what'd be the difference?
        //! In this case, if all the keys are non-existing, which is a requirement for `MSET`,
        //! only then would the keys be set. That is, only if all the keys can be set, will the action
        //! run and return code `0` - otherwise the action won't do anything and return an overwrite error.
        //! There is no point of using _strong actions_ for a single key/value pair, since it will only
        //! slow things down due to the checks performed.
        //! Do note that this isn't the same as the gurantees provided by ACID transactions
        pub use self::{sdel::sdel, sset::sset, supdate::supdate};
        mod sdel {
            use crate::actions::strong::StrongActionResult;
            use crate::dbnet::connection::prelude::*;
            use crate::kvengine::{KVEStandard, SingleEncoder};
            use crate::protocol::iter::DerefUnsafeSlice;
            use crate::util::compiler;
            use core::slice::Iter;
            #[doc = r" Run an `SDEL` query"]
            #[doc = r""]
            #[doc =
            r" This either returns `Okay` if all the keys were `del`eted, or it returns a"]
            #[doc = r" `Nil`, which is code `1`"]
            pub async fn sdel<'a, T: 'a +
                crate::dbnet::connection::ClientConnection<P, Strm>,
                Strm: crate::dbnet::connection::Stream,
                P: crate::protocol::interface::ProtocolSpec>(handle:
                    &crate::corestore::Corestore, con: &mut T,
                act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
                ensure_length::<P>(act.len(), |len| len != 0)?;
                let kve = handle.get_table_with::<P, KVEBlob>()?;
                if registry::state_okay() {
                        let key_encoder = kve.get_key_encoder();
                        let outcome =
                            unsafe {
                                self::snapshot_and_del(kve, key_encoder, act.into_inner())
                            };
                        match outcome {
                            StrongActionResult::Okay =>
                                con._write_raw(P::RCODE_OKAY).await?,
                            StrongActionResult::Nil => {
                                return util::err(P::RCODE_NIL);
                            }
                            StrongActionResult::ServerError =>
                                return util::err(P::RCODE_SERVER_ERR),
                            StrongActionResult::EncodingError => {
                                return util::err(P::RCODE_ENCODING_ERROR);
                            }
                            StrongActionResult::OverwriteError => unsafe {
                                core::hint::unreachable_unchecked()
                            },
                        }
                    } else { return util::err(P::RCODE_SERVER_ERR); }
                Ok(())
            }
            /// Snapshot the current status and then delete maintaining concurrency
            /// guarantees
            pub(super) fn snapshot_and_del<'a, T: 'a +
                DerefUnsafeSlice>(kve: &'a KVEStandard,
                key_encoder: SingleEncoder, act: Iter<'a, T>)
                -> StrongActionResult {
                let mut snapshots = Vec::with_capacity(act.len());
                let mut err_enc = false;
                let iter_stat_ok;
                {
                    iter_stat_ok =
                        act.as_ref().iter().all(|key|
                                {
                                    let key = unsafe { key.deref_slice() };
                                    if compiler::likely(key_encoder(key)) {
                                            if let Some(snap) = kve.take_snapshot_unchecked(key) {
                                                    snapshots.push(snap);
                                                    true
                                                } else { false }
                                        } else { err_enc = true; false }
                                });
                }
                ;
                if compiler::unlikely(err_enc) {
                        return compiler::cold_err(StrongActionResult::EncodingError);
                    }
                if registry::state_okay() {
                        if iter_stat_ok {
                                let kve = kve;
                                let lowtable = kve.get_inner_ref();
                                act.zip(snapshots).for_each(|(key, snapshot)|
                                        {
                                            let key = unsafe { key.deref_slice() };
                                            let _ = lowtable.remove_if(key, |_, val| val.eq(&snapshot));
                                        });
                                StrongActionResult::Okay
                            } else { StrongActionResult::Nil }
                    } else { StrongActionResult::ServerError }
            }
        }
        mod sset {
            use crate::actions::strong::StrongActionResult;
            use crate::corestore::Data;
            use crate::dbnet::connection::prelude::*;
            use crate::kvengine::DoubleEncoder;
            use crate::kvengine::KVEStandard;
            use crate::protocol::iter::DerefUnsafeSlice;
            use crate::util::compiler;
            use core::slice::Iter;
            #[doc = r" Run an `SSET` query"]
            #[doc = r""]
            #[doc =
            r" This either returns `Okay` if all the keys were set, or it returns an"]
            #[doc = r" `Overwrite Error` or code `2`"]
            pub async fn sset<'a, T: 'a +
                crate::dbnet::connection::ClientConnection<P, Strm>,
                Strm: crate::dbnet::connection::Stream,
                P: crate::protocol::interface::ProtocolSpec>(handle:
                    &crate::corestore::Corestore, con: &mut T,
                act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
                let howmany = act.len();
                ensure_length::<P>(howmany,
                        |size| size & 1 == 0 && size != 0)?;
                let kve = handle.get_table_with::<P, KVEBlob>()?;
                if registry::state_okay() {
                        let encoder = kve.get_double_encoder();
                        let outcome =
                            unsafe {
                                self::snapshot_and_insert(kve, encoder, act.into_inner())
                            };
                        match outcome {
                            StrongActionResult::Okay =>
                                con._write_raw(P::RCODE_OKAY).await?,
                            StrongActionResult::OverwriteError =>
                                return util::err(P::RCODE_OVERWRITE_ERR),
                            StrongActionResult::ServerError =>
                                return util::err(P::RCODE_SERVER_ERR),
                            StrongActionResult::EncodingError => {
                                return util::err(P::RCODE_ENCODING_ERROR);
                            }
                            StrongActionResult::Nil => unsafe {
                                core::hint::unreachable_unchecked()
                            },
                        }
                    } else { return util::err(P::RCODE_SERVER_ERR); }
                Ok(())
            }
            /// Take a consistent snapshot of the database at this current point in time
            /// and then mutate the entries, respecting concurrency guarantees
            pub(super) fn snapshot_and_insert<'a, T: 'a +
                DerefUnsafeSlice>(kve: &'a KVEStandard,
                encoder: DoubleEncoder, mut act: Iter<'a, T>)
                -> StrongActionResult {
                let mut enc_err = false;
                let lowtable = kve.get_inner_ref();
                let key_iter_stat_ok;
                {
                    key_iter_stat_ok =
                        act.as_ref().chunks_exact(2).all(|kv|
                                unsafe {
                                    let key = (*(kv.as_ptr().add(0 as usize))).deref_slice();
                                    let value = (*(kv.as_ptr().add(1 as usize))).deref_slice();
                                    if compiler::likely(encoder(key, value)) {
                                            lowtable.get(key).is_none()
                                        } else { enc_err = true; false }
                                });
                }
                ;
                if compiler::unlikely(enc_err) {
                        return compiler::cold_err(StrongActionResult::EncodingError);
                    }
                if registry::state_okay() {
                        if key_iter_stat_ok {
                                let _kve = kve;
                                let lowtable = lowtable;
                                while let (Some(key), Some(value)) =
                                        (act.next(), act.next()) {
                                    unsafe {
                                        if let Some(fresh) =
                                                    lowtable.fresh_entry(Data::copy_from_slice(key.deref_slice()))
                                                {
                                                fresh.insert(Data::copy_from_slice(value.deref_slice()));
                                            }
                                    }
                                }
                                StrongActionResult::Okay
                            } else { StrongActionResult::OverwriteError }
                    } else { StrongActionResult::ServerError }
            }
        }
        mod supdate {
            use crate::actions::strong::StrongActionResult;
            use crate::corestore::Data;
            use crate::dbnet::connection::prelude::*;
            use crate::kvengine::DoubleEncoder;
            use crate::kvengine::KVEStandard;
            use crate::protocol::iter::DerefUnsafeSlice;
            use crate::util::compiler;
            use core::slice::Iter;
            #[doc = r" Run an `SUPDATE` query"]
            #[doc = r""]
            #[doc =
            r" This either returns `Okay` if all the keys were updated, or it returns `Nil`"]
            #[doc = r" or code `1`"]
            pub async fn supdate<'a, T: 'a +
                crate::dbnet::connection::ClientConnection<P, Strm>,
                Strm: crate::dbnet::connection::Stream,
                P: crate::protocol::interface::ProtocolSpec>(handle:
                    &crate::corestore::Corestore, con: &mut T,
                act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
                let howmany = act.len();
                ensure_length::<P>(howmany,
                        |size| size & 1 == 0 && size != 0)?;
                let kve = handle.get_table_with::<P, KVEBlob>()?;
                if registry::state_okay() {
                        let encoder = kve.get_double_encoder();
                        let outcome =
                            unsafe {
                                self::snapshot_and_update(kve, encoder, act.into_inner())
                            };
                        match outcome {
                            StrongActionResult::Okay =>
                                con._write_raw(P::RCODE_OKAY).await?,
                            StrongActionResult::Nil => {
                                return util::err(P::RCODE_NIL);
                            }
                            StrongActionResult::ServerError =>
                                return util::err(P::RCODE_SERVER_ERR),
                            StrongActionResult::EncodingError => {
                                return util::err(P::RCODE_ENCODING_ERROR);
                            }
                            StrongActionResult::OverwriteError => unsafe {
                                core::hint::unreachable_unchecked()
                            },
                        }
                    } else { return util::err(P::RCODE_SERVER_ERR); }
                Ok(())
            }
            /// Take a consistent snapshot of the database at this point in time. Once snapshotting
            /// completes, mutate the entries in place while keeping up with isolation guarantees
            /// `(all_okay, enc_err)`
            pub(super) fn snapshot_and_update<'a, T: 'a +
                DerefUnsafeSlice>(kve: &'a KVEStandard,
                encoder: DoubleEncoder, mut act: Iter<'a, T>)
                -> StrongActionResult {
                let mut enc_err = false;
                let mut snapshots = Vec::with_capacity(act.len());
                let iter_stat_ok;
                {
                    iter_stat_ok =
                        act.as_ref().chunks_exact(2).all(|kv|
                                unsafe {
                                    let key = (*(kv.as_ptr().add(0 as usize))).deref_slice();
                                    let value = (*(kv.as_ptr().add(1 as usize))).deref_slice();
                                    if compiler::likely(encoder(key, value)) {
                                            if let Some(snapshot) = kve.take_snapshot_unchecked(key) {
                                                    snapshots.push(snapshot);
                                                    true
                                                } else { false }
                                        } else { enc_err = true; false }
                                });
                }
                ;
                if compiler::unlikely(enc_err) {
                        return compiler::cold_err(StrongActionResult::EncodingError);
                    }
                if registry::state_okay() {
                        if iter_stat_ok {
                                let kve = kve;
                                let mut snap_cc = snapshots.into_iter();
                                let lowtable = kve.get_inner_ref();
                                while let (Some(key), Some(value), Some(snapshot)) =
                                        (act.next(), act.next(), snap_cc.next()) {
                                    unsafe {
                                        if let Some(mut mutable) =
                                                    lowtable.mut_entry(Data::copy_from_slice(key.deref_slice()))
                                                {
                                                if mutable.value().eq(&snapshot) {
                                                        mutable.insert(Data::copy_from_slice(value.deref_slice()));
                                                    } else { drop(mutable); }
                                            }
                                    }
                                }
                                StrongActionResult::Okay
                            } else { StrongActionResult::Nil }
                    } else { StrongActionResult::ServerError }
            }
        }
        enum StrongActionResult {

            /// Internal server error
            ServerError,

            /// A single value was not found
            Nil,

            /// An overwrite error for a single value
            OverwriteError,

            /// An encoding error occurred
            EncodingError,

            /// Everything worked as expected
            Okay,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for StrongActionResult {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&StrongActionResult::ServerError,) => {
                        ::core::fmt::Formatter::write_str(f, "ServerError")
                    }
                    (&StrongActionResult::Nil,) => {
                        ::core::fmt::Formatter::write_str(f, "Nil")
                    }
                    (&StrongActionResult::OverwriteError,) => {
                        ::core::fmt::Formatter::write_str(f, "OverwriteError")
                    }
                    (&StrongActionResult::EncodingError,) => {
                        ::core::fmt::Formatter::write_str(f, "EncodingError")
                    }
                    (&StrongActionResult::Okay,) => {
                        ::core::fmt::Formatter::write_str(f, "Okay")
                    }
                }
            }
        }
    }
    pub mod update {
        //! # `UPDATE` queries
        //! This module provides functions to work with `UPDATE` queries
        //!
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::*;
        #[doc = r" Run an `UPDATE` query"]
        pub async fn update<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 2)?;
            if registry::state_okay() {
                    let did_we =
                        {
                            let writer = handle.get_table_with::<P, KVEBlob>()?;
                            match unsafe {
                                    writer.update(Data::copy_from_slice(act.next_unchecked()),
                                        Data::copy_from_slice(act.next_unchecked()))
                                } {
                                Ok(true) => Some(true),
                                Ok(false) => Some(false),
                                Err(()) => None,
                            }
                        };
                    con._write_raw(P::UPDATE_NLUT[did_we]).await?;
                } else { return util::err(P::RCODE_SERVER_ERR); }
            Ok(())
        }
    }
    pub mod uset {
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding::ENCODING_LUT_ITER_PAIR;
        use crate::queryengine::ActionIter;
        use crate::util::compiler;
        #[doc = r" Run an `USET` query"]
        #[doc = r""]
        #[doc = r#" This is like "INSERT or UPDATE""#]
        pub async fn uset<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            let howmany = act.len();
            ensure_length::<P>(howmany, |size| size & 1 == 0 && size != 0)?;
            let kve = handle.get_table_with::<P, KVEBlob>()?;
            let encoding_is_okay =
                ENCODING_LUT_ITER_PAIR[kve.get_encoding_tuple()](&act);
            if compiler::likely(encoding_is_okay) {
                    if registry::state_okay() {
                            while let (Some(key), Some(val)) = (act.next(), act.next())
                                {
                                kve.upsert_unchecked(Data::copy_from_slice(key),
                                    Data::copy_from_slice(val));
                            }
                            con.write_usize(howmany / 2).await?;
                        } else { return util::err(P::RCODE_SERVER_ERR); }
                } else { return util::err(P::RCODE_ENCODING_ERROR); }
            Ok(())
        }
    }
    pub mod whereami {
        use crate::dbnet::connection::prelude::*;
        pub async fn whereami<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(store: &Corestore,
            con: &mut T, act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 0)?;
            match store.get_ids() {
                (Some(ks), Some(tbl)) => {
                    con.write_typed_non_null_array_header(2, b'+').await?;
                    con.write_typed_non_null_array_element(ks).await?;
                    con.write_typed_non_null_array_element(tbl).await?;
                }
                (Some(ks), None) => {
                    con.write_typed_non_null_array_header(1, b'+').await?;
                    con.write_typed_non_null_array_element(ks).await?;
                }
                _ => unsafe { core::hint::unreachable_unchecked() },
            }
            Ok(())
        }
    }
    use crate::corestore::memstore::DdlError;
    use crate::protocol::interface::ProtocolSpec;
    use crate::util;
    use std::io::Error as IoError;
    /// A generic result for actions
    pub type ActionResult<T> = Result<T, ActionError>;
    /// Errors that can occur while running actions
    pub enum ActionError {
        ActionError(&'static [u8]),
        IoError(std::io::Error),
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for ActionError {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&ActionError::ActionError(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "ActionError");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&ActionError::IoError(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "IoError");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
            }
        }
    }
    impl PartialEq for ActionError {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (Self::ActionError(a1), Self::ActionError(a2)) => a1 == a2,
                (Self::IoError(ioe1), Self::IoError(ioe2)) =>
                    ioe1.to_string() == ioe2.to_string(),
                _ => false,
            }
        }
    }
    impl From<&'static [u8]> for ActionError {
        fn from(e: &'static [u8]) -> Self { Self::ActionError(e) }
    }
    impl From<IoError> for ActionError {
        fn from(e: IoError) -> Self { Self::IoError(e) }
    }
    #[cold]
    #[inline(never)]
    fn map_ddl_error_to_status<P: ProtocolSpec>(e: DdlError) -> ActionError {
        let r =
            match e {
                DdlError::AlreadyExists => P::RSTRING_ALREADY_EXISTS,
                DdlError::DdlTransactionFailure =>
                    P::RSTRING_DDL_TRANSACTIONAL_FAILURE,
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
    pub fn translate_ddl_error<P: ProtocolSpec, T>(r: Result<T, DdlError>)
        -> Result<T, ActionError> {
        match r {
            Ok(r) => Ok(r),
            Err(e) => Err(map_ddl_error_to_status::<P>(e)),
        }
    }
    pub fn ensure_length<P: ProtocolSpec>(len: usize,
        is_valid: fn(usize) -> bool) -> ActionResult<()> {
        if util::compiler::likely(is_valid(len)) {
                Ok(())
            } else { util::err(P::RCODE_ACTION_ERR) }
    }
    pub fn ensure_boolean_or_aerr<P: ProtocolSpec>(boolean: bool)
        -> ActionResult<()> {
        if util::compiler::likely(boolean) {
                Ok(())
            } else { util::err(P::RCODE_ACTION_ERR) }
    }
    pub fn ensure_cond_or_err(cond: bool, err: &'static [u8])
        -> ActionResult<()> {
        if util::compiler::likely(cond) { Ok(()) } else { util::err(err) }
    }
    pub mod heya {
        //! Respond to `HEYA` queries
        use crate::dbnet::connection::prelude::*;
        #[doc = r" Returns a `HEY!` `Response`"]
        pub async fn heya<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(_handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 0 || len == 1)?;
            if act.len() == 1 {
                    let raw_byte = unsafe { act.next_unchecked() };
                    con.write_mono_length_prefixed_with_tsymbol(raw_byte,
                                b'+').await?;
                } else { con._write_raw(P::ELEMRESP_HEYA).await?; }
            Ok(())
        }
    }
}
mod admin {
    //! Modules for administration of Skytable
    pub mod mksnap {
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding;
        use crate::storage::v1::sengine::SnapshotActionResult;
        use core::str;
        use std::path::{Component, PathBuf};
        #[doc = r" Create a snapshot"]
        #[doc = r""]
        pub async fn mksnap<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle:
                &crate::corestore::Corestore, con: &mut T,
            mut act: ActionIter<'a>) -> crate::actions::ActionResult<()> {
            let engine = handle.get_engine();
            if act.is_empty() {
                    match engine.mksnap(handle.clone_store()).await {
                        SnapshotActionResult::Ok =>
                            con._write_raw(P::RCODE_OKAY).await?,
                        SnapshotActionResult::Failure =>
                            return util::err(P::RCODE_SERVER_ERR),
                        SnapshotActionResult::Disabled =>
                            return util::err(P::RSTRING_SNAPSHOT_DISABLED),
                        SnapshotActionResult::Busy =>
                            return util::err(P::RSTRING_SNAPSHOT_BUSY),
                        _ => unsafe { core::hint::unreachable_unchecked() },
                    }
                } else if act.len() == 1 {
                   let name = unsafe { act.next_unchecked_bytes() };
                   if !encoding::is_utf8(&name) {
                           return util::err(P::RCODE_ENCODING_ERROR);
                       }
                   let st = unsafe { str::from_utf8_unchecked(&name) };
                   let path = PathBuf::from(st);
                   let illegal_snapshot =
                       path.components().filter(|dir|
                                       {
                                           dir == &Component::RootDir || dir == &Component::ParentDir
                                       }).count() != 0;
                   if illegal_snapshot {
                           return util::err(P::RSTRING_SNAPSHOT_ILLEGAL_NAME);
                       }
                   match engine.mkrsnap(name, handle.clone_store()).await {
                       SnapshotActionResult::Ok =>
                           con._write_raw(P::RCODE_OKAY).await?,
                       SnapshotActionResult::Failure =>
                           return util::err(P::RCODE_SERVER_ERR),
                       SnapshotActionResult::Busy =>
                           return util::err(P::RSTRING_SNAPSHOT_BUSY),
                       SnapshotActionResult::AlreadyExists => {
                           return util::err(P::RSTRING_SNAPSHOT_DUPLICATE)
                       }
                       _ => unsafe { core::hint::unreachable_unchecked() },
                   }
               } else { return util::err(P::RCODE_ACTION_ERR); }
            Ok(())
        }
    }
    pub mod sys {
        use crate::{
            corestore::booltable::BoolTable, dbnet::connection::prelude::*,
            storage::v1::interface::DIR_ROOT,
        };
        use ::libsky::VERSION;
        const INFO: &[u8] = b"info";
        const METRIC: &[u8] = b"metric";
        const INFO_PROTOCOL: &[u8] = b"protocol";
        const INFO_PROTOVER: &[u8] = b"protover";
        const INFO_VERSION: &[u8] = b"version";
        const METRIC_HEALTH: &[u8] = b"health";
        const METRIC_STORAGE_USAGE: &[u8] = b"storage";
        const ERR_UNKNOWN_PROPERTY: &[u8] = b"!16\nunknown-property\n";
        const ERR_UNKNOWN_METRIC: &[u8] = b"!14\nunknown-metric\n";
        const HEALTH_TABLE: BoolTable<&str> =
            BoolTable::new("good", "critical");
        pub async fn sys<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(_handle: &Corestore,
            con: &mut T, iter: ActionIter<'_>)
            -> crate::actions::ActionResult<()> {
            let mut iter = iter;
            ensure_boolean_or_aerr::<P>(iter.len() == 2)?;
            match unsafe { iter.next_lowercase_unchecked() }.as_ref() {
                INFO => sys_info(con, &mut iter).await,
                METRIC => sys_metric(con, &mut iter).await,
                _ => util::err(P::RCODE_UNKNOWN_ACTION),
            }
        }
        pub async fn sys_info<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
            iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
            match unsafe { iter.next_lowercase_unchecked() }.as_ref() {
                INFO_PROTOCOL =>
                    con.write_string(P::PROTOCOL_VERSIONSTRING).await?,
                INFO_PROTOVER => con.write_float(P::PROTOCOL_VERSION).await?,
                INFO_VERSION => con.write_string(VERSION).await?,
                _ => return util::err(ERR_UNKNOWN_PROPERTY),
            }
            Ok(())
        }
        pub async fn sys_metric<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
            iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
            match unsafe { iter.next_lowercase_unchecked() }.as_ref() {
                METRIC_HEALTH => {
                    con.write_string(HEALTH_TABLE[registry::state_okay()]).await?
                }
                METRIC_STORAGE_USAGE => {
                    match util::os::dirsize(DIR_ROOT) {
                        Ok(size) => con.write_int64(size).await?,
                        Err(e) => {
                            {
                                let lvl = ::log::Level::Error;
                                if lvl <= ::log::STATIC_MAX_LEVEL &&
                                            lvl <= ::log::max_level() {
                                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Failed to get storage usage with: "],
                                                &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                            &("skyd::admin::sys", "skyd::admin::sys",
                                                    "server/src/admin/sys.rs", 73u32),
                                            ::log::__private_api::Option::None);
                                    }
                            };
                            return util::err(P::RCODE_SERVER_ERR);
                        }
                    }
                }
                _ => return util::err(ERR_UNKNOWN_METRIC),
            }
            Ok(())
        }
    }
}
mod arbiter {
    use crate::{
        auth::AuthProvider,
        config::{ConfigurationSet, SnapshotConfig, SnapshotPref},
        corestore::Corestore, dbnet::{self, Terminator},
        diskstore::flock::FileLock, services,
        storage::v1::sengine::SnapshotEngine,
        util::{
            error::{Error, SkyResult},
            os::TerminationSignal,
        },
    };
    use std::{sync::Arc, thread::sleep};
    use tokio::{
        sync::{broadcast, mpsc::{self, Sender}},
        task::{self, JoinHandle},
        time::Duration,
    };
    const TERMSIG_THRESHOLD: usize = 3;
    /// Start the server waiting for incoming connections or a termsig
    pub async fn run(ConfigurationSet {
            ports, bgsave, snapshot, maxcon, auth, protocol, .. }:
            ConfigurationSet, restore_filepath: Option<String>)
        -> SkyResult<Corestore> {
        let (signal, _) = broadcast::channel(1);
        let engine =
            match &snapshot {
                SnapshotConfig::Enabled(SnapshotPref { atmost, .. }) =>
                    SnapshotEngine::new(*atmost),
                SnapshotConfig::Disabled => SnapshotEngine::new_disabled(),
            };
        let engine = Arc::new(engine);
        services::restore_data(restore_filepath).map_err(|e|
                    Error::ioerror_extra(e, "restoring data from backup"))?;
        let db = Corestore::init_with_snapcfg(engine.clone())?;
        engine.parse_dir()?;
        let auth_provider =
            match auth.origin_key {
                Some(key) => {
                    let authref = db.get_store().setup_auth();
                    AuthProvider::new(authref, Some(key.into_inner()))
                }
                None => AuthProvider::new_disabled(),
            };
        let bgsave_handle =
            tokio::spawn(services::bgsave::bgsave_scheduler(db.clone(),
                    bgsave, Terminator::new(signal.subscribe())));
        let snapshot_handle =
            tokio::spawn(services::snapshot::snapshot_service(engine,
                    db.clone(), snapshot, Terminator::new(signal.subscribe())));
        let termsig =
            TerminationSignal::init().map_err(|e|
                        Error::ioerror_extra(e, "binding to signals"))?;
        let mut server =
            dbnet::connect(ports, protocol, maxcon, db.clone(), auth_provider,
                        signal.clone()).await?;
        {
            #[doc(hidden)]
            mod __tokio_select_util {
                pub(super) enum Out<_0, _1> { _0(_0), _1(_1), Disabled, }
                pub(super) type Mask = u8;
            }
            use ::tokio::macros::support::Future;
            use ::tokio::macros::support::Pin;
            use ::tokio::macros::support::Poll::{Ready, Pending};
            const BRANCHES: u32 = 2;
            let mut disabled: __tokio_select_util::Mask = Default::default();
            if !true {
                    let mask: __tokio_select_util::Mask = 1 << 0;
                    disabled |= mask;
                }
            if !true {
                    let mask: __tokio_select_util::Mask = 1 << 1;
                    disabled |= mask;
                }
            let mut output =
                {
                    let mut futures = (server.run_server(), termsig);
                    ::tokio::macros::support::poll_fn(|cx|
                                {
                                    let mut is_pending = false;
                                    let start =
                                        { ::tokio::macros::support::thread_rng_n(BRANCHES) };
                                    for i in 0..BRANCHES {
                                        let branch;

                                        #[allow(clippy :: modulo_one)]
                                        { branch = (start + i) % BRANCHES; }
                                        match branch
                                            {
                                                #[allow(unreachable_code)]
                                                0 => {
                                                let mask = 1 << branch;
                                                if disabled & mask == mask { continue; }
                                                let (fut, ..) = &mut futures;
                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                let out =
                                                    match Future::poll(fut, cx) {
                                                        Ready(out) => out,
                                                        Pending => { is_pending = true; continue; }
                                                    };
                                                disabled |= mask;

                                                #[allow(unused_variables)]
                                                #[allow(unused_mut)]
                                                match &out { _ => {} _ => continue, }
                                                return Ready(__tokio_select_util::Out::_0(out));
                                            }
                                                #[allow(unreachable_code)]
                                                1 => {
                                                let mask = 1 << branch;
                                                if disabled & mask == mask { continue; }
                                                let (_, fut, ..) = &mut futures;
                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                let out =
                                                    match Future::poll(fut, cx) {
                                                        Ready(out) => out,
                                                        Pending => { is_pending = true; continue; }
                                                    };
                                                disabled |= mask;

                                                #[allow(unused_variables)]
                                                #[allow(unused_mut)]
                                                match &out { _ => {} _ => continue, }
                                                return Ready(__tokio_select_util::Out::_1(out));
                                            }
                                            _ =>
                                                ::core::panicking::unreachable_display(&"reaching this means there probably is an off by one bug"),
                                        }
                                    }
                                    if is_pending {
                                            Pending
                                        } else { Ready(__tokio_select_util::Out::Disabled) }
                                }).await
                };
            match output {
                __tokio_select_util::Out::_0(_) => {}
                __tokio_select_util::Out::_1(_) => {}
                __tokio_select_util::Out::Disabled => {
                    ::std::rt::begin_panic("all branches are disabled and there is no else branch")
                }
                _ =>
                    ::core::panicking::unreachable_display(&"failed to match bind"),
            }
        }
        {
            let lvl = ::log::Level::Info;
            if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                    ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Signalling all workers to shut down"],
                            &[]), lvl,
                        &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                119u32), ::log::__private_api::Option::None);
                }
        };
        drop(signal);
        server.finish_with_termsig().await;
        let _ = snapshot_handle.await;
        let _ = bgsave_handle.await;
        Ok(db)
    }
    fn spawn_task(tx: Sender<bool>, db: Corestore, do_sleep: bool)
        -> JoinHandle<()> {
        task::spawn_blocking(move ||
                {
                    if do_sleep {
                            {
                                let lvl = ::log::Level::Info;
                                if lvl <= ::log::STATIC_MAX_LEVEL &&
                                            lvl <= ::log::max_level() {
                                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Waiting for 10 seconds before retrying ..."],
                                                &[]), lvl,
                                            &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                                    133u32), ::log::__private_api::Option::None);
                                    }
                            };
                            sleep(Duration::from_secs(10));
                        }
                    let ret =
                        match crate::services::bgsave::run_bgsave(&db) {
                            Ok(()) => {
                                {
                                    let lvl = ::log::Level::Info;
                                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                lvl <= ::log::max_level() {
                                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Save before termination successful"],
                                                    &[]), lvl,
                                                &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                                        138u32), ::log::__private_api::Option::None);
                                        }
                                };
                                true
                            }
                            Err(e) => {
                                {
                                    let lvl = ::log::Level::Error;
                                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                lvl <= ::log::max_level() {
                                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Failed to run save on termination: "],
                                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                                        142u32), ::log::__private_api::Option::None);
                                        }
                                };
                                false
                            }
                        };
                    tx.blocking_send(ret).expect("Receiver dropped");
                })
    }
    pub fn finalize_shutdown(corestore: Corestore, pid_file: FileLock) {
        match (&corestore.strong_count(), &1) {
            (left_val, right_val) => {
                if !(*left_val == *right_val) {
                        let kind = ::core::panicking::AssertKind::Eq;
                        ::core::panicking::assert_failed(kind, &*left_val,
                            &*right_val,
                            ::core::option::Option::Some(::core::fmt::Arguments::new_v1(&["Correctness error. finalize_shutdown called before dropping server runtime"],
                                    &[])));
                    }
            }
        };
        let rt =
            tokio::runtime::Builder::new_multi_thread().thread_name("server-final").enable_all().build().unwrap();
        let dbc = corestore.clone();
        let mut okay: bool =
            rt.block_on(async move
                    {
                    let db = dbc;
                    let (tx, mut rx) = mpsc::channel::<bool>(1);
                    spawn_task(tx.clone(), db.clone(), false);
                    let spawn_again =
                        || { spawn_task(tx.clone(), db.clone(), true) };
                    let mut threshold = TERMSIG_THRESHOLD;
                    loop {
                        let termsig =
                            match TerminationSignal::init().map_err(|e| e.to_string()) {
                                Ok(sig) => sig,
                                Err(e) => {
                                    {
                                        let lvl = ::log::Level::Error;
                                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                    lvl <= ::log::max_level() {
                                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Failed to bind to signal with error: "],
                                                        &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                    &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                                            176u32), ::log::__private_api::Option::None);
                                            }
                                    };
                                    crate::exit_error();
                                }
                            };
                        {
                            #[doc(hidden)]
                            mod __tokio_select_util {
                                pub(super) enum Out<_0, _1> { _0(_0), _1(_1), Disabled, }
                                pub(super) type Mask = u8;
                            }
                            use ::tokio::macros::support::Future;
                            use ::tokio::macros::support::Pin;
                            use ::tokio::macros::support::Poll::{Ready, Pending};
                            const BRANCHES: u32 = 2;
                            let mut disabled: __tokio_select_util::Mask =
                                Default::default();
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 0;
                                    disabled |= mask;
                                }
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 1;
                                    disabled |= mask;
                                }
                            let mut output =
                                {
                                    let mut futures = (rx.recv(), termsig);
                                    ::tokio::macros::support::poll_fn(|cx|
                                                {
                                                    let mut is_pending = false;
                                                    let start =
                                                        { ::tokio::macros::support::thread_rng_n(BRANCHES) };
                                                    for i in 0..BRANCHES {
                                                        let branch;

                                                        #[allow(clippy :: modulo_one)]
                                                        { branch = (start + i) % BRANCHES; }
                                                        match branch
                                                            {
                                                                #[allow(unreachable_code)]
                                                                0 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { ret => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_0(out));
                                                            }
                                                                #[allow(unreachable_code)]
                                                                1 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (_, fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { _ => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_1(out));
                                                            }
                                                            _ =>
                                                                ::core::panicking::unreachable_display(&"reaching this means there probably is an off by one bug"),
                                                        }
                                                    }
                                                    if is_pending {
                                                            Pending
                                                        } else { Ready(__tokio_select_util::Out::Disabled) }
                                                }).await
                                };
                            match output {
                                __tokio_select_util::Out::_0(ret) => {
                                    if ret.unwrap() { break true; } else { spawn_again(); }
                                }
                                __tokio_select_util::Out::_1(_) => {
                                    threshold -= 1;
                                    if threshold == 0 {
                                            {
                                                let lvl = ::log::Level::Error;
                                                if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                            lvl <= ::log::max_level() {
                                                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Termination signal received but failed to flush data. Quitting because threshold exceeded"],
                                                                &[]), lvl,
                                                            &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                                                    192u32), ::log::__private_api::Option::None);
                                                    }
                                            };
                                            break false;
                                        } else {
                                           {
                                               let lvl = ::log::Level::Error;
                                               if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                           lvl <= ::log::max_level() {
                                                       ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Termination signal received but failed to flush data. Threshold is at "],
                                                               &[::core::fmt::ArgumentV1::new_display(&threshold)]), lvl,
                                                           &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                                                   195u32), ::log::__private_api::Option::None);
                                                   }
                                           };
                                           continue;
                                       }
                                }
                                __tokio_select_util::Out::Disabled => {
                                    ::std::rt::begin_panic("all branches are disabled and there is no else branch")
                                }
                                _ =>
                                    ::core::panicking::unreachable_display(&"failed to match bind"),
                            }
                        }
                    }
                });
        okay &=
            services::pre_shutdown_cleanup(pid_file,
                Some(corestore.get_store()));
        if okay {
                {
                    let lvl = ::log::Level::Info;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Goodbye :)"],
                                    &[]), lvl,
                                &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                        204u32), ::log::__private_api::Option::None);
                        }
                };
            } else {
               {
                   let lvl = ::log::Level::Error;
                   if lvl <= ::log::STATIC_MAX_LEVEL &&
                               lvl <= ::log::max_level() {
                           ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Didn\'t terminate successfully"],
                                   &[]), lvl,
                               &("skyd::arbiter", "skyd::arbiter", "server/src/arbiter.rs",
                                       206u32), ::log::__private_api::Option::None);
                       }
               };
               crate::exit_error();
           }
    }
}
mod auth {
    mod keys {
        use super::provider::{Authkey, AUTHKEY_SIZE};
        use crate::corestore::array::Array;
        type AuthkeyArray = Array<u8, { AUTHKEY_SIZE }>;
        const RAN_BYTES_SIZE: usize = 40;
        /// Return a "human readable key" and the "authbytes" that can be stored
        /// safely. To do this:
        /// - Generate 64 random bytes
        /// - Encode that into base64. This is the client key
        /// - Hash the key using rcrypt. This is the server key that
        /// will be stored
        pub fn generate_full() -> (String, Authkey) {
            let mut bytes: [u8; RAN_BYTES_SIZE] = [0u8; RAN_BYTES_SIZE];
            openssl::rand::rand_bytes(&mut bytes).unwrap();
            let ret = base64::encode_config(&bytes, base64::BCRYPT);
            let hash = rcrypt::hash(&ret, rcrypt::DEFAULT_COST).unwrap();
            let store_in_db =
                unsafe {
                    let mut array = AuthkeyArray::new();
                    array.extend_from_slice_unchecked(&hash);
                    array.into_array_unchecked()
                };
            (ret, store_in_db)
        }
        /// Verify a "human readable key" against the provided "authbytes"
        pub fn verify_key(input: &[u8], hash: &[u8]) -> Option<bool> {
            rcrypt::verify(input, hash).ok()
        }
    }
    pub mod provider {
        use super::keys;
        use crate::actions::{ActionError, ActionResult};
        use crate::corestore::array::Array;
        use crate::corestore::htable::Coremap;
        use crate::protocol::interface::ProtocolSpec;
        use crate::util::err;
        use std::sync::Arc;
        /// Size of an authn key in bytes
        pub const AUTHKEY_SIZE: usize = 40;
        /// Size of an authn ID in bytes
        pub const AUTHID_SIZE: usize = 40;
        pub mod testsuite_data {
            #![allow(unused)]
            //! Temporary users created by the testsuite in debug mode
            pub const TESTSUITE_ROOT_USER: &str = "root";
            pub const TESTSUITE_TEST_USER: &str = "testuser";
            pub const TESTSUITE_ROOT_TOKEN: &str =
                "XUOdVKhEONnnGwNwT7WeLqbspDgVtKex0/nwFwBSW7XJxioHwpg6H.";
            pub const TESTSUITE_TEST_TOKEN: &str =
                "mpobAB7EY8vnBs70d/..h1VvfinKIeEJgt1rg4wUkwF6aWCvGGR9le";
        }
        const USER_ROOT_ARRAY: [::core::mem::MaybeUninit<u8>; 40] =
            {
                let mut ret = [::core::mem::MaybeUninit::uninit(); 40];
                let mut idx = 0;
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'r');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'o');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'o');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b't');
                ret
            };
        /// The root user
        const USER_ROOT: AuthID =
            unsafe { AuthID::from_const(USER_ROOT_ARRAY, 4) };
        /// An authn ID
        type AuthID = Array<u8, AUTHID_SIZE>;
        /// An authn key
        pub type Authkey = [u8; AUTHKEY_SIZE];
        /// Authmap
        pub type Authmap = Arc<Coremap<AuthID, Authkey>>;
        /// The authn/authz provider
        ///
        pub struct AuthProvider {
            origin: Option<Authkey>,
            /// the current user
            whoami: Option<AuthID>,
            /// a map of users
            authmap: Authmap,
        }
        impl AuthProvider {
            fn _new(authmap: Authmap, whoami: Option<AuthID>,
                origin: Option<Authkey>) -> Self {
                Self { authmap, whoami, origin }
            }
            /// New provider with no origin-key
            pub fn new_disabled() -> Self {
                Self::_new(Default::default(), None, None)
            }
            /// New provider with users from the provided map
            ///
            /// ## Test suite
            /// The testsuite creates users `root` and `testuser`; this **does not** apply to
            /// release mode
            pub fn new(authmap: Arc<Coremap<AuthID, Authkey>>,
                origin: Option<Authkey>) -> Self {
                let slf = Self::_new(authmap, None, origin);

                #[cfg(debug_assertions)]
                {
                    slf.authmap.true_if_insert(AuthID::try_from_slice(testsuite_data::TESTSUITE_ROOT_USER).unwrap(),
                        [172, 143, 117, 169, 158, 156, 33, 106, 139, 107, 20, 106,
                                91, 219, 34, 157, 98, 147, 142, 91, 222, 238, 205, 120, 72,
                                171, 90, 218, 147, 2, 75, 67, 44, 108, 185, 124, 55, 40,
                                156, 252]);
                    slf.authmap.true_if_insert(AuthID::try_from_slice(testsuite_data::TESTSUITE_TEST_USER).unwrap(),
                        [172, 183, 60, 221, 53, 240, 231, 217, 113, 112, 98, 16,
                                109, 62, 235, 95, 184, 107, 130, 139, 43, 197, 40, 31, 176,
                                127, 185, 22, 172, 124, 39, 225, 124, 71, 193, 115, 176,
                                162, 239, 93]);
                }
                slf
            }
            pub const fn is_enabled(&self) -> bool {
                match self.origin { Some(_) => true, _ => false, }
            }
            pub fn claim_root<P: ProtocolSpec>(&mut self, origin_key: &[u8])
                -> ActionResult<String> {
                self.verify_origin::<P>(origin_key)?;
                let (key, store) = keys::generate_full();
                if self.authmap.true_if_insert(USER_ROOT, store) {
                        self.whoami = Some(USER_ROOT);
                        Ok(key)
                    } else { err(P::AUTH_ERROR_ALREADYCLAIMED) }
            }
            fn are_you_root<P: ProtocolSpec>(&self) -> ActionResult<bool> {
                self.ensure_enabled::<P>()?;
                match self.whoami.as_ref().map(|v| v.eq(&USER_ROOT)) {
                    Some(v) => Ok(v),
                    None => err(P::AUTH_CODE_PERMS),
                }
            }
            pub fn claim_user<P: ProtocolSpec>(&self, claimant: &[u8])
                -> ActionResult<String> {
                self.ensure_root::<P>()?;
                self._claim_user::<P>(claimant)
            }
            pub fn _claim_user<P: ProtocolSpec>(&self, claimant: &[u8])
                -> ActionResult<String> {
                let (key, store) = keys::generate_full();
                if self.authmap.true_if_insert(Self::try_auth_id::<P>(claimant)?,
                            store) {
                        Ok(key)
                    } else { err(P::AUTH_ERROR_ALREADYCLAIMED) }
            }
            pub fn login<P: ProtocolSpec>(&mut self, account: &[u8],
                token: &[u8]) -> ActionResult<()> {
                self.ensure_enabled::<P>()?;
                match self.authmap.get(account).map(|token_hash|
                            keys::verify_key(token, token_hash.as_slice())) {
                    Some(Some(true)) => {
                        self.whoami = Some(Self::try_auth_id::<P>(account)?);
                        Ok(())
                    }
                    _ => { err(P::AUTH_CODE_BAD_CREDENTIALS) }
                }
            }
            pub fn regenerate_using_origin<P: ProtocolSpec>(&self,
                origin: &[u8], account: &[u8]) -> ActionResult<String> {
                self.verify_origin::<P>(origin)?;
                self._regenerate::<P>(account)
            }
            pub fn regenerate<P: ProtocolSpec>(&self, account: &[u8])
                -> ActionResult<String> {
                self.ensure_root::<P>()?;
                self._regenerate::<P>(account)
            }
            /// Regenerate the token for the given user. This returns a new token
            fn _regenerate<P: ProtocolSpec>(&self, account: &[u8])
                -> ActionResult<String> {
                let id = Self::try_auth_id::<P>(account)?;
                let (key, store) = keys::generate_full();
                if self.authmap.true_if_update(id, store) {
                        Ok(key)
                    } else { err(P::AUTH_CODE_BAD_CREDENTIALS) }
            }
            fn try_auth_id<P: ProtocolSpec>(authid: &[u8])
                -> ActionResult<AuthID> {
                if authid.is_ascii() && authid.len() <= AUTHID_SIZE {
                        Ok(unsafe { AuthID::from_slice(authid) })
                    } else { err(P::AUTH_ERROR_ILLEGAL_USERNAME) }
            }
            pub fn logout<P: ProtocolSpec>(&mut self) -> ActionResult<()> {
                self.ensure_enabled::<P>()?;
                self.whoami.take().map(|_|
                            ()).ok_or(ActionError::ActionError(P::AUTH_CODE_PERMS))
            }
            fn ensure_enabled<P: ProtocolSpec>(&self) -> ActionResult<()> {
                self.origin.as_ref().map(|_|
                            ()).ok_or(ActionError::ActionError(P::AUTH_ERROR_DISABLED))
            }
            pub fn verify_origin<P: ProtocolSpec>(&self, origin: &[u8])
                -> ActionResult<()> {
                if self.get_origin::<P>()?.eq(origin) {
                        Ok(())
                    } else { err(P::AUTH_CODE_BAD_CREDENTIALS) }
            }
            fn get_origin<P: ProtocolSpec>(&self) -> ActionResult<&Authkey> {
                match self.origin.as_ref() {
                    Some(key) => Ok(key),
                    None => err(P::AUTH_ERROR_DISABLED),
                }
            }
            fn ensure_root<P: ProtocolSpec>(&self) -> ActionResult<()> {
                if self.are_you_root::<P>()? {
                        Ok(())
                    } else { err(P::AUTH_CODE_PERMS) }
            }
            pub fn delete_user<P: ProtocolSpec>(&self, user: &[u8])
                -> ActionResult<()> {
                self.ensure_root::<P>()?;
                if user.eq(&USER_ROOT) {
                        err(P::AUTH_ERROR_FAILED_TO_DELETE_USER)
                    } else if self.authmap.true_if_removed(user) {
                       Ok(())
                   } else { err(P::AUTH_CODE_BAD_CREDENTIALS) }
            }
            /// List all the users
            pub fn collect_usernames<P: ProtocolSpec>(&self)
                -> ActionResult<Vec<String>> {
                self.ensure_root::<P>()?;
                Ok(self.authmap.iter().map(|kv|
                                String::from_utf8_lossy(kv.key()).to_string()).collect())
            }
            /// Return the AuthID of the current user
            pub fn whoami<P: ProtocolSpec>(&self) -> ActionResult<String> {
                self.ensure_enabled::<P>()?;
                self.whoami.as_ref().map(|v|
                            String::from_utf8_lossy(v).to_string()).ok_or(ActionError::ActionError(P::AUTH_CODE_PERMS))
            }
        }
        impl Clone for AuthProvider {
            fn clone(&self) -> Self {
                Self {
                    authmap: self.authmap.clone(),
                    whoami: None,
                    origin: self.origin,
                }
            }
        }
    }
    pub use provider::{AuthProvider, Authmap};
    use crate::dbnet::connection::prelude::*;
    const AUTH_CLAIM: &[u8] = b"claim";
    const AUTH_LOGIN: &[u8] = b"login";
    const AUTH_LOGOUT: &[u8] = b"logout";
    const AUTH_ADDUSER: &[u8] = b"adduser";
    const AUTH_DELUSER: &[u8] = b"deluser";
    const AUTH_RESTORE: &[u8] = b"restore";
    const AUTH_LISTUSER: &[u8] = b"listuser";
    const AUTH_WHOAMI: &[u8] = b"whoami";
    #[doc = r" Handle auth. Should have passed the `auth` token"]
    pub async fn auth<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>, iter: ActionIter<'_>)
        -> crate::actions::ActionResult<()> {
        let mut iter = iter;
        match iter.next_lowercase().unwrap_or_aerr::<P>()?.as_ref() {
            AUTH_LOGIN => self::_auth_login(con, auth, &mut iter).await,
            AUTH_CLAIM => self::_auth_claim(con, auth, &mut iter).await,
            AUTH_ADDUSER => {
                ensure_boolean_or_aerr::<P>(iter.len() == 1)?;
                let username = unsafe { iter.next_unchecked() };
                let key = auth.provider_mut().claim_user::<P>(username)?;
                con.write_string(&key).await?;
                Ok(())
            }
            AUTH_LOGOUT => {
                ensure_boolean_or_aerr::<P>(iter.is_empty())?;
                auth.provider_mut().logout::<P>()?;
                auth.swap_executor_to_anonymous();
                con._write_raw(P::RCODE_OKAY).await?;
                Ok(())
            }
            AUTH_DELUSER => {
                ensure_boolean_or_aerr::<P>(iter.len() == 1)?;
                auth.provider_mut().delete_user::<P>(unsafe {
                            iter.next_unchecked()
                        })?;
                con._write_raw(P::RCODE_OKAY).await?;
                Ok(())
            }
            AUTH_RESTORE => self::auth_restore(con, auth, &mut iter).await,
            AUTH_LISTUSER => self::auth_listuser(con, auth, &mut iter).await,
            AUTH_WHOAMI => self::auth_whoami(con, auth, &mut iter).await,
            _ => util::err(P::RCODE_UNKNOWN_ACTION),
        }
    }
    pub async fn auth_whoami<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
        ensure_boolean_or_aerr::<P>(ActionIter::is_empty(iter))?;
        con.write_string(&auth.provider().whoami::<P>()?).await?;
        Ok(())
    }
    pub async fn auth_listuser<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
        ensure_boolean_or_aerr::<P>(ActionIter::is_empty(iter))?;
        let usernames = auth.provider().collect_usernames::<P>()?;
        con.write_typed_non_null_array_header(usernames.len(), b'+').await?;
        for username in usernames {
            con.write_typed_non_null_array_element(username.as_bytes()).await?;
        }
        Ok(())
    }
    pub async fn auth_restore<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
        let newkey =
            match iter.len() {
                1 => {
                    auth.provider().regenerate::<P>(unsafe {
                                iter.next_unchecked()
                            })?
                }
                2 => {
                    let origin = unsafe { iter.next_unchecked() };
                    let id = unsafe { iter.next_unchecked() };
                    auth.provider().regenerate_using_origin::<P>(origin, id)?
                }
                _ => return util::err(P::RCODE_ACTION_ERR),
            };
        con.write_string(&newkey).await?;
        Ok(())
    }
    pub async fn _auth_claim<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
        ensure_boolean_or_aerr::<P>(iter.len() == 1)?;
        let origin_key = unsafe { iter.next_unchecked() };
        let key = auth.provider_mut().claim_root::<P>(origin_key)?;
        auth.swap_executor_to_authenticated();
        con.write_string(&key).await?;
        Ok(())
    }
    #[doc =
    r" Handle a login operation only. The **`login` token is expected to be present**"]
    pub async fn auth_login_only<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>, iter: ActionIter<'_>)
        -> crate::actions::ActionResult<()> {
        let mut iter = iter;
        match iter.next_lowercase().unwrap_or_aerr::<P>()?.as_ref() {
            AUTH_LOGIN => self::_auth_login(con, auth, &mut iter).await,
            AUTH_CLAIM => self::_auth_claim(con, auth, &mut iter).await,
            AUTH_RESTORE => self::auth_restore(con, auth, &mut iter).await,
            AUTH_WHOAMI => self::auth_whoami(con, auth, &mut iter).await,
            _ => util::err(P::AUTH_CODE_PERMS),
        }
    }
    pub async fn _auth_login<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        iter: &mut ActionIter<'_>) -> crate::actions::ActionResult<()> {
        ensure_boolean_or_aerr::<P>(iter.len() == 2)?;
        let (username, password) =
            unsafe { (iter.next_unchecked(), iter.next_unchecked()) };
        auth.provider_mut().login::<P>(username, password)?;
        auth.swap_executor_to_authenticated();
        con._write_raw(P::RCODE_OKAY).await?;
        Ok(())
    }
}
mod blueql {
    #![allow(dead_code)]
    use {crate::util::Life, core::{marker::PhantomData, slice}};
    pub struct Slice {
        start_ptr: *const u8,
        len: usize,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for Slice {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                Slice { start_ptr: ref __self_0_0, len: ref __self_0_1 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "Slice");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "start_ptr", &&(*__self_0_0));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "len",
                            &&(*__self_0_1));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for Slice {
        #[inline]
        fn clone(&self) -> Slice {
            {
                let _: ::core::clone::AssertParamIsClone<*const u8>;
                let _: ::core::clone::AssertParamIsClone<usize>;
                *self
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::marker::Copy for Slice { }
    unsafe impl Send for Slice {}
    unsafe impl Sync for Slice {}
    impl Slice {
        /// ## Safety
        /// Ensure that `start_ptr` and `len` are valid during construction and use
        #[inline(always)]
        pub const unsafe fn new(start_ptr: *const u8, len: usize) -> Self {
            Slice { start_ptr, len }
        }
        /// ## Safety
        /// Ensure that the slice is valid in this context
        #[inline(always)]
        pub unsafe fn as_slice(&self) -> &[u8] {
            slice::from_raw_parts(self.start_ptr, self.len)
        }
    }
    impl<'a, T> From<T> for Slice where T: AsRef<[u8]> + 'a {
        #[inline(always)]
        fn from(oth: T) -> Self {
            unsafe {
                let oth = oth.as_ref();
                Self::new(oth.as_ptr(), oth.len())
            }
        }
    }
    #[inline(always)]
    fn find_ptr_distance(start: *const u8, stop: *const u8) -> usize {
        stop as usize - start as usize
    }
    pub struct Scanner<'a> {
        cursor: *const u8,
        end_ptr: *const u8,
        _lt: PhantomData<&'a [u8]>,
    }
    impl<'a> Scanner<'a> {
        #[inline(always)]
        const fn new(buf: &[u8]) -> Self {
            unsafe {
                Self {
                    cursor: buf.as_ptr(),
                    end_ptr: buf.as_ptr().add(buf.len()),
                    _lt: PhantomData {},
                }
            }
        }
    }
    impl<'a> Scanner<'a> {
        #[inline(always)]
        pub fn exhausted(&self) -> bool { self.cursor >= self.end_ptr }
        #[inline(always)]
        pub fn not_exhausted(&self) -> bool { self.cursor < self.end_ptr }
    }
    impl<'a> Scanner<'a> {
        #[inline(always)]
        pub fn next_token(&mut self) -> Slice {
            let start_ptr = self.cursor;
            let mut ptr = self.cursor;
            while self.end_ptr > ptr && unsafe { *ptr != b' ' } {
                ptr = unsafe { ptr.add(1) };
            }
            self.cursor = ptr;
            let ptr_is_whitespace =
                unsafe { self.not_exhausted() && *self.cursor == b' ' };
            self.cursor =
                unsafe { self.cursor.add(ptr_is_whitespace as usize) };
            unsafe {
                Slice::new(start_ptr, find_ptr_distance(start_ptr, ptr))
            }
        }
        pub fn parse_into_tokens(buf: &'a [u8]) -> Vec<Life<'a, Slice>> {
            let mut slf = Scanner::new(buf);
            let mut r = Vec::new();
            while slf.not_exhausted() { r.push(Life::new(slf.next_token())); }
            r
        }
    }
    pub enum Token<'a> {
        Create,
        Drop,
        Model,
        Space,
        String,
        Binary,
        Ident(Life<'a, Slice>),
        Number(Life<'a, Slice>),
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a> ::core::fmt::Debug for Token<'a> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&Token::Create,) => {
                    ::core::fmt::Formatter::write_str(f, "Create")
                }
                (&Token::Drop,) => {
                    ::core::fmt::Formatter::write_str(f, "Drop")
                }
                (&Token::Model,) => {
                    ::core::fmt::Formatter::write_str(f, "Model")
                }
                (&Token::Space,) => {
                    ::core::fmt::Formatter::write_str(f, "Space")
                }
                (&Token::String,) => {
                    ::core::fmt::Formatter::write_str(f, "String")
                }
                (&Token::Binary,) => {
                    ::core::fmt::Formatter::write_str(f, "Binary")
                }
                (&Token::Ident(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Ident");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&Token::Number(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Number");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a> ::core::clone::Clone for Token<'a> {
        #[inline]
        fn clone(&self) -> Token<'a> {
            {
                let _: ::core::clone::AssertParamIsClone<Life<'a, Slice>>;
                let _: ::core::clone::AssertParamIsClone<Life<'a, Slice>>;
                *self
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a> ::core::marker::Copy for Token<'a> { }
    impl<'a> ::core::marker::StructuralPartialEq for Token<'a> {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<'a> ::core::cmp::PartialEq for Token<'a> {
        #[inline]
        fn eq(&self, other: &Token<'a>) -> bool {
            {
                let __self_vi =
                    ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi =
                    ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                        match (&*self, &*other) {
                            (&Token::Ident(ref __self_0), &Token::Ident(ref __arg_1_0))
                                => (*__self_0) == (*__arg_1_0),
                            (&Token::Number(ref __self_0),
                                &Token::Number(ref __arg_1_0)) =>
                                (*__self_0) == (*__arg_1_0),
                            _ => true,
                        }
                    } else { false }
            }
        }
        #[inline]
        fn ne(&self, other: &Token<'a>) -> bool {
            {
                let __self_vi =
                    ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi =
                    ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                        match (&*self, &*other) {
                            (&Token::Ident(ref __self_0), &Token::Ident(ref __arg_1_0))
                                => (*__self_0) != (*__arg_1_0),
                            (&Token::Number(ref __self_0),
                                &Token::Number(ref __arg_1_0)) =>
                                (*__self_0) != (*__arg_1_0),
                            _ => false,
                        }
                    } else { true }
            }
        }
    }
}
mod config {
    use crate::auth::provider::Authkey;
    use clap::{load_yaml, App};
    use core::str::FromStr;
    use std::env::VarError;
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr};
    mod cfgcli {
        use super::{ConfigSourceParseResult, Configset, TryFromConfigSource};
        use clap::ArgMatches;
        /// A flag. The flag is said to be set if `self.set` is true and unset if `self.set` is false. However,
        /// if the flag is set, the value of SWITCH determines what value it is set to
        pub(super) struct Flag<const SWITCH : bool> {
            set: bool,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<const SWITCH : bool> ::core::marker::Copy for Flag<SWITCH> { }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<const SWITCH : bool> ::core::clone::Clone for Flag<SWITCH> {
            #[inline]
            fn clone(&self) -> Flag<SWITCH> {
                { let _: ::core::clone::AssertParamIsClone<bool>; *self }
            }
        }
        impl<const SWITCH : bool> Flag<SWITCH> {
            pub(super) fn new(set: bool) -> Self { Self { set } }
        }
        impl<const SWITCH : bool> TryFromConfigSource<bool> for Flag<SWITCH> {
            fn is_present(&self) -> bool { self.set }
            fn mutate_failed(self, target: &mut bool, trip: &mut bool)
                -> bool {
                if self.set { *trip = true; *target = SWITCH; }
                false
            }
            fn try_parse(self) -> ConfigSourceParseResult<bool> {
                if self.set {
                        ConfigSourceParseResult::Okay(SWITCH)
                    } else { ConfigSourceParseResult::Absent }
            }
        }
        pub(super) fn parse_cli_args(matches: ArgMatches) -> Configset {
            let mut defset = Configset::new_cli();
            macro_rules! fcli {
                ($fn : ident, $($source : expr, $key : literal), *) =>
                { defset.$fn($($source, $key,) *) } ;
            }
            defset.protocol_settings(matches.value_of("protover"),
                "--protover");
            defset.server_tcp(matches.value_of("host"), "--host",
                matches.value_of("port"), "--port");
            defset.server_noart(Flag::<true>::new(matches.is_present("noart")),
                "--noart");
            defset.server_mode(matches.value_of("mode"), "--mode");
            defset.server_maxcon(matches.value_of("maxcon"), "--maxcon");
            defset.bgsave_settings(Flag::<false>::new(matches.is_present("nosave")),
                "--nosave", matches.value_of("saveduration"),
                "--saveduration");
            defset.snapshot_settings(matches.value_of("snapevery"),
                "--snapevery", matches.value_of("snapkeep"), "--snapkeep",
                matches.value_of("stop-write-on-fail"),
                "--stop-write-on-fail");
            defset.tls_settings(matches.value_of("sslkey"), "--sslkey",
                matches.value_of("sslchain"), "--sslchain",
                matches.value_of("sslport"), "--sslport",
                Flag::<true>::new(matches.is_present("sslonly")), "--sslonly",
                matches.value_of("tlspass"), "--tlspassin");
            defset.auth_settings(matches.value_of("authkey"),
                "--auth-origin-key");
            defset
        }
    }
    mod cfgenv {
        use super::Configset;
        /// Returns the environment configuration
        pub(super) fn parse_env_config() -> Configset {
            let mut defset = Configset::new_env();
            macro_rules! fenv {
                ($fn : ident, $($field : ident), *) =>
                {
                    defset.$fn($(:: std :: env :: var(stringify! ($field)),
                    stringify! ($field),) *) ;
                } ;
            }
            defset.protocol_settings(::std::env::var("SKY_PROTOCOL_VERSION"),
                "SKY_PROTOCOL_VERSION");
            ;
            defset.server_tcp(::std::env::var("SKY_SYSTEM_HOST"),
                "SKY_SYSTEM_HOST", ::std::env::var("SKY_SYSTEM_PORT"),
                "SKY_SYSTEM_PORT");
            ;
            defset.server_noart(::std::env::var("SKY_SYSTEM_NOART"),
                "SKY_SYSTEM_NOART");
            ;
            defset.server_maxcon(::std::env::var("SKY_SYSTEM_MAXCON"),
                "SKY_SYSTEM_MAXCON");
            ;
            defset.server_mode(::std::env::var("SKY_DEPLOY_MODE"),
                "SKY_DEPLOY_MODE");
            ;
            defset.bgsave_settings(::std::env::var("SKY_BGSAVE_ENABLED"),
                "SKY_BGSAVE_ENABLED", ::std::env::var("SKY_BGSAVE_DURATION"),
                "SKY_BGSAVE_DURATION");
            ;
            defset.snapshot_settings(::std::env::var("SKY_SNAPSHOT_DURATION"),
                "SKY_SNAPSHOT_DURATION", ::std::env::var("SKY_SNAPSHOT_KEEP"),
                "SKY_SNAPSHOT_KEEP", ::std::env::var("SKY_SNAPSHOT_FAILSAFE"),
                "SKY_SNAPSHOT_FAILSAFE");
            ;
            defset.tls_settings(::std::env::var("SKY_TLS_KEY"), "SKY_TLS_KEY",
                ::std::env::var("SKY_TLS_CERT"), "SKY_TLS_CERT",
                ::std::env::var("SKY_TLS_PORT"), "SKY_TLS_PORT",
                ::std::env::var("SKY_TLS_ONLY"), "SKY_TLS_ONLY",
                ::std::env::var("SKY_TLS_PASSIN"), "SKY_TLS_PASSIN");
            ;
            defset.auth_settings(::std::env::var("SKY_AUTH_ORIGIN_KEY"),
                "SKY_AUTH_ORIGIN_KEY");
            ;
            defset
        }
    }
    mod cfgfile {
        use super::{
            AuthSettings, ConfigSourceParseResult, Configset, Modeset,
            OptString, ProtocolVersion, TryFromConfigSource,
        };
        use serde::Deserialize;
        use std::net::IpAddr;
        /// This struct is an _object representation_ used for parsing the TOML file
        pub struct Config {
            /// The `server` key
            pub(super) server: ConfigKeyServer,
            /// The `bgsave` key
            pub(super) bgsave: Option<ConfigKeyBGSAVE>,
            /// The snapshot key
            pub(super) snapshot: Option<ConfigKeySnapshot>,
            /// SSL configuration
            pub(super) ssl: Option<KeySslOpts>,
            /// auth settings
            pub(super) auth: Option<AuthSettings>,
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for Config {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field {
                            __field0,
                            __field1,
                            __field2,
                            __field3,
                            __field4,
                            __ignore,
                        }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    1u64 => _serde::__private::Ok(__Field::__field1),
                                    2u64 => _serde::__private::Ok(__Field::__field2),
                                    3u64 => _serde::__private::Ok(__Field::__field3),
                                    4u64 => _serde::__private::Ok(__Field::__field4),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "server" => _serde::__private::Ok(__Field::__field0),
                                    "bgsave" => _serde::__private::Ok(__Field::__field1),
                                    "snapshot" => _serde::__private::Ok(__Field::__field2),
                                    "ssl" => _serde::__private::Ok(__Field::__field3),
                                    "auth" => _serde::__private::Ok(__Field::__field4),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"server" => _serde::__private::Ok(__Field::__field0),
                                    b"bgsave" => _serde::__private::Ok(__Field::__field1),
                                    b"snapshot" => _serde::__private::Ok(__Field::__field2),
                                    b"ssl" => _serde::__private::Ok(__Field::__field3),
                                    b"auth" => _serde::__private::Ok(__Field::__field4),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<Config>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = Config;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct Config")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<ConfigKeyServer>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct Config with 5 elements"));
                                        }
                                    };
                                let __field1 =
                                    match match _serde::de::SeqAccess::next_element::<Option<ConfigKeyBGSAVE>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(1usize,
                                                        &"struct Config with 5 elements"));
                                        }
                                    };
                                let __field2 =
                                    match match _serde::de::SeqAccess::next_element::<Option<ConfigKeySnapshot>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(2usize,
                                                        &"struct Config with 5 elements"));
                                        }
                                    };
                                let __field3 =
                                    match match _serde::de::SeqAccess::next_element::<Option<KeySslOpts>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(3usize,
                                                        &"struct Config with 5 elements"));
                                        }
                                    };
                                let __field4 =
                                    match match _serde::de::SeqAccess::next_element::<Option<AuthSettings>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(4usize,
                                                        &"struct Config with 5 elements"));
                                        }
                                    };
                                _serde::__private::Ok(Config {
                                        server: __field0,
                                        bgsave: __field1,
                                        snapshot: __field2,
                                        ssl: __field3,
                                        auth: __field4,
                                    })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0:
                                        _serde::__private::Option<ConfigKeyServer> =
                                    _serde::__private::None;
                                let mut __field1:
                                        _serde::__private::Option<Option<ConfigKeyBGSAVE>> =
                                    _serde::__private::None;
                                let mut __field2:
                                        _serde::__private::Option<Option<ConfigKeySnapshot>> =
                                    _serde::__private::None;
                                let mut __field3:
                                        _serde::__private::Option<Option<KeySslOpts>> =
                                    _serde::__private::None;
                                let mut __field4:
                                        _serde::__private::Option<Option<AuthSettings>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("server"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<ConfigKeyServer>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field1 => {
                                            if _serde::__private::Option::is_some(&__field1) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("bgsave"));
                                                }
                                            __field1 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<ConfigKeyBGSAVE>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field2 => {
                                            if _serde::__private::Option::is_some(&__field2) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("snapshot"));
                                                }
                                            __field2 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<ConfigKeySnapshot>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field3 => {
                                            if _serde::__private::Option::is_some(&__field3) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("ssl"));
                                                }
                                            __field3 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<KeySslOpts>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field4 => {
                                            if _serde::__private::Option::is_some(&__field4) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("auth"));
                                                }
                                            __field4 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<AuthSettings>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("server") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field1 =
                                    match __field1 {
                                        _serde::__private::Some(__field1) => __field1,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("bgsave") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field2 =
                                    match __field2 {
                                        _serde::__private::Some(__field2) => __field2,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("snapshot") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field3 =
                                    match __field3 {
                                        _serde::__private::Some(__field3) => __field3,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("ssl") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field4 =
                                    match __field4 {
                                        _serde::__private::Some(__field4) => __field4,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("auth") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(Config {
                                        server: __field0,
                                        bgsave: __field1,
                                        snapshot: __field2,
                                        ssl: __field3,
                                        auth: __field4,
                                    })
                            }
                        }
                        const FIELDS: &'static [&'static str] =
                            &["server", "bgsave", "snapshot", "ssl", "auth"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "Config", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<Config>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Config {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Config {
                        server: ref __self_0_0,
                        bgsave: ref __self_0_1,
                        snapshot: ref __self_0_2,
                        ssl: ref __self_0_3,
                        auth: ref __self_0_4 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Config");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "server", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "bgsave", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "snapshot", &&(*__self_0_2));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "ssl",
                                &&(*__self_0_3));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "auth",
                                &&(*__self_0_4));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for Config {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for Config {
            #[inline]
            fn eq(&self, other: &Config) -> bool {
                match *other {
                    Config {
                        server: ref __self_1_0,
                        bgsave: ref __self_1_1,
                        snapshot: ref __self_1_2,
                        ssl: ref __self_1_3,
                        auth: ref __self_1_4 } =>
                        match *self {
                            Config {
                                server: ref __self_0_0,
                                bgsave: ref __self_0_1,
                                snapshot: ref __self_0_2,
                                ssl: ref __self_0_3,
                                auth: ref __self_0_4 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                                (*__self_0_1) == (*__self_1_1) &&
                                            (*__self_0_2) == (*__self_1_2) &&
                                        (*__self_0_3) == (*__self_1_3) &&
                                    (*__self_0_4) == (*__self_1_4),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &Config) -> bool {
                match *other {
                    Config {
                        server: ref __self_1_0,
                        bgsave: ref __self_1_1,
                        snapshot: ref __self_1_2,
                        ssl: ref __self_1_3,
                        auth: ref __self_1_4 } =>
                        match *self {
                            Config {
                                server: ref __self_0_0,
                                bgsave: ref __self_0_1,
                                snapshot: ref __self_0_2,
                                ssl: ref __self_0_3,
                                auth: ref __self_0_4 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                                (*__self_0_1) != (*__self_1_1) ||
                                            (*__self_0_2) != (*__self_1_2) ||
                                        (*__self_0_3) != (*__self_1_3) ||
                                    (*__self_0_4) != (*__self_1_4),
                        },
                }
            }
        }
        /// This struct represents the `server` key in the TOML file
        pub struct ConfigKeyServer {
            /// The host key is any valid IPv4/IPv6 address
            pub(super) host: IpAddr,
            /// The port key is any valid port
            pub(super) port: u16,
            /// The noart key is an `Option`al boolean value which is set to true
            /// for secure environments to disable terminal artwork
            pub(super) noart: Option<bool>,
            /// The maximum number of clients
            pub(super) maxclient: Option<usize>,
            /// The deployment mode
            pub(super) mode: Option<Modeset>,
            pub(super) protocol: Option<ProtocolVersion>,
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for ConfigKeyServer {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field {
                            __field0,
                            __field1,
                            __field2,
                            __field3,
                            __field4,
                            __field5,
                            __ignore,
                        }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    1u64 => _serde::__private::Ok(__Field::__field1),
                                    2u64 => _serde::__private::Ok(__Field::__field2),
                                    3u64 => _serde::__private::Ok(__Field::__field3),
                                    4u64 => _serde::__private::Ok(__Field::__field4),
                                    5u64 => _serde::__private::Ok(__Field::__field5),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "host" => _serde::__private::Ok(__Field::__field0),
                                    "port" => _serde::__private::Ok(__Field::__field1),
                                    "noart" => _serde::__private::Ok(__Field::__field2),
                                    "maxclient" => _serde::__private::Ok(__Field::__field3),
                                    "mode" => _serde::__private::Ok(__Field::__field4),
                                    "protocol" => _serde::__private::Ok(__Field::__field5),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"host" => _serde::__private::Ok(__Field::__field0),
                                    b"port" => _serde::__private::Ok(__Field::__field1),
                                    b"noart" => _serde::__private::Ok(__Field::__field2),
                                    b"maxclient" => _serde::__private::Ok(__Field::__field3),
                                    b"mode" => _serde::__private::Ok(__Field::__field4),
                                    b"protocol" => _serde::__private::Ok(__Field::__field5),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<ConfigKeyServer>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = ConfigKeyServer;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct ConfigKeyServer")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<IpAddr>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct ConfigKeyServer with 6 elements"));
                                        }
                                    };
                                let __field1 =
                                    match match _serde::de::SeqAccess::next_element::<u16>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(1usize,
                                                        &"struct ConfigKeyServer with 6 elements"));
                                        }
                                    };
                                let __field2 =
                                    match match _serde::de::SeqAccess::next_element::<Option<bool>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(2usize,
                                                        &"struct ConfigKeyServer with 6 elements"));
                                        }
                                    };
                                let __field3 =
                                    match match _serde::de::SeqAccess::next_element::<Option<usize>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(3usize,
                                                        &"struct ConfigKeyServer with 6 elements"));
                                        }
                                    };
                                let __field4 =
                                    match match _serde::de::SeqAccess::next_element::<Option<Modeset>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(4usize,
                                                        &"struct ConfigKeyServer with 6 elements"));
                                        }
                                    };
                                let __field5 =
                                    match match _serde::de::SeqAccess::next_element::<Option<ProtocolVersion>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(5usize,
                                                        &"struct ConfigKeyServer with 6 elements"));
                                        }
                                    };
                                _serde::__private::Ok(ConfigKeyServer {
                                        host: __field0,
                                        port: __field1,
                                        noart: __field2,
                                        maxclient: __field3,
                                        mode: __field4,
                                        protocol: __field5,
                                    })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0: _serde::__private::Option<IpAddr> =
                                    _serde::__private::None;
                                let mut __field1: _serde::__private::Option<u16> =
                                    _serde::__private::None;
                                let mut __field2: _serde::__private::Option<Option<bool>> =
                                    _serde::__private::None;
                                let mut __field3: _serde::__private::Option<Option<usize>> =
                                    _serde::__private::None;
                                let mut __field4:
                                        _serde::__private::Option<Option<Modeset>> =
                                    _serde::__private::None;
                                let mut __field5:
                                        _serde::__private::Option<Option<ProtocolVersion>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("host"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<IpAddr>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field1 => {
                                            if _serde::__private::Option::is_some(&__field1) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("port"));
                                                }
                                            __field1 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<u16>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field2 => {
                                            if _serde::__private::Option::is_some(&__field2) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("noart"));
                                                }
                                            __field2 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<bool>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field3 => {
                                            if _serde::__private::Option::is_some(&__field3) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("maxclient"));
                                                }
                                            __field3 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<usize>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field4 => {
                                            if _serde::__private::Option::is_some(&__field4) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("mode"));
                                                }
                                            __field4 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<Modeset>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field5 => {
                                            if _serde::__private::Option::is_some(&__field5) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("protocol"));
                                                }
                                            __field5 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<ProtocolVersion>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("host") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field1 =
                                    match __field1 {
                                        _serde::__private::Some(__field1) => __field1,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("port") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field2 =
                                    match __field2 {
                                        _serde::__private::Some(__field2) => __field2,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("noart") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field3 =
                                    match __field3 {
                                        _serde::__private::Some(__field3) => __field3,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("maxclient") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field4 =
                                    match __field4 {
                                        _serde::__private::Some(__field4) => __field4,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("mode") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field5 =
                                    match __field5 {
                                        _serde::__private::Some(__field5) => __field5,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("protocol") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(ConfigKeyServer {
                                        host: __field0,
                                        port: __field1,
                                        noart: __field2,
                                        maxclient: __field3,
                                        mode: __field4,
                                        protocol: __field5,
                                    })
                            }
                        }
                        const FIELDS: &'static [&'static str] =
                            &["host", "port", "noart", "maxclient", "mode", "protocol"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "ConfigKeyServer", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<ConfigKeyServer>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ConfigKeyServer {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    ConfigKeyServer {
                        host: ref __self_0_0,
                        port: ref __self_0_1,
                        noart: ref __self_0_2,
                        maxclient: ref __self_0_3,
                        mode: ref __self_0_4,
                        protocol: ref __self_0_5 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "ConfigKeyServer");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "host",
                                &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "port",
                                &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "noart", &&(*__self_0_2));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "maxclient", &&(*__self_0_3));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "mode",
                                &&(*__self_0_4));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "protocol", &&(*__self_0_5));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ConfigKeyServer {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ConfigKeyServer {
            #[inline]
            fn eq(&self, other: &ConfigKeyServer) -> bool {
                match *other {
                    ConfigKeyServer {
                        host: ref __self_1_0,
                        port: ref __self_1_1,
                        noart: ref __self_1_2,
                        maxclient: ref __self_1_3,
                        mode: ref __self_1_4,
                        protocol: ref __self_1_5 } =>
                        match *self {
                            ConfigKeyServer {
                                host: ref __self_0_0,
                                port: ref __self_0_1,
                                noart: ref __self_0_2,
                                maxclient: ref __self_0_3,
                                mode: ref __self_0_4,
                                protocol: ref __self_0_5 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                                    (*__self_0_1) == (*__self_1_1) &&
                                                (*__self_0_2) == (*__self_1_2) &&
                                            (*__self_0_3) == (*__self_1_3) &&
                                        (*__self_0_4) == (*__self_1_4) &&
                                    (*__self_0_5) == (*__self_1_5),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &ConfigKeyServer) -> bool {
                match *other {
                    ConfigKeyServer {
                        host: ref __self_1_0,
                        port: ref __self_1_1,
                        noart: ref __self_1_2,
                        maxclient: ref __self_1_3,
                        mode: ref __self_1_4,
                        protocol: ref __self_1_5 } =>
                        match *self {
                            ConfigKeyServer {
                                host: ref __self_0_0,
                                port: ref __self_0_1,
                                noart: ref __self_0_2,
                                maxclient: ref __self_0_3,
                                mode: ref __self_0_4,
                                protocol: ref __self_0_5 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                                    (*__self_0_1) != (*__self_1_1) ||
                                                (*__self_0_2) != (*__self_1_2) ||
                                            (*__self_0_3) != (*__self_1_3) ||
                                        (*__self_0_4) != (*__self_1_4) ||
                                    (*__self_0_5) != (*__self_1_5),
                        },
                }
            }
        }
        /// The BGSAVE section in the config file
        pub struct ConfigKeyBGSAVE {
            /// Whether BGSAVE is enabled or not
            ///
            /// If this key is missing, then we can assume that BGSAVE is enabled
            pub(super) enabled: Option<bool>,
            /// The duration after which BGSAVE should start
            ///
            /// If this is the only key specified, then it is clear that BGSAVE is enabled
            /// and the duration is `every`
            pub(super) every: Option<u64>,
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for ConfigKeyBGSAVE {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field { __field0, __field1, __ignore, }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    1u64 => _serde::__private::Ok(__Field::__field1),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "enabled" => _serde::__private::Ok(__Field::__field0),
                                    "every" => _serde::__private::Ok(__Field::__field1),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"enabled" => _serde::__private::Ok(__Field::__field0),
                                    b"every" => _serde::__private::Ok(__Field::__field1),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<ConfigKeyBGSAVE>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = ConfigKeyBGSAVE;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct ConfigKeyBGSAVE")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<Option<bool>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct ConfigKeyBGSAVE with 2 elements"));
                                        }
                                    };
                                let __field1 =
                                    match match _serde::de::SeqAccess::next_element::<Option<u64>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(1usize,
                                                        &"struct ConfigKeyBGSAVE with 2 elements"));
                                        }
                                    };
                                _serde::__private::Ok(ConfigKeyBGSAVE {
                                        enabled: __field0,
                                        every: __field1,
                                    })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0: _serde::__private::Option<Option<bool>> =
                                    _serde::__private::None;
                                let mut __field1: _serde::__private::Option<Option<u64>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("enabled"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<bool>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field1 => {
                                            if _serde::__private::Option::is_some(&__field1) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("every"));
                                                }
                                            __field1 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<u64>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("enabled") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field1 =
                                    match __field1 {
                                        _serde::__private::Some(__field1) => __field1,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("every") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(ConfigKeyBGSAVE {
                                        enabled: __field0,
                                        every: __field1,
                                    })
                            }
                        }
                        const FIELDS: &'static [&'static str] =
                            &["enabled", "every"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "ConfigKeyBGSAVE", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<ConfigKeyBGSAVE>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ConfigKeyBGSAVE {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    ConfigKeyBGSAVE {
                        enabled: ref __self_0_0, every: ref __self_0_1 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "ConfigKeyBGSAVE");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "enabled", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "every", &&(*__self_0_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ConfigKeyBGSAVE {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ConfigKeyBGSAVE {
            #[inline]
            fn eq(&self, other: &ConfigKeyBGSAVE) -> bool {
                match *other {
                    ConfigKeyBGSAVE {
                        enabled: ref __self_1_0, every: ref __self_1_1 } =>
                        match *self {
                            ConfigKeyBGSAVE {
                                enabled: ref __self_0_0, every: ref __self_0_1 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                    (*__self_0_1) == (*__self_1_1),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &ConfigKeyBGSAVE) -> bool {
                match *other {
                    ConfigKeyBGSAVE {
                        enabled: ref __self_1_0, every: ref __self_1_1 } =>
                        match *self {
                            ConfigKeyBGSAVE {
                                enabled: ref __self_0_0, every: ref __self_0_1 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                    (*__self_0_1) != (*__self_1_1),
                        },
                }
            }
        }
        /// The snapshot section in the TOML file
        pub struct ConfigKeySnapshot {
            /// After how many seconds should the snapshot be created
            pub(super) every: u64,
            /// The maximum number of snapshots to keep
            ///
            /// If atmost is set to `0`, then all the snapshots will be kept
            pub(super) atmost: usize,
            /// Prevent writes to the database if snapshotting fails
            pub(super) failsafe: Option<bool>,
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for ConfigKeySnapshot {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field { __field0, __field1, __field2, __ignore, }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    1u64 => _serde::__private::Ok(__Field::__field1),
                                    2u64 => _serde::__private::Ok(__Field::__field2),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "every" => _serde::__private::Ok(__Field::__field0),
                                    "atmost" => _serde::__private::Ok(__Field::__field1),
                                    "failsafe" => _serde::__private::Ok(__Field::__field2),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"every" => _serde::__private::Ok(__Field::__field0),
                                    b"atmost" => _serde::__private::Ok(__Field::__field1),
                                    b"failsafe" => _serde::__private::Ok(__Field::__field2),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<ConfigKeySnapshot>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = ConfigKeySnapshot;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct ConfigKeySnapshot")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<u64>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct ConfigKeySnapshot with 3 elements"));
                                        }
                                    };
                                let __field1 =
                                    match match _serde::de::SeqAccess::next_element::<usize>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(1usize,
                                                        &"struct ConfigKeySnapshot with 3 elements"));
                                        }
                                    };
                                let __field2 =
                                    match match _serde::de::SeqAccess::next_element::<Option<bool>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(2usize,
                                                        &"struct ConfigKeySnapshot with 3 elements"));
                                        }
                                    };
                                _serde::__private::Ok(ConfigKeySnapshot {
                                        every: __field0,
                                        atmost: __field1,
                                        failsafe: __field2,
                                    })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0: _serde::__private::Option<u64> =
                                    _serde::__private::None;
                                let mut __field1: _serde::__private::Option<usize> =
                                    _serde::__private::None;
                                let mut __field2: _serde::__private::Option<Option<bool>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("every"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<u64>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field1 => {
                                            if _serde::__private::Option::is_some(&__field1) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("atmost"));
                                                }
                                            __field1 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<usize>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field2 => {
                                            if _serde::__private::Option::is_some(&__field2) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("failsafe"));
                                                }
                                            __field2 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<bool>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("every") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field1 =
                                    match __field1 {
                                        _serde::__private::Some(__field1) => __field1,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("atmost") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field2 =
                                    match __field2 {
                                        _serde::__private::Some(__field2) => __field2,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("failsafe") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(ConfigKeySnapshot {
                                        every: __field0,
                                        atmost: __field1,
                                        failsafe: __field2,
                                    })
                            }
                        }
                        const FIELDS: &'static [&'static str] =
                            &["every", "atmost", "failsafe"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "ConfigKeySnapshot", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<ConfigKeySnapshot>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ConfigKeySnapshot {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    ConfigKeySnapshot {
                        every: ref __self_0_0,
                        atmost: ref __self_0_1,
                        failsafe: ref __self_0_2 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "ConfigKeySnapshot");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "every", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "atmost", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "failsafe", &&(*__self_0_2));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ConfigKeySnapshot {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ConfigKeySnapshot {
            #[inline]
            fn eq(&self, other: &ConfigKeySnapshot) -> bool {
                match *other {
                    ConfigKeySnapshot {
                        every: ref __self_1_0,
                        atmost: ref __self_1_1,
                        failsafe: ref __self_1_2 } =>
                        match *self {
                            ConfigKeySnapshot {
                                every: ref __self_0_0,
                                atmost: ref __self_0_1,
                                failsafe: ref __self_0_2 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                        (*__self_0_1) == (*__self_1_1) &&
                                    (*__self_0_2) == (*__self_1_2),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &ConfigKeySnapshot) -> bool {
                match *other {
                    ConfigKeySnapshot {
                        every: ref __self_1_0,
                        atmost: ref __self_1_1,
                        failsafe: ref __self_1_2 } =>
                        match *self {
                            ConfigKeySnapshot {
                                every: ref __self_0_0,
                                atmost: ref __self_0_1,
                                failsafe: ref __self_0_2 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                        (*__self_0_1) != (*__self_1_1) ||
                                    (*__self_0_2) != (*__self_1_2),
                        },
                }
            }
        }
        pub struct KeySslOpts {
            pub(super) key: String,
            pub(super) chain: String,
            pub(super) port: u16,
            pub(super) only: Option<bool>,
            pub(super) passin: Option<String>,
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for KeySslOpts {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field {
                            __field0,
                            __field1,
                            __field2,
                            __field3,
                            __field4,
                            __ignore,
                        }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    1u64 => _serde::__private::Ok(__Field::__field1),
                                    2u64 => _serde::__private::Ok(__Field::__field2),
                                    3u64 => _serde::__private::Ok(__Field::__field3),
                                    4u64 => _serde::__private::Ok(__Field::__field4),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "key" => _serde::__private::Ok(__Field::__field0),
                                    "chain" => _serde::__private::Ok(__Field::__field1),
                                    "port" => _serde::__private::Ok(__Field::__field2),
                                    "only" => _serde::__private::Ok(__Field::__field3),
                                    "passin" => _serde::__private::Ok(__Field::__field4),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"key" => _serde::__private::Ok(__Field::__field0),
                                    b"chain" => _serde::__private::Ok(__Field::__field1),
                                    b"port" => _serde::__private::Ok(__Field::__field2),
                                    b"only" => _serde::__private::Ok(__Field::__field3),
                                    b"passin" => _serde::__private::Ok(__Field::__field4),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<KeySslOpts>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = KeySslOpts;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct KeySslOpts")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<String>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct KeySslOpts with 5 elements"));
                                        }
                                    };
                                let __field1 =
                                    match match _serde::de::SeqAccess::next_element::<String>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(1usize,
                                                        &"struct KeySslOpts with 5 elements"));
                                        }
                                    };
                                let __field2 =
                                    match match _serde::de::SeqAccess::next_element::<u16>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(2usize,
                                                        &"struct KeySslOpts with 5 elements"));
                                        }
                                    };
                                let __field3 =
                                    match match _serde::de::SeqAccess::next_element::<Option<bool>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(3usize,
                                                        &"struct KeySslOpts with 5 elements"));
                                        }
                                    };
                                let __field4 =
                                    match match _serde::de::SeqAccess::next_element::<Option<String>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(4usize,
                                                        &"struct KeySslOpts with 5 elements"));
                                        }
                                    };
                                _serde::__private::Ok(KeySslOpts {
                                        key: __field0,
                                        chain: __field1,
                                        port: __field2,
                                        only: __field3,
                                        passin: __field4,
                                    })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0: _serde::__private::Option<String> =
                                    _serde::__private::None;
                                let mut __field1: _serde::__private::Option<String> =
                                    _serde::__private::None;
                                let mut __field2: _serde::__private::Option<u16> =
                                    _serde::__private::None;
                                let mut __field3: _serde::__private::Option<Option<bool>> =
                                    _serde::__private::None;
                                let mut __field4:
                                        _serde::__private::Option<Option<String>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("key"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<String>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field1 => {
                                            if _serde::__private::Option::is_some(&__field1) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("chain"));
                                                }
                                            __field1 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<String>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field2 => {
                                            if _serde::__private::Option::is_some(&__field2) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("port"));
                                                }
                                            __field2 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<u16>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field3 => {
                                            if _serde::__private::Option::is_some(&__field3) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("only"));
                                                }
                                            __field3 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<bool>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field4 => {
                                            if _serde::__private::Option::is_some(&__field4) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("passin"));
                                                }
                                            __field4 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<String>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("key") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field1 =
                                    match __field1 {
                                        _serde::__private::Some(__field1) => __field1,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("chain") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field2 =
                                    match __field2 {
                                        _serde::__private::Some(__field2) => __field2,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("port") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field3 =
                                    match __field3 {
                                        _serde::__private::Some(__field3) => __field3,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("only") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field4 =
                                    match __field4 {
                                        _serde::__private::Some(__field4) => __field4,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("passin") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(KeySslOpts {
                                        key: __field0,
                                        chain: __field1,
                                        port: __field2,
                                        only: __field3,
                                        passin: __field4,
                                    })
                            }
                        }
                        const FIELDS: &'static [&'static str] =
                            &["key", "chain", "port", "only", "passin"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "KeySslOpts", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<KeySslOpts>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for KeySslOpts {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    KeySslOpts {
                        key: ref __self_0_0,
                        chain: ref __self_0_1,
                        port: ref __self_0_2,
                        only: ref __self_0_3,
                        passin: ref __self_0_4 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "KeySslOpts");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "key",
                                &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "chain", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "port",
                                &&(*__self_0_2));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "only",
                                &&(*__self_0_3));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "passin", &&(*__self_0_4));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for KeySslOpts {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for KeySslOpts {
            #[inline]
            fn eq(&self, other: &KeySslOpts) -> bool {
                match *other {
                    KeySslOpts {
                        key: ref __self_1_0,
                        chain: ref __self_1_1,
                        port: ref __self_1_2,
                        only: ref __self_1_3,
                        passin: ref __self_1_4 } =>
                        match *self {
                            KeySslOpts {
                                key: ref __self_0_0,
                                chain: ref __self_0_1,
                                port: ref __self_0_2,
                                only: ref __self_0_3,
                                passin: ref __self_0_4 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                                (*__self_0_1) == (*__self_1_1) &&
                                            (*__self_0_2) == (*__self_1_2) &&
                                        (*__self_0_3) == (*__self_1_3) &&
                                    (*__self_0_4) == (*__self_1_4),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &KeySslOpts) -> bool {
                match *other {
                    KeySslOpts {
                        key: ref __self_1_0,
                        chain: ref __self_1_1,
                        port: ref __self_1_2,
                        only: ref __self_1_3,
                        passin: ref __self_1_4 } =>
                        match *self {
                            KeySslOpts {
                                key: ref __self_0_0,
                                chain: ref __self_0_1,
                                port: ref __self_0_2,
                                only: ref __self_0_3,
                                passin: ref __self_0_4 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                                (*__self_0_1) != (*__self_1_1) ||
                                            (*__self_0_2) != (*__self_1_2) ||
                                        (*__self_0_3) != (*__self_1_3) ||
                                    (*__self_0_4) != (*__self_1_4),
                        },
                }
            }
        }
        /// A custom non-null type for config files
        pub struct NonNull<T> {
            val: T,
        }
        impl<T> From<T> for NonNull<T> {
            fn from(val: T) -> Self { Self { val } }
        }
        impl<T> TryFromConfigSource<T> for NonNull<T> {
            fn is_present(&self) -> bool { true }
            fn mutate_failed(self, target: &mut T, trip: &mut bool) -> bool {
                *target = self.val;
                *trip = true;
                false
            }
            fn try_parse(self) -> ConfigSourceParseResult<T> {
                ConfigSourceParseResult::Okay(self.val)
            }
        }
        pub struct Optional<T> {
            base: Option<T>,
        }
        impl<T> Optional<T> {
            pub const fn some(val: T) -> Self { Self { base: Some(val) } }
        }
        impl<T> From<Option<T>> for Optional<T> {
            fn from(base: Option<T>) -> Self { Self { base } }
        }
        impl<T> TryFromConfigSource<T> for Optional<T> {
            fn is_present(&self) -> bool { self.base.is_some() }
            fn mutate_failed(self, target: &mut T, trip: &mut bool) -> bool {
                if let Some(v) = self.base { *trip = true; *target = v; }
                false
            }
            fn try_parse(self) -> ConfigSourceParseResult<T> {
                match self.base {
                    Some(v) => ConfigSourceParseResult::Okay(v),
                    None => ConfigSourceParseResult::Absent,
                }
            }
        }
        type ConfigFile = Config;
        pub fn from_file(file: ConfigFile) -> Configset {
            let mut set = Configset::new_file();
            let ConfigFile { server, bgsave, snapshot, ssl, auth } = file;
            set.server_tcp(Optional::some(server.host), "server.host",
                Optional::some(server.port), "server.port");
            set.protocol_settings(server.protocol, "server.protocol");
            set.server_maxcon(Optional::from(server.maxclient),
                "server.maxcon");
            set.server_noart(Optional::from(server.noart), "server.noart");
            set.server_mode(Optional::from(server.mode), "server.mode");
            if let Some(bgsave) = bgsave {
                    let ConfigKeyBGSAVE { enabled, every } = bgsave;
                    set.bgsave_settings(Optional::from(enabled),
                        "bgsave.enabled", Optional::from(every), "bgsave.every");
                }
            if let Some(snapshot) = snapshot {
                    let ConfigKeySnapshot { every, atmost, failsafe } =
                        snapshot;
                    set.snapshot_settings(NonNull::from(every),
                        "snapshot.every", NonNull::from(atmost), "snapshot.atmost",
                        Optional::from(failsafe), "snapshot.failsafe");
                }
            if let Some(tls) = ssl {
                    let KeySslOpts { key, chain, port, only, passin } = tls;
                    set.tls_settings(NonNull::from(key), "ssl.key",
                        NonNull::from(chain), "ssl.chain", NonNull::from(port),
                        "ssl.port", Optional::from(only), "ssl.only",
                        OptString::from(passin), "ssl.passin");
                }
            if let Some(auth) = auth {
                    let AuthSettings { origin_key } = auth;
                    set.auth_settings(Optional::from(origin_key), "auth.origin")
                }
            set
        }
    }
    mod definitions {
        use super::{feedback::WarningStack, DEFAULT_IPV4, DEFAULT_PORT};
        use crate::config::AuthkeyWrapper;
        use crate::dbnet::MAXIMUM_CONNECTION_LIMIT;
        use core::fmt;
        use core::str::FromStr;
        use serde::{
            de::{self, Deserializer, Visitor},
            Deserialize,
        };
        use std::net::IpAddr;
        /// The BGSAVE configuration
        ///
        /// If BGSAVE is enabled, then the duration (corresponding to `every`) is wrapped in the `Enabled`
        /// variant. Otherwise, the `Disabled` variant is to be used
        pub enum BGSave { Enabled(u64), Disabled, }
        impl ::core::marker::StructuralPartialEq for BGSave {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for BGSave {
            #[inline]
            fn eq(&self, other: &BGSave) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&BGSave::Enabled(ref __self_0),
                                    &BGSave::Enabled(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                _ => true,
                            }
                        } else { false }
                }
            }
            #[inline]
            fn ne(&self, other: &BGSave) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&BGSave::Enabled(ref __self_0),
                                    &BGSave::Enabled(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                _ => false,
                            }
                        } else { true }
                }
            }
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for BGSave {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&BGSave::Enabled(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "Enabled");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&BGSave::Disabled,) => {
                        ::core::fmt::Formatter::write_str(f, "Disabled")
                    }
                }
            }
        }
        impl BGSave {
            /// Create a new BGSAVE configuration with all the fields
            pub const fn new(enabled: bool, every: u64) -> Self {
                if enabled {
                        BGSave::Enabled(every)
                    } else { BGSave::Disabled }
            }
            /// The default BGSAVE configuration
            ///
            /// Defaults:
            /// - `enabled`: true
            /// - `every`: 120
            pub const fn default() -> Self { BGSave::new(true, 120) }
            /// Check if BGSAVE is disabled
            pub const fn is_disabled(&self) -> bool {
                match self { Self::Disabled => true, _ => false, }
            }
        }
        #[repr(u8)]
        pub enum ProtocolVersion { V1, V2, }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ProtocolVersion {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&ProtocolVersion::V1,) => {
                        ::core::fmt::Formatter::write_str(f, "V1")
                    }
                    (&ProtocolVersion::V2,) => {
                        ::core::fmt::Formatter::write_str(f, "V2")
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ProtocolVersion {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ProtocolVersion {
            #[inline]
            fn eq(&self, other: &ProtocolVersion) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) { _ => true, }
                        } else { false }
                }
            }
        }
        impl Default for ProtocolVersion {
            fn default() -> Self { Self::V2 }
        }
        impl ToString for ProtocolVersion {
            fn to_string(&self) -> String {
                match self {
                    Self::V1 => "Skyhash 1.0".to_owned(),
                    Self::V2 => "Skyhash 2.0".to_owned(),
                }
            }
        }
        struct ProtocolVersionVisitor;
        impl<'de> Visitor<'de> for ProtocolVersionVisitor {
            type Value = ProtocolVersion;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(&["a 40 character ASCII string"],
                        &[]))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where
                E: de::Error {
                value.parse().map_err(|_|
                        {
                            E::custom("Invalid value for protocol version. Valid inputs: 1.0, 1.1, 1.2, 2.0")
                        })
            }
        }
        impl<'de> Deserialize<'de> for ProtocolVersion {
            fn deserialize<D>(deserializer: D)
                -> Result<ProtocolVersion, D::Error> where
                D: Deserializer<'de> {
                deserializer.deserialize_str(ProtocolVersionVisitor)
            }
        }
        /// A `ConfigurationSet` which can be used by main::check_args_or_connect() to bind
        /// to a `TcpListener` and show the corresponding terminal output for the given
        /// configuration
        pub struct ConfigurationSet {
            /// If `noart` is set to true, no terminal artwork should be displayed
            pub noart: bool,
            /// The BGSAVE configuration
            pub bgsave: BGSave,
            /// The snapshot configuration
            pub snapshot: SnapshotConfig,
            /// Port configuration
            pub ports: PortConfig,
            /// The maximum number of connections
            pub maxcon: usize,
            /// The deployment mode
            pub mode: Modeset,
            /// The auth settings
            pub auth: AuthSettings,
            /// The protocol version
            pub protocol: ProtocolVersion,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ConfigurationSet {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    ConfigurationSet {
                        noart: ref __self_0_0,
                        bgsave: ref __self_0_1,
                        snapshot: ref __self_0_2,
                        ports: ref __self_0_3,
                        maxcon: ref __self_0_4,
                        mode: ref __self_0_5,
                        auth: ref __self_0_6,
                        protocol: ref __self_0_7 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "ConfigurationSet");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "noart", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "bgsave", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "snapshot", &&(*__self_0_2));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "ports", &&(*__self_0_3));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "maxcon", &&(*__self_0_4));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "mode",
                                &&(*__self_0_5));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "auth",
                                &&(*__self_0_6));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "protocol", &&(*__self_0_7));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ConfigurationSet {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ConfigurationSet {
            #[inline]
            fn eq(&self, other: &ConfigurationSet) -> bool {
                match *other {
                    ConfigurationSet {
                        noart: ref __self_1_0,
                        bgsave: ref __self_1_1,
                        snapshot: ref __self_1_2,
                        ports: ref __self_1_3,
                        maxcon: ref __self_1_4,
                        mode: ref __self_1_5,
                        auth: ref __self_1_6,
                        protocol: ref __self_1_7 } =>
                        match *self {
                            ConfigurationSet {
                                noart: ref __self_0_0,
                                bgsave: ref __self_0_1,
                                snapshot: ref __self_0_2,
                                ports: ref __self_0_3,
                                maxcon: ref __self_0_4,
                                mode: ref __self_0_5,
                                auth: ref __self_0_6,
                                protocol: ref __self_0_7 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                                            (*__self_0_1) == (*__self_1_1) &&
                                                        (*__self_0_2) == (*__self_1_2) &&
                                                    (*__self_0_3) == (*__self_1_3) &&
                                                (*__self_0_4) == (*__self_1_4) &&
                                            (*__self_0_5) == (*__self_1_5) &&
                                        (*__self_0_6) == (*__self_1_6) &&
                                    (*__self_0_7) == (*__self_1_7),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &ConfigurationSet) -> bool {
                match *other {
                    ConfigurationSet {
                        noart: ref __self_1_0,
                        bgsave: ref __self_1_1,
                        snapshot: ref __self_1_2,
                        ports: ref __self_1_3,
                        maxcon: ref __self_1_4,
                        mode: ref __self_1_5,
                        auth: ref __self_1_6,
                        protocol: ref __self_1_7 } =>
                        match *self {
                            ConfigurationSet {
                                noart: ref __self_0_0,
                                bgsave: ref __self_0_1,
                                snapshot: ref __self_0_2,
                                ports: ref __self_0_3,
                                maxcon: ref __self_0_4,
                                mode: ref __self_0_5,
                                auth: ref __self_0_6,
                                protocol: ref __self_0_7 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                                            (*__self_0_1) != (*__self_1_1) ||
                                                        (*__self_0_2) != (*__self_1_2) ||
                                                    (*__self_0_3) != (*__self_1_3) ||
                                                (*__self_0_4) != (*__self_1_4) ||
                                            (*__self_0_5) != (*__self_1_5) ||
                                        (*__self_0_6) != (*__self_1_6) ||
                                    (*__self_0_7) != (*__self_1_7),
                        },
                }
            }
        }
        impl ConfigurationSet {
            #[allow(clippy :: too_many_arguments)]
            pub const fn new(noart: bool, bgsave: BGSave,
                snapshot: SnapshotConfig, ports: PortConfig, maxcon: usize,
                mode: Modeset, auth: AuthSettings, protocol: ProtocolVersion)
                -> Self {
                Self {
                    noart,
                    bgsave,
                    snapshot,
                    ports,
                    maxcon,
                    mode,
                    auth,
                    protocol,
                }
            }
            /// Create a default `ConfigurationSet` with the following setup defaults:
            /// - `host`: 127.0.0.1
            /// - `port` : 2003
            /// - `noart` : false
            /// - `bgsave_enabled` : true
            /// - `bgsave_duration` : 120
            /// - `ssl` : disabled
            pub const fn default() -> Self {
                Self::new(false, BGSave::default(), SnapshotConfig::default(),
                    PortConfig::new_insecure_only(DEFAULT_IPV4, 2003),
                    MAXIMUM_CONNECTION_LIMIT, Modeset::Dev,
                    AuthSettings::default(), ProtocolVersion::V2)
            }
            /// Returns `false` if `noart` is enabled. Otherwise it returns `true`
            pub const fn is_artful(&self) -> bool { !self.noart }
        }
        /// Port configuration
        ///
        /// This enumeration determines whether the ports are:
        /// - `Multi`: This means that the database server will be listening to both
        /// SSL **and** non-SSL requests
        /// - `SecureOnly` : This means that the database server will only accept SSL requests
        /// and will not even activate the non-SSL socket
        /// - `InsecureOnly` : This indicates that the server would only accept non-SSL connections
        /// and will not even activate the SSL socket
        pub enum PortConfig {
            SecureOnly {
                host: IpAddr,
                ssl: SslOpts,
            },
            Multi {
                host: IpAddr,
                port: u16,
                ssl: SslOpts,
            },
            InsecureOnly {
                host: IpAddr,
                port: u16,
            },
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for PortConfig {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&PortConfig::SecureOnly {
                        host: ref __self_0, ssl: ref __self_1 },) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "SecureOnly");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "host",
                                &&(*__self_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "ssl",
                                &&(*__self_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                    (&PortConfig::Multi {
                        host: ref __self_0, port: ref __self_1, ssl: ref __self_2
                        },) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Multi");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "host",
                                &&(*__self_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "port",
                                &&(*__self_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "ssl",
                                &&(*__self_2));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                    (&PortConfig::InsecureOnly {
                        host: ref __self_0, port: ref __self_1 },) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "InsecureOnly");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "host",
                                &&(*__self_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "port",
                                &&(*__self_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for PortConfig {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for PortConfig {
            #[inline]
            fn eq(&self, other: &PortConfig) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&PortConfig::SecureOnly {
                                    host: ref __self_0, ssl: ref __self_1 },
                                    &PortConfig::SecureOnly {
                                    host: ref __arg_1_0, ssl: ref __arg_1_1 }) =>
                                    (*__self_0) == (*__arg_1_0) && (*__self_1) == (*__arg_1_1),
                                (&PortConfig::Multi {
                                    host: ref __self_0, port: ref __self_1, ssl: ref __self_2 },
                                    &PortConfig::Multi {
                                    host: ref __arg_1_0, port: ref __arg_1_1, ssl: ref __arg_1_2
                                    }) =>
                                    (*__self_0) == (*__arg_1_0) && (*__self_1) == (*__arg_1_1)
                                        && (*__self_2) == (*__arg_1_2),
                                (&PortConfig::InsecureOnly {
                                    host: ref __self_0, port: ref __self_1 },
                                    &PortConfig::InsecureOnly {
                                    host: ref __arg_1_0, port: ref __arg_1_1 }) =>
                                    (*__self_0) == (*__arg_1_0) && (*__self_1) == (*__arg_1_1),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { false }
                }
            }
            #[inline]
            fn ne(&self, other: &PortConfig) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&PortConfig::SecureOnly {
                                    host: ref __self_0, ssl: ref __self_1 },
                                    &PortConfig::SecureOnly {
                                    host: ref __arg_1_0, ssl: ref __arg_1_1 }) =>
                                    (*__self_0) != (*__arg_1_0) || (*__self_1) != (*__arg_1_1),
                                (&PortConfig::Multi {
                                    host: ref __self_0, port: ref __self_1, ssl: ref __self_2 },
                                    &PortConfig::Multi {
                                    host: ref __arg_1_0, port: ref __arg_1_1, ssl: ref __arg_1_2
                                    }) =>
                                    (*__self_0) != (*__arg_1_0) || (*__self_1) != (*__arg_1_1)
                                        || (*__self_2) != (*__arg_1_2),
                                (&PortConfig::InsecureOnly {
                                    host: ref __self_0, port: ref __self_1 },
                                    &PortConfig::InsecureOnly {
                                    host: ref __arg_1_0, port: ref __arg_1_1 }) =>
                                    (*__self_0) != (*__arg_1_0) || (*__self_1) != (*__arg_1_1),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { true }
                }
            }
        }
        impl Default for PortConfig {
            fn default() -> PortConfig {
                PortConfig::InsecureOnly {
                    host: DEFAULT_IPV4,
                    port: DEFAULT_PORT,
                }
            }
        }
        impl PortConfig {
            pub const fn new_secure_only(host: IpAddr, ssl: SslOpts) -> Self {
                PortConfig::SecureOnly { host, ssl }
            }
            pub const fn new_insecure_only(host: IpAddr, port: u16) -> Self {
                PortConfig::InsecureOnly { host, port }
            }
            pub fn get_host(&self) -> IpAddr {
                match self {
                    Self::InsecureOnly { host, .. } | Self::SecureOnly { host,
                        .. } | Self::Multi { host, .. } => *host,
                }
            }
            pub fn upgrade_to_tls(&mut self, ssl: SslOpts) {
                match self {
                    Self::InsecureOnly { host, port } => {
                        *self = Self::Multi { host: *host, port: *port, ssl }
                    }
                    Self::SecureOnly { .. } | Self::Multi { .. } => {
                        ::core::panicking::panic_fmt(::core::fmt::Arguments::new_v1(&["Port config is already upgraded to TLS"],
                                &[]))
                    }
                }
            }
            pub const fn insecure_only(&self) -> bool {
                match self { Self::InsecureOnly { .. } => true, _ => false, }
            }
            pub const fn secure_only(&self) -> bool {
                match self { Self::SecureOnly { .. } => true, _ => false, }
            }
            pub fn get_description(&self) -> String {
                match self {
                    Self::Multi { host, port, ssl } => {
                        {
                            let res =
                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1_formatted(&["skyhash://",
                                                    ":", " and skyhash-secure://", ":"],
                                        &[::core::fmt::ArgumentV1::new_display(&ssl.get_port()),
                                                    ::core::fmt::ArgumentV1::new_display(&host),
                                                    ::core::fmt::ArgumentV1::new_display(&port)],
                                        &[::core::fmt::rt::v1::Argument {
                                                        position: 1usize,
                                                        format: ::core::fmt::rt::v1::FormatSpec {
                                                            fill: ' ',
                                                            align: ::core::fmt::rt::v1::Alignment::Unknown,
                                                            flags: 0u32,
                                                            precision: ::core::fmt::rt::v1::Count::Implied,
                                                            width: ::core::fmt::rt::v1::Count::Implied,
                                                        },
                                                    },
                                                    ::core::fmt::rt::v1::Argument {
                                                        position: 2usize,
                                                        format: ::core::fmt::rt::v1::FormatSpec {
                                                            fill: ' ',
                                                            align: ::core::fmt::rt::v1::Alignment::Unknown,
                                                            flags: 0u32,
                                                            precision: ::core::fmt::rt::v1::Count::Implied,
                                                            width: ::core::fmt::rt::v1::Count::Implied,
                                                        },
                                                    },
                                                    ::core::fmt::rt::v1::Argument {
                                                        position: 1usize,
                                                        format: ::core::fmt::rt::v1::FormatSpec {
                                                            fill: ' ',
                                                            align: ::core::fmt::rt::v1::Alignment::Unknown,
                                                            flags: 0u32,
                                                            precision: ::core::fmt::rt::v1::Count::Implied,
                                                            width: ::core::fmt::rt::v1::Count::Implied,
                                                        },
                                                    },
                                                    ::core::fmt::rt::v1::Argument {
                                                        position: 0usize,
                                                        format: ::core::fmt::rt::v1::FormatSpec {
                                                            fill: ' ',
                                                            align: ::core::fmt::rt::v1::Alignment::Unknown,
                                                            flags: 0u32,
                                                            precision: ::core::fmt::rt::v1::Count::Implied,
                                                            width: ::core::fmt::rt::v1::Count::Implied,
                                                        },
                                                    }], unsafe { ::core::fmt::UnsafeArg::new() }));
                            res
                        }
                    }
                    Self::SecureOnly { host, ssl: SslOpts { port, .. } } => {
                        let res =
                            ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["skyhash-secure://",
                                                ":"],
                                    &[::core::fmt::ArgumentV1::new_display(&host),
                                                ::core::fmt::ArgumentV1::new_display(&port)]));
                        res
                    }
                    Self::InsecureOnly { host, port } => {
                        let res =
                            ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["skyhash://",
                                                ":"],
                                    &[::core::fmt::ArgumentV1::new_display(&host),
                                                ::core::fmt::ArgumentV1::new_display(&port)]));
                        res
                    }
                }
            }
        }
        pub struct SslOpts {
            pub key: String,
            pub chain: String,
            pub port: u16,
            pub passfile: Option<String>,
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for SslOpts {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field {
                            __field0,
                            __field1,
                            __field2,
                            __field3,
                            __ignore,
                        }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    1u64 => _serde::__private::Ok(__Field::__field1),
                                    2u64 => _serde::__private::Ok(__Field::__field2),
                                    3u64 => _serde::__private::Ok(__Field::__field3),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "key" => _serde::__private::Ok(__Field::__field0),
                                    "chain" => _serde::__private::Ok(__Field::__field1),
                                    "port" => _serde::__private::Ok(__Field::__field2),
                                    "passfile" => _serde::__private::Ok(__Field::__field3),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"key" => _serde::__private::Ok(__Field::__field0),
                                    b"chain" => _serde::__private::Ok(__Field::__field1),
                                    b"port" => _serde::__private::Ok(__Field::__field2),
                                    b"passfile" => _serde::__private::Ok(__Field::__field3),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<SslOpts>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = SslOpts;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct SslOpts")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<String>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct SslOpts with 4 elements"));
                                        }
                                    };
                                let __field1 =
                                    match match _serde::de::SeqAccess::next_element::<String>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(1usize,
                                                        &"struct SslOpts with 4 elements"));
                                        }
                                    };
                                let __field2 =
                                    match match _serde::de::SeqAccess::next_element::<u16>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(2usize,
                                                        &"struct SslOpts with 4 elements"));
                                        }
                                    };
                                let __field3 =
                                    match match _serde::de::SeqAccess::next_element::<Option<String>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(3usize,
                                                        &"struct SslOpts with 4 elements"));
                                        }
                                    };
                                _serde::__private::Ok(SslOpts {
                                        key: __field0,
                                        chain: __field1,
                                        port: __field2,
                                        passfile: __field3,
                                    })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0: _serde::__private::Option<String> =
                                    _serde::__private::None;
                                let mut __field1: _serde::__private::Option<String> =
                                    _serde::__private::None;
                                let mut __field2: _serde::__private::Option<u16> =
                                    _serde::__private::None;
                                let mut __field3:
                                        _serde::__private::Option<Option<String>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("key"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<String>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field1 => {
                                            if _serde::__private::Option::is_some(&__field1) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("chain"));
                                                }
                                            __field1 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<String>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field2 => {
                                            if _serde::__private::Option::is_some(&__field2) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("port"));
                                                }
                                            __field2 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<u16>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        __Field::__field3 => {
                                            if _serde::__private::Option::is_some(&__field3) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("passfile"));
                                                }
                                            __field3 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<String>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("key") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field1 =
                                    match __field1 {
                                        _serde::__private::Some(__field1) => __field1,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("chain") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field2 =
                                    match __field2 {
                                        _serde::__private::Some(__field2) => __field2,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("port") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                let __field3 =
                                    match __field3 {
                                        _serde::__private::Some(__field3) => __field3,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("passfile") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(SslOpts {
                                        key: __field0,
                                        chain: __field1,
                                        port: __field2,
                                        passfile: __field3,
                                    })
                            }
                        }
                        const FIELDS: &'static [&'static str] =
                            &["key", "chain", "port", "passfile"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "SslOpts", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<SslOpts>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for SslOpts {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    SslOpts {
                        key: ref __self_0_0,
                        chain: ref __self_0_1,
                        port: ref __self_0_2,
                        passfile: ref __self_0_3 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "SslOpts");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "key",
                                &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "chain", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "port",
                                &&(*__self_0_2));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "passfile", &&(*__self_0_3));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for SslOpts {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for SslOpts {
            #[inline]
            fn eq(&self, other: &SslOpts) -> bool {
                match *other {
                    SslOpts {
                        key: ref __self_1_0,
                        chain: ref __self_1_1,
                        port: ref __self_1_2,
                        passfile: ref __self_1_3 } =>
                        match *self {
                            SslOpts {
                                key: ref __self_0_0,
                                chain: ref __self_0_1,
                                port: ref __self_0_2,
                                passfile: ref __self_0_3 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                            (*__self_0_1) == (*__self_1_1) &&
                                        (*__self_0_2) == (*__self_1_2) &&
                                    (*__self_0_3) == (*__self_1_3),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &SslOpts) -> bool {
                match *other {
                    SslOpts {
                        key: ref __self_1_0,
                        chain: ref __self_1_1,
                        port: ref __self_1_2,
                        passfile: ref __self_1_3 } =>
                        match *self {
                            SslOpts {
                                key: ref __self_0_0,
                                chain: ref __self_0_1,
                                port: ref __self_0_2,
                                passfile: ref __self_0_3 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                            (*__self_0_1) != (*__self_1_1) ||
                                        (*__self_0_2) != (*__self_1_2) ||
                                    (*__self_0_3) != (*__self_1_3),
                        },
                }
            }
        }
        impl SslOpts {
            pub const fn new(key: String, chain: String, port: u16,
                passfile: Option<String>) -> Self {
                SslOpts { key, chain, port, passfile }
            }
            pub const fn get_port(&self) -> u16 { self.port }
        }
        /// The snapshot configuration
        ///
        pub struct SnapshotPref {
            /// Capture a snapshot `every` seconds
            pub every: u64,
            /// The maximum numeber of snapshots to be kept
            pub atmost: usize,
            /// Lock writes if snapshotting fails
            pub poison: bool,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for SnapshotPref {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    SnapshotPref {
                        every: ref __self_0_0,
                        atmost: ref __self_0_1,
                        poison: ref __self_0_2 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "SnapshotPref");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "every", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "atmost", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "poison", &&(*__self_0_2));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for SnapshotPref {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for SnapshotPref {
            #[inline]
            fn eq(&self, other: &SnapshotPref) -> bool {
                match *other {
                    SnapshotPref {
                        every: ref __self_1_0,
                        atmost: ref __self_1_1,
                        poison: ref __self_1_2 } =>
                        match *self {
                            SnapshotPref {
                                every: ref __self_0_0,
                                atmost: ref __self_0_1,
                                poison: ref __self_0_2 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                        (*__self_0_1) == (*__self_1_1) &&
                                    (*__self_0_2) == (*__self_1_2),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &SnapshotPref) -> bool {
                match *other {
                    SnapshotPref {
                        every: ref __self_1_0,
                        atmost: ref __self_1_1,
                        poison: ref __self_1_2 } =>
                        match *self {
                            SnapshotPref {
                                every: ref __self_0_0,
                                atmost: ref __self_0_1,
                                poison: ref __self_0_2 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                        (*__self_0_1) != (*__self_1_1) ||
                                    (*__self_0_2) != (*__self_1_2),
                        },
                }
            }
        }
        impl SnapshotPref {
            /// Create a new a new `SnapshotPref` instance
            pub const fn new(every: u64, atmost: usize, poison: bool)
                -> Self {
                SnapshotPref { every, atmost, poison }
            }
            /// Returns `every,almost` as a tuple for pattern matching
            pub const fn decompose(self) -> (u64, usize, bool) {
                (self.every, self.atmost, self.poison)
            }
        }
        /// Snapshotting configuration
        ///
        /// The variant `Enabled` directly carries a `ConfigKeySnapshot` object that
        /// is parsed from the configuration file, The variant `Disabled` is a ZST, and doesn't
        /// hold any data
        pub enum SnapshotConfig {

            /// Snapshotting is enabled: this variant wraps around a `SnapshotPref`
            /// object
            Enabled(SnapshotPref),

            /// Snapshotting is disabled
            Disabled,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for SnapshotConfig {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&SnapshotConfig::Enabled(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "Enabled");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&SnapshotConfig::Disabled,) => {
                        ::core::fmt::Formatter::write_str(f, "Disabled")
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for SnapshotConfig {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for SnapshotConfig {
            #[inline]
            fn eq(&self, other: &SnapshotConfig) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&SnapshotConfig::Enabled(ref __self_0),
                                    &SnapshotConfig::Enabled(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                _ => true,
                            }
                        } else { false }
                }
            }
            #[inline]
            fn ne(&self, other: &SnapshotConfig) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&SnapshotConfig::Enabled(ref __self_0),
                                    &SnapshotConfig::Enabled(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                _ => false,
                            }
                        } else { true }
                }
            }
        }
        impl SnapshotConfig {
            /// Snapshots are disabled by default, so `SnapshotConfig::Disabled` is the
            /// default configuration
            pub const fn default() -> Self { SnapshotConfig::Disabled }
        }
        type RestoreFile = Option<String>;
        /// The type of configuration:
        /// - The default configuration
        /// - A custom supplied configuration
        pub struct ConfigType {
            pub(super) config: ConfigurationSet,
            restore: RestoreFile,
            is_custom: bool,
            warnings: Option<WarningStack>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ConfigType {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    ConfigType {
                        config: ref __self_0_0,
                        restore: ref __self_0_1,
                        is_custom: ref __self_0_2,
                        warnings: ref __self_0_3 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "ConfigType");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "config", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "restore", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "is_custom", &&(*__self_0_2));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "warnings", &&(*__self_0_3));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ConfigType {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ConfigType {
            #[inline]
            fn eq(&self, other: &ConfigType) -> bool {
                match *other {
                    ConfigType {
                        config: ref __self_1_0,
                        restore: ref __self_1_1,
                        is_custom: ref __self_1_2,
                        warnings: ref __self_1_3 } =>
                        match *self {
                            ConfigType {
                                config: ref __self_0_0,
                                restore: ref __self_0_1,
                                is_custom: ref __self_0_2,
                                warnings: ref __self_0_3 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                            (*__self_0_1) == (*__self_1_1) &&
                                        (*__self_0_2) == (*__self_1_2) &&
                                    (*__self_0_3) == (*__self_1_3),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &ConfigType) -> bool {
                match *other {
                    ConfigType {
                        config: ref __self_1_0,
                        restore: ref __self_1_1,
                        is_custom: ref __self_1_2,
                        warnings: ref __self_1_3 } =>
                        match *self {
                            ConfigType {
                                config: ref __self_0_0,
                                restore: ref __self_0_1,
                                is_custom: ref __self_0_2,
                                warnings: ref __self_0_3 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                            (*__self_0_1) != (*__self_1_1) ||
                                        (*__self_0_2) != (*__self_1_2) ||
                                    (*__self_0_3) != (*__self_1_3),
                        },
                }
            }
        }
        impl ConfigType {
            fn _new(config: ConfigurationSet, restore: RestoreFile,
                is_custom: bool, warnings: Option<WarningStack>) -> Self {
                Self { config, restore, is_custom, warnings }
            }
            pub fn print_warnings(&self) {
                if let Some(warnings) = self.warnings.as_ref() {
                        warnings.print_warnings()
                    }
            }
            pub fn finish(self) -> (ConfigurationSet, Option<String>) {
                (self.config, self.restore)
            }
            pub fn is_custom(&self) -> bool { self.is_custom }
            pub fn is_artful(&self) -> bool { self.config.is_artful() }
            pub fn new_custom(config: ConfigurationSet, restore: RestoreFile,
                warnings: WarningStack) -> Self {
                Self::_new(config, restore, true, Some(warnings))
            }
            pub fn new_default(restore: RestoreFile) -> Self {
                Self::_new(ConfigurationSet::default(), restore, false, None)
            }
            /// Check if the current deploy mode is prod
            pub const fn is_prod_mode(&self) -> bool {
                match self.config.mode { Modeset::Prod => true, _ => false, }
            }
            pub fn wpush(&mut self, w: impl ToString) {
                match self.warnings.as_mut() {
                    Some(stack) => stack.push(w),
                    None => {
                        self.warnings =
                            {
                                let mut wstack = WarningStack::new("");
                                wstack.push(w);
                                Some(wstack)
                            };
                    }
                }
            }
        }
        pub enum Modeset { Dev, Prod, }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Modeset {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&Modeset::Dev,) => {
                        ::core::fmt::Formatter::write_str(f, "Dev")
                    }
                    (&Modeset::Prod,) => {
                        ::core::fmt::Formatter::write_str(f, "Prod")
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for Modeset {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for Modeset {
            #[inline]
            fn eq(&self, other: &Modeset) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) { _ => true, }
                        } else { false }
                }
            }
        }
        impl FromStr for Modeset {
            type Err = ();
            fn from_str(st: &str) -> Result<Modeset, Self::Err> {
                match st {
                    "dev" => Ok(Modeset::Dev),
                    "prod" => Ok(Modeset::Prod),
                    _ => Err(()),
                }
            }
        }
        struct ModesetVisitor;
        impl<'de> Visitor<'de> for ModesetVisitor {
            type Value = Modeset;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(&["Expecting a string with the deployment mode"],
                        &[]))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where
                E: de::Error {
                match value {
                    "dev" => Ok(Modeset::Dev),
                    "prod" => Ok(Modeset::Prod),
                    _ =>
                        return Err(E::custom({
                                        let res =
                                            ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Bad value `",
                                                                "` for modeset"],
                                                    &[::core::fmt::ArgumentV1::new_display(&value)]));
                                        res
                                    })),
                }
            }
        }
        impl<'de> Deserialize<'de> for Modeset {
            fn deserialize<D>(deserializer: D) -> Result<Modeset, D::Error>
                where D: Deserializer<'de> {
                deserializer.deserialize_str(ModesetVisitor)
            }
        }
        pub struct AuthSettings {
            pub origin_key: Option<AuthkeyWrapper>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for AuthSettings {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    AuthSettings { origin_key: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "AuthSettings");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "origin_key", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for AuthSettings {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for AuthSettings {
            #[inline]
            fn eq(&self, other: &AuthSettings) -> bool {
                match *other {
                    AuthSettings { origin_key: ref __self_1_0 } =>
                        match *self {
                            AuthSettings { origin_key: ref __self_0_0 } =>
                                (*__self_0_0) == (*__self_1_0),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &AuthSettings) -> bool {
                match *other {
                    AuthSettings { origin_key: ref __self_1_0 } =>
                        match *self {
                            AuthSettings { origin_key: ref __self_0_0 } =>
                                (*__self_0_0) != (*__self_1_0),
                        },
                }
            }
        }
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes,
        unused_qualifications)]
        const _: () =
            {
                #[allow(unused_extern_crates, clippy :: useless_attribute)]
                extern crate serde as _serde;
                #[allow(unused_macros)]
                macro_rules! try {
                    ($__expr : expr) =>
                    {
                        match $__expr
                        {
                            _serde :: __private :: Ok(__val) => __val, _serde ::
                            __private :: Err(__err) =>
                            { return _serde :: __private :: Err(__err) ; }
                        }
                    }
                }
                #[automatically_derived]
                impl<'de> _serde::Deserialize<'de> for AuthSettings {
                    fn deserialize<__D>(__deserializer: __D)
                        -> _serde::__private::Result<Self, __D::Error> where
                        __D: _serde::Deserializer<'de> {
                        #[allow(non_camel_case_types)]
                        enum __Field { __field0, __ignore, }
                        struct __FieldVisitor;
                        impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                            type Value = __Field;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "field identifier")
                            }
                            fn visit_u64<__E>(self, __value: u64)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    0u64 => _serde::__private::Ok(__Field::__field0),
                                    _ => _serde::__private::Ok(__Field::__ignore),
                                }
                            }
                            fn visit_str<__E>(self, __value: &str)
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    "origin_key" => _serde::__private::Ok(__Field::__field0),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                            fn visit_bytes<__E>(self, __value: &[u8])
                                -> _serde::__private::Result<Self::Value, __E> where
                                __E: _serde::de::Error {
                                match __value {
                                    b"origin_key" => _serde::__private::Ok(__Field::__field0),
                                    _ => { _serde::__private::Ok(__Field::__ignore) }
                                }
                            }
                        }
                        impl<'de> _serde::Deserialize<'de> for __Field {
                            #[inline]
                            fn deserialize<__D>(__deserializer: __D)
                                -> _serde::__private::Result<Self, __D::Error> where
                                __D: _serde::Deserializer<'de> {
                                _serde::Deserializer::deserialize_identifier(__deserializer,
                                    __FieldVisitor)
                            }
                        }
                        struct __Visitor<'de> {
                            marker: _serde::__private::PhantomData<AuthSettings>,
                            lifetime: _serde::__private::PhantomData<&'de ()>,
                        }
                        impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                            type Value = AuthSettings;
                            fn expecting(&self,
                                __formatter: &mut _serde::__private::Formatter)
                                -> _serde::__private::fmt::Result {
                                _serde::__private::Formatter::write_str(__formatter,
                                    "struct AuthSettings")
                            }
                            #[inline]
                            fn visit_seq<__A>(self, mut __seq: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::SeqAccess<'de> {
                                let __field0 =
                                    match match _serde::de::SeqAccess::next_element::<Option<AuthkeyWrapper>>(&mut __seq)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                        _serde::__private::Some(__value) => __value,
                                        _serde::__private::None => {
                                            return _serde::__private::Err(_serde::de::Error::invalid_length(0usize,
                                                        &"struct AuthSettings with 1 element"));
                                        }
                                    };
                                _serde::__private::Ok(AuthSettings { origin_key: __field0 })
                            }
                            #[inline]
                            fn visit_map<__A>(self, mut __map: __A)
                                -> _serde::__private::Result<Self::Value, __A::Error> where
                                __A: _serde::de::MapAccess<'de> {
                                let mut __field0:
                                        _serde::__private::Option<Option<AuthkeyWrapper>> =
                                    _serde::__private::None;
                                while let _serde::__private::Some(__key) =
                                        match _serde::de::MapAccess::next_key::<__Field>(&mut __map)
                                            {
                                            _serde::__private::Ok(__val) => __val,
                                            _serde::__private::Err(__err) => {
                                                return _serde::__private::Err(__err);
                                            }
                                        } {
                                    match __key {
                                        __Field::__field0 => {
                                            if _serde::__private::Option::is_some(&__field0) {
                                                    return _serde::__private::Err(<__A::Error as
                                                                    _serde::de::Error>::duplicate_field("origin_key"));
                                                }
                                            __field0 =
                                                _serde::__private::Some(match _serde::de::MapAccess::next_value::<Option<AuthkeyWrapper>>(&mut __map)
                                                        {
                                                        _serde::__private::Ok(__val) => __val,
                                                        _serde::__private::Err(__err) => {
                                                            return _serde::__private::Err(__err);
                                                        }
                                                    });
                                        }
                                        _ => {
                                            let _ =
                                                match _serde::de::MapAccess::next_value::<_serde::de::IgnoredAny>(&mut __map)
                                                    {
                                                    _serde::__private::Ok(__val) => __val,
                                                    _serde::__private::Err(__err) => {
                                                        return _serde::__private::Err(__err);
                                                    }
                                                };
                                        }
                                    }
                                }
                                let __field0 =
                                    match __field0 {
                                        _serde::__private::Some(__field0) => __field0,
                                        _serde::__private::None =>
                                            match _serde::__private::de::missing_field("origin_key") {
                                                _serde::__private::Ok(__val) => __val,
                                                _serde::__private::Err(__err) => {
                                                    return _serde::__private::Err(__err);
                                                }
                                            },
                                    };
                                _serde::__private::Ok(AuthSettings { origin_key: __field0 })
                            }
                        }
                        const FIELDS: &'static [&'static str] = &["origin_key"];
                        _serde::Deserializer::deserialize_struct(__deserializer,
                            "AuthSettings", FIELDS,
                            __Visitor {
                                marker: _serde::__private::PhantomData::<AuthSettings>,
                                lifetime: _serde::__private::PhantomData,
                            })
                    }
                }
            };
        impl AuthSettings {
            pub const fn default() -> Self { Self { origin_key: None } }
        }
        struct AuthSettingsVisitor;
        impl<'de> Visitor<'de> for AuthSettingsVisitor {
            type Value = AuthkeyWrapper;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(&["a 40 character ASCII string"],
                        &[]))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> where
                E: de::Error {
                AuthkeyWrapper::try_new(value).ok_or_else(||
                        {
                            E::custom("Invalid value for authkey. must be 40 ASCII characters with nonzero first char")
                        })
            }
        }
        impl<'de> Deserialize<'de> for AuthkeyWrapper {
            fn deserialize<D>(deserializer: D)
                -> Result<AuthkeyWrapper, D::Error> where
                D: Deserializer<'de> {
                deserializer.deserialize_str(AuthSettingsVisitor)
            }
        }
    }
    mod feedback {
        use toml::de::Error as TomlError;
        use core::fmt;
        use core::ops;
        use std::io::Error as IoError;
        use super::{ConfigurationSet, SnapshotConfig, SnapshotPref};
        #[cfg(unix)]
        use crate::util::os::ResourceLimit;
        const EMSG_PROD: &str = "Production mode";
        const TAB: &str = "    ";
        pub struct FeedbackStack {
            stack: Vec<String>,
            feedback_type: &'static str,
            feedback_source: &'static str,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for FeedbackStack {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    FeedbackStack {
                        stack: ref __self_0_0,
                        feedback_type: ref __self_0_1,
                        feedback_source: ref __self_0_2 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "FeedbackStack");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "stack", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "feedback_type", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "feedback_source", &&(*__self_0_2));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for FeedbackStack {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for FeedbackStack {
            #[inline]
            fn eq(&self, other: &FeedbackStack) -> bool {
                match *other {
                    FeedbackStack {
                        stack: ref __self_1_0,
                        feedback_type: ref __self_1_1,
                        feedback_source: ref __self_1_2 } =>
                        match *self {
                            FeedbackStack {
                                stack: ref __self_0_0,
                                feedback_type: ref __self_0_1,
                                feedback_source: ref __self_0_2 } =>
                                (*__self_0_0) == (*__self_1_0) &&
                                        (*__self_0_1) == (*__self_1_1) &&
                                    (*__self_0_2) == (*__self_1_2),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &FeedbackStack) -> bool {
                match *other {
                    FeedbackStack {
                        stack: ref __self_1_0,
                        feedback_type: ref __self_1_1,
                        feedback_source: ref __self_1_2 } =>
                        match *self {
                            FeedbackStack {
                                stack: ref __self_0_0,
                                feedback_type: ref __self_0_1,
                                feedback_source: ref __self_0_2 } =>
                                (*__self_0_0) != (*__self_1_0) ||
                                        (*__self_0_1) != (*__self_1_1) ||
                                    (*__self_0_2) != (*__self_1_2),
                        },
                }
            }
        }
        impl FeedbackStack {
            fn new(feedback_source: &'static str, feedback_type: &'static str)
                -> Self {
                Self { stack: Vec::new(), feedback_type, feedback_source }
            }
            pub fn source(&self) -> &'static str { self.feedback_source }
            pub fn push(&mut self, f: impl ToString) {
                self.stack.push(f.to_string())
            }
        }
        impl fmt::Display for FeedbackStack {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if !self.is_empty() {
                        if self.len() == 1 {
                                f.write_fmt(::core::fmt::Arguments::new_v1(&["", " ", ": "],
                                            &[::core::fmt::ArgumentV1::new_display(&self.feedback_source),
                                                        ::core::fmt::ArgumentV1::new_display(&self.feedback_type),
                                                        ::core::fmt::ArgumentV1::new_display(&self.stack[0])]))?;
                            } else {
                               f.write_fmt(::core::fmt::Arguments::new_v1(&["", " ", ":"],
                                           &[::core::fmt::ArgumentV1::new_display(&self.feedback_source),
                                                       ::core::fmt::ArgumentV1::new_display(&self.feedback_type)]))?;
                               for err in self.stack.iter() {
                                   f.write_fmt(::core::fmt::Arguments::new_v1(&["\n", "- "],
                                               &[::core::fmt::ArgumentV1::new_display(&TAB),
                                                           ::core::fmt::ArgumentV1::new_display(&err)]))?;
                               }
                           }
                    }
                Ok(())
            }
        }
        impl ops::Deref for FeedbackStack {
            type Target = Vec<String>;
            fn deref(&self) -> &Self::Target { &self.stack }
        }
        impl ops::DerefMut for FeedbackStack {
            fn deref_mut(&mut self) -> &mut Self::Target { &mut self.stack }
        }
        pub struct ErrorStack {
            feedback: FeedbackStack,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ErrorStack {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    ErrorStack { feedback: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "ErrorStack");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "feedback", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for ErrorStack {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for ErrorStack {
            #[inline]
            fn eq(&self, other: &ErrorStack) -> bool {
                match *other {
                    ErrorStack { feedback: ref __self_1_0 } =>
                        match *self {
                            ErrorStack { feedback: ref __self_0_0 } =>
                                (*__self_0_0) == (*__self_1_0),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &ErrorStack) -> bool {
                match *other {
                    ErrorStack { feedback: ref __self_1_0 } =>
                        match *self {
                            ErrorStack { feedback: ref __self_0_0 } =>
                                (*__self_0_0) != (*__self_1_0),
                        },
                }
            }
        }
        impl ErrorStack {
            pub fn new(err_source: &'static str) -> Self {
                Self { feedback: FeedbackStack::new(err_source, "errors") }
            }
        }
        impl fmt::Display for ErrorStack {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(&[""],
                        &[::core::fmt::ArgumentV1::new_display(&self.feedback)]))
            }
        }
        impl ops::Deref for ErrorStack {
            type Target = FeedbackStack;
            fn deref(&self) -> &Self::Target { &self.feedback }
        }
        impl ops::DerefMut for ErrorStack {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.feedback
            }
        }
        pub struct WarningStack {
            feedback: FeedbackStack,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for WarningStack {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    WarningStack { feedback: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "WarningStack");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "feedback", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl ::core::marker::StructuralPartialEq for WarningStack {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for WarningStack {
            #[inline]
            fn eq(&self, other: &WarningStack) -> bool {
                match *other {
                    WarningStack { feedback: ref __self_1_0 } =>
                        match *self {
                            WarningStack { feedback: ref __self_0_0 } =>
                                (*__self_0_0) == (*__self_1_0),
                        },
                }
            }
            #[inline]
            fn ne(&self, other: &WarningStack) -> bool {
                match *other {
                    WarningStack { feedback: ref __self_1_0 } =>
                        match *self {
                            WarningStack { feedback: ref __self_0_0 } =>
                                (*__self_0_0) != (*__self_1_0),
                        },
                }
            }
        }
        impl WarningStack {
            pub fn new(warning_source: &'static str) -> Self {
                Self {
                    feedback: FeedbackStack::new(warning_source, "warnings"),
                }
            }
            pub fn print_warnings(&self) {
                if !self.feedback.is_empty() {
                        {
                            let lvl = ::log::Level::Warn;
                            if lvl <= ::log::STATIC_MAX_LEVEL &&
                                        lvl <= ::log::max_level() {
                                    ::log::__private_api_log(::core::fmt::Arguments::new_v1(&[""],
                                            &[::core::fmt::ArgumentV1::new_display(&self)]), lvl,
                                        &("skyd::config::feedback", "skyd::config::feedback",
                                                "server/src/config/feedback.rs", 143u32),
                                        ::log::__private_api::Option::None);
                                }
                        };
                    }
            }
        }
        impl ops::Deref for WarningStack {
            type Target = FeedbackStack;
            fn deref(&self) -> &Self::Target { &self.feedback }
        }
        impl ops::DerefMut for WarningStack {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.feedback
            }
        }
        impl fmt::Display for WarningStack {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(&[""],
                        &[::core::fmt::ArgumentV1::new_display(&self.feedback)]))
            }
        }
        pub enum ConfigError {
            OSError(IoError),
            CfgError(ErrorStack),
            ConfigFileParseError(TomlError),
            Conflict,
            ProdError(ErrorStack),
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for ConfigError {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&ConfigError::OSError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "OSError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&ConfigError::CfgError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "CfgError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&ConfigError::ConfigFileParseError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f,
                                    "ConfigFileParseError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&ConfigError::Conflict,) => {
                        ::core::fmt::Formatter::write_str(f, "Conflict")
                    }
                    (&ConfigError::ProdError(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "ProdError");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl PartialEq for ConfigError {
            fn eq(&self, oth: &Self) -> bool {
                match (self, oth) {
                    (Self::OSError(lhs), Self::OSError(rhs)) =>
                        lhs.to_string() == rhs.to_string(),
                    (Self::CfgError(lhs), Self::CfgError(rhs)) => lhs == rhs,
                    (Self::ConfigFileParseError(lhs),
                        Self::ConfigFileParseError(rhs)) => lhs == rhs,
                    (Self::Conflict, Self::Conflict) => true,
                    (Self::ProdError(lhs), Self::ProdError(rhs)) => lhs == rhs,
                    _ => false,
                }
            }
        }
        impl From<IoError> for ConfigError {
            fn from(e: IoError) -> Self { Self::OSError(e) }
        }
        impl From<toml::de::Error> for ConfigError {
            fn from(e: toml::de::Error) -> Self {
                Self::ConfigFileParseError(e)
            }
        }
        impl fmt::Display for ConfigError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Self::ConfigFileParseError(e) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Configuration file parse failed: "],
                                &[::core::fmt::ArgumentV1::new_display(&e)])),
                    Self::OSError(e) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["OS Error: "],
                                &[::core::fmt::ArgumentV1::new_display(&e)])),
                    Self::CfgError(e) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&[""],
                                &[::core::fmt::ArgumentV1::new_display(&e)])),
                    Self::Conflict =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Conflict error: Either provide CLI args, environment variables or a config file for configuration"],
                                &[])),
                    Self::ProdError(e) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["You have invalid configuration for production mode. "],
                                &[::core::fmt::ArgumentV1::new_display(&e)])),
                }
            }
        }
        #[cfg(unix)]
        fn check_rlimit_or_err(current: usize, estack: &mut ErrorStack)
            -> Result<(), ConfigError> {
            let rlim = ResourceLimit::get()?;
            if rlim.is_over_limit(current) {
                    estack.push("The value for maximum connections exceeds available resources to the server process");
                    estack.push({
                            let res =
                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["The current process is set to a resource limit of ",
                                                    " and can be set to a maximum limit of ", " in the OS"],
                                        &[::core::fmt::ArgumentV1::new_display(&rlim.current()),
                                                    ::core::fmt::ArgumentV1::new_display(&rlim.max())]));
                            res
                        });
                }
            Ok(())
        }
        /// Check if the settings are suitable for use in production mode
        pub(super) fn evaluate_prod_settings(cfg: &ConfigurationSet)
            -> Result<(), ConfigError> {
            let mut estack = ErrorStack::new(EMSG_PROD);
            if cfg.is_artful() {
                    estack.push("Terminal artwork should be disabled");
                }
            if cfg.bgsave.is_disabled() {
                    estack.push("BGSAVE must be enabled");
                }
            if let SnapshotConfig::Enabled(SnapshotPref { poison, .. }) =
                        cfg.snapshot {
                    if !poison { estack.push("Snapshots must be failsafe"); }
                }
            if cfg.ports.insecure_only() {
                    estack.push("Either multi-socket (TCP and TLS) or TLS only must be enabled");
                }
            if cfg.auth.origin_key.is_some() && !cfg.ports.secure_only() {
                    estack.push("When authn+authz is enabled, TLS-only mode must be enabled");
                }
            check_rlimit_or_err(cfg.maxcon, &mut estack)?;
            if estack.is_empty() {
                    Ok(())
                } else { Err(ConfigError::ProdError(estack)) }
        }
    }
    use self::cfgfile::Config as ConfigFile;
    pub use self::definitions::*;
    use self::feedback::{ConfigError, ErrorStack, WarningStack};
    use crate::dbnet::MAXIMUM_CONNECTION_LIMIT;
    const DEFAULT_IPV4: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    const DEFAULT_PORT: u16 = 2003;
    const DEFAULT_BGSAVE_DURATION: u64 = 120;
    const DEFAULT_SNAPSHOT_FAILSAFE: bool = true;
    const DEFAULT_SSL_PORT: u16 = 2004;
    type StaticStr = &'static str;
    pub struct AuthkeyWrapper(pub Authkey);
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for AuthkeyWrapper {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                AuthkeyWrapper(ref __self_0_0) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f,
                                "AuthkeyWrapper");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for AuthkeyWrapper {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for AuthkeyWrapper {
        #[inline]
        fn eq(&self, other: &AuthkeyWrapper) -> bool {
            match *other {
                AuthkeyWrapper(ref __self_1_0) =>
                    match *self {
                        AuthkeyWrapper(ref __self_0_0) =>
                            (*__self_0_0) == (*__self_1_0),
                    },
            }
        }
        #[inline]
        fn ne(&self, other: &AuthkeyWrapper) -> bool {
            match *other {
                AuthkeyWrapper(ref __self_1_0) =>
                    match *self {
                        AuthkeyWrapper(ref __self_0_0) =>
                            (*__self_0_0) != (*__self_1_0),
                    },
            }
        }
    }
    impl AuthkeyWrapper {
        pub const fn empty() -> Self { Self([0u8; 40]) }
        pub fn try_new(slf: &str) -> Option<Self> {
            let valid =
                slf.len() == 40 &&
                    slf.chars().all(|ch| char::is_ascii_alphanumeric(&ch));
            if valid {
                    let mut ret = Self::empty();
                    slf.bytes().enumerate().for_each(|(idx, byte)|
                            { ret.0[idx] = byte; });
                    Some(ret)
                } else { None }
        }
        pub fn into_inner(self) -> Authkey { self.0 }
    }
    /// An enum representing the outcome of a parse operation for a specific configuration item from a
    /// specific configuration source
    pub enum ConfigSourceParseResult<T> { Okay(T), Absent, ParseFailure, }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<T: ::core::fmt::Debug> ::core::fmt::Debug for
        ConfigSourceParseResult<T> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&ConfigSourceParseResult::Okay(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Okay");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&ConfigSourceParseResult::Absent,) => {
                    ::core::fmt::Formatter::write_str(f, "Absent")
                }
                (&ConfigSourceParseResult::ParseFailure,) => {
                    ::core::fmt::Formatter::write_str(f, "ParseFailure")
                }
            }
        }
    }
    /// A trait for configuration sources. Any type implementing this trait is considered to be a valid
    /// source for configuration
    pub trait TryFromConfigSource<T: Sized>: Sized {
        /// Check if the value is present
        fn is_present(&self)
        -> bool;
        /// Attempt to mutate the value if present. If any error occurs
        /// while parsing the value, return true. Else return false if all went well.
        /// Here:
        /// - `target_value`: is a mutable reference to the target var
        /// - `trip`: is a mutable ref to a bool that will be set to true if a value is present
        /// (whether parseable or not)
        fn mutate_failed(self, target_value: &mut T, trip: &mut bool)
        -> bool;
        /// Attempt to parse the value into the target type
        fn try_parse(self)
        -> ConfigSourceParseResult<T>;
    }
    impl<'a, T: FromStr + 'a> TryFromConfigSource<T> for Option<&'a str> {
        fn is_present(&self) -> bool { self.is_some() }
        fn mutate_failed(self, target_value: &mut T, trip: &mut bool)
            -> bool {
            self.map(|slf|
                        {
                            *trip = true;
                            match slf.parse() {
                                Ok(p) => { *target_value = p; false }
                                Err(_) => true,
                            }
                        }).unwrap_or(false)
        }
        fn try_parse(self) -> ConfigSourceParseResult<T> {
            self.map(|s|
                        {
                            s.parse().map(|ret|
                                        ConfigSourceParseResult::Okay(ret)).unwrap_or(ConfigSourceParseResult::ParseFailure)
                        }).unwrap_or(ConfigSourceParseResult::Absent)
        }
    }
    impl FromStr for AuthkeyWrapper {
        type Err = ();
        fn from_str(slf: &str) -> Result<Self, Self::Err> {
            Self::try_new(slf).ok_or(())
        }
    }
    impl<'a, T: FromStr + 'a> TryFromConfigSource<T> for
        Result<String, VarError> {
        fn is_present(&self) -> bool {
            !match self { Err(VarError::NotPresent) => true, _ => false, }
        }
        fn mutate_failed(self, target_value: &mut T, trip: &mut bool)
            -> bool {
            match self {
                Ok(s) => {
                    *trip = true;
                    s.parse().map(|v|
                                { *target_value = v; false }).unwrap_or(true)
                }
                Err(e) => {
                    if match e { VarError::NotPresent => true, _ => false, } {
                            false
                        } else { *trip = true; true }
                }
            }
        }
        fn try_parse(self) -> ConfigSourceParseResult<T> {
            match self {
                Ok(s) =>
                    s.parse().map(|v|
                                ConfigSourceParseResult::Okay(v)).unwrap_or(ConfigSourceParseResult::ParseFailure),
                Err(e) =>
                    match e {
                        VarError::NotPresent => ConfigSourceParseResult::Absent,
                        VarError::NotUnicode(_) =>
                            ConfigSourceParseResult::ParseFailure,
                    },
            }
        }
    }
    /// Since we have conflicting trait implementations, we define a custom `Option<String>` type
    pub struct OptString {
        base: Option<String>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for OptString {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                OptString { base: ref __self_0_0 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "OptString");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "base",
                            &&(*__self_0_0));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for OptString {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for OptString {
        #[inline]
        fn eq(&self, other: &OptString) -> bool {
            match *other {
                OptString { base: ref __self_1_0 } =>
                    match *self {
                        OptString { base: ref __self_0_0 } =>
                            (*__self_0_0) == (*__self_1_0),
                    },
            }
        }
        #[inline]
        fn ne(&self, other: &OptString) -> bool {
            match *other {
                OptString { base: ref __self_1_0 } =>
                    match *self {
                        OptString { base: ref __self_0_0 } =>
                            (*__self_0_0) != (*__self_1_0),
                    },
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::default::Default for OptString {
        #[inline]
        fn default() -> OptString {
            OptString { base: ::core::default::Default::default() }
        }
    }
    impl OptString {
        pub const fn new_null() -> Self { Self { base: None } }
    }
    impl From<Option<String>> for OptString {
        fn from(base: Option<String>) -> Self { Self { base } }
    }
    impl FromStr for OptString {
        type Err = ();
        fn from_str(st: &str) -> Result<Self, Self::Err> {
            Ok(Self { base: Some(st.to_string()) })
        }
    }
    impl FromStr for ProtocolVersion {
        type Err = ();
        fn from_str(st: &str) -> Result<Self, Self::Err> {
            match st {
                "1" | "1.0" | "1.1" | "1.2" => Ok(Self::V1),
                "2" | "2.0" => Ok(Self::V2),
                _ => Err(()),
            }
        }
    }
    impl TryFromConfigSource<ProtocolVersion> for Option<ProtocolVersion> {
        fn is_present(&self) -> bool { self.is_some() }
        fn mutate_failed(self, target: &mut ProtocolVersion, trip: &mut bool)
            -> bool {
            if let Some(v) = self { *target = v; *trip = true; }
            false
        }
        fn try_parse(self) -> ConfigSourceParseResult<ProtocolVersion> {
            self.map(ConfigSourceParseResult::Okay).unwrap_or(ConfigSourceParseResult::Absent)
        }
    }
    impl TryFromConfigSource<OptString> for OptString {
        fn is_present(&self) -> bool { self.base.is_some() }
        fn mutate_failed(self, target: &mut OptString, trip: &mut bool)
            -> bool {
            if let Some(v) = self.base {
                    target.base = Some(v);
                    *trip = true;
                }
            false
        }
        fn try_parse(self) -> ConfigSourceParseResult<OptString> {
            self.base.map(|v|
                        ConfigSourceParseResult::Okay(OptString {
                                base: Some(v),
                            })).unwrap_or(ConfigSourceParseResult::Absent)
        }
    }
    /// A high-level configuration set that automatically handles errors, warnings and provides a convenient [`Result`]
    /// type that can be used
    pub struct Configset {
        did_mutate: bool,
        cfg: ConfigurationSet,
        estack: ErrorStack,
        wstack: WarningStack,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for Configset {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                Configset {
                    did_mutate: ref __self_0_0,
                    cfg: ref __self_0_1,
                    estack: ref __self_0_2,
                    wstack: ref __self_0_3 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "Configset");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "did_mutate", &&(*__self_0_0));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "cfg",
                            &&(*__self_0_1));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "estack", &&(*__self_0_2));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "wstack", &&(*__self_0_3));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl Configset {
        const EMSG_ENV: StaticStr = "Environment";
        const EMSG_CLI: StaticStr = "CLI";
        const EMSG_FILE: StaticStr = "Configuration file";
        /// Internal ctor for a given feedback source. We do not want to expose this to avoid
        /// erroneous feedback source names
        fn _new(feedback_source: StaticStr) -> Self {
            Self {
                did_mutate: false,
                cfg: ConfigurationSet::default(),
                estack: ErrorStack::new(feedback_source),
                wstack: WarningStack::new(feedback_source),
            }
        }
        /// Create a new configset for environment variables
        pub fn new_env() -> Self { Self::_new(Self::EMSG_ENV) }
        /// Create a new configset for CLI args
        pub fn new_cli() -> Self { Self::_new(Self::EMSG_CLI) }
        /// Create a new configset for config files
        pub fn new_file() -> Self {
            Self {
                did_mutate: true,
                cfg: ConfigurationSet::default(),
                estack: ErrorStack::new(Self::EMSG_FILE),
                wstack: WarningStack::new(Self::EMSG_FILE),
            }
        }
        /// Mark the configset mutated
        fn mutated(&mut self) { self.did_mutate = true; }
        /// Push an error onto the error stack
        fn epush(&mut self, field_key: StaticStr, expected: StaticStr) {
            self.estack.push({
                    let res =
                        ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Bad value for `",
                                            "`. Expected "],
                                &[::core::fmt::ArgumentV1::new_display(&field_key),
                                            ::core::fmt::ArgumentV1::new_display(&expected)]));
                    res
                })
        }
        /// Check if no errors have occurred
        pub fn is_okay(&self) -> bool { self.estack.is_empty() }
        /// Check if the configset was mutated
        pub fn is_mutated(&self) -> bool { self.did_mutate }
        /// Attempt to mutate with a target `TryFromConfigSource` type, and push in any error that occurs
        /// using the given diagnostic info
        fn try_mutate<T>(&mut self, new: impl TryFromConfigSource<T>,
            target: &mut T, field_key: StaticStr, expected: StaticStr) {
            if new.mutate_failed(target, &mut self.did_mutate) {
                    self.epush(field_key, expected)
                }
        }
        /// Attempt to mutate with a target `TryFromConfigSource` type, and push in any error that occurs
        /// using the given diagnostic info while checking the correctly parsed type using the provided validation
        /// closure for any additional validation check that goes beyond type correctness
        fn try_mutate_with_condcheck<T,
            F>(&mut self, new: impl TryFromConfigSource<T>, target: &mut T,
            field_key: StaticStr, expected: StaticStr, validation_fn: F) where
            F: Fn(&T) -> bool {
            let mut needs_error = false;
            match new.try_parse() {
                ConfigSourceParseResult::Okay(ok) => {
                    self.mutated();
                    needs_error = !validation_fn(&ok);
                    *target = ok;
                }
                ConfigSourceParseResult::ParseFailure => {
                    self.mutated();
                    needs_error = true
                }
                ConfigSourceParseResult::Absent => {}
            }
            if needs_error { self.epush(field_key, expected) }
        }
        /// This method can be used to chain configurations to ultimately return the first modified configuration
        /// that occurs. For example: `cfg_file.and_then(cfg_cli).and_then(cfg_env)`; it will return the first
        /// modified Configset
        ///
        /// ## Panics
        /// This method will panic if both the provided sets are mutated. Hence, you need to check beforehand that
        /// there is no conflict
        pub fn and_then(self, other: Self) -> Self {
            if self.is_mutated() {
                    if other.is_mutated() {
                            ::core::panicking::panic_fmt(::core::fmt::Arguments::new_v1(&["Double mutation: ",
                                                " and "],
                                    &[::core::fmt::ArgumentV1::new_display(&self.estack.source()),
                                                ::core::fmt::ArgumentV1::new_display(&other.estack.source())]));
                        }
                    self
                } else { other }
        }
        /// Turns self into a Result that can be used by config::get_config()
        pub fn into_result(self, restore_file: Option<String>)
            -> Result<ConfigType, ConfigError> {
            let mut target =
                if self.is_okay() {
                        if self.is_mutated() {
                                let Self { cfg, wstack, .. } = self;
                                ConfigType::new_custom(cfg, restore_file, wstack)
                            } else { ConfigType::new_default(restore_file) }
                    } else { return Err(ConfigError::CfgError(self.estack)); };
            if target.config.protocol != ProtocolVersion::default() {
                    target.wpush({
                            let res =
                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["",
                                                    " is deprecated. Switch to "],
                                        &[::core::fmt::ArgumentV1::new_display(&target.config.protocol.to_string()),
                                                    ::core::fmt::ArgumentV1::new_display(&ProtocolVersion::default().to_string())]));
                            res
                        });
                }
            if target.is_prod_mode() {
                    self::feedback::evaluate_prod_settings(&target.config).map(|_|
                            target)
                } else {
                   target.wpush("Running in `user` mode. Set mode to `prod` in production");
                   Ok(target)
               }
        }
    }
    impl Configset {
        pub fn protocol_settings(&mut self,
            nproto: impl TryFromConfigSource<ProtocolVersion>,
            nproto_key: StaticStr) {
            let mut proto = ProtocolVersion::default();
            self.try_mutate(nproto, &mut proto, nproto_key,
                "a protocol version like 2.0 or 1.0");
            self.cfg.protocol = proto;
        }
    }
    impl Configset {
        pub fn server_tcp(&mut self, nhost: impl TryFromConfigSource<IpAddr>,
            nhost_key: StaticStr, nport: impl TryFromConfigSource<u16>,
            nport_key: StaticStr) {
            let mut host = DEFAULT_IPV4;
            let mut port = DEFAULT_PORT;
            self.try_mutate(nhost, &mut host, nhost_key,
                "an IPv4/IPv6 address");
            self.try_mutate(nport, &mut port, nport_key,
                "a 16-bit positive integer");
            self.cfg.ports = PortConfig::new_insecure_only(host, port);
        }
        pub fn server_noart(&mut self, nart: impl TryFromConfigSource<bool>,
            nart_key: StaticStr) {
            let mut noart = false;
            self.try_mutate(nart, &mut noart, nart_key, "true/false");
            self.cfg.noart = noart;
        }
        pub fn server_maxcon(&mut self,
            nmaxcon: impl TryFromConfigSource<usize>,
            nmaxcon_key: StaticStr) {
            let mut maxcon = MAXIMUM_CONNECTION_LIMIT;
            self.try_mutate_with_condcheck(nmaxcon, &mut maxcon, nmaxcon_key,
                "a positive integer greater than zero", |max| *max > 0);
            self.cfg.maxcon = maxcon;
        }
        pub fn server_mode(&mut self,
            nmode: impl TryFromConfigSource<Modeset>, nmode_key: StaticStr) {
            let mut modeset = Modeset::Dev;
            self.try_mutate(nmode, &mut modeset, nmode_key,
                "a string with 'user' or 'prod'");
            self.cfg.mode = modeset;
        }
    }
    impl Configset {
        pub fn bgsave_settings(&mut self,
            nenabled: impl TryFromConfigSource<bool>, nenabled_key: StaticStr,
            nduration: impl TryFromConfigSource<u64>,
            nduration_key: StaticStr) {
            let mut enabled = true;
            let mut duration = DEFAULT_BGSAVE_DURATION;
            let has_custom_duration = nduration.is_present();
            self.try_mutate(nenabled, &mut enabled, nenabled_key,
                "true/false");
            self.try_mutate_with_condcheck(nduration, &mut duration,
                nduration_key, "a positive integer greater than zero",
                |dur| *dur > 0);
            if enabled {
                    self.cfg.bgsave = BGSave::Enabled(duration);
                } else {
                   if has_custom_duration {
                           self.wstack.push({
                                   let res =
                                       ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Specifying `",
                                                           "` is useless when BGSAVE is disabled"],
                                               &[::core::fmt::ArgumentV1::new_display(&nduration_key)]));
                                   res
                               });
                       }
                   self.wstack.push("BGSAVE is disabled. You may lose data if the host crashes");
               }
        }
    }
    impl Configset {
        pub fn snapshot_settings(&mut self,
            nevery: impl TryFromConfigSource<u64>, nevery_key: StaticStr,
            natmost: impl TryFromConfigSource<usize>, natmost_key: StaticStr,
            nfailsafe: impl TryFromConfigSource<bool>,
            nfailsafe_key: StaticStr) {
            match (nevery.is_present(), natmost.is_present()) {
                (false, false) => {
                    if nfailsafe.is_present() {
                            let mut _failsafe = DEFAULT_SNAPSHOT_FAILSAFE;
                            self.try_mutate(nfailsafe, &mut _failsafe, nfailsafe_key,
                                "true/false");
                            self.wstack.push({
                                    let res =
                                        ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Specifying `",
                                                            "` is usless when snapshots are disabled"],
                                                &[::core::fmt::ArgumentV1::new_display(&nfailsafe_key)]));
                                    res
                                });
                        }
                }
                (true, true) => {
                    let mut every = 0;
                    let mut atmost = 0;
                    let mut failsafe = DEFAULT_SNAPSHOT_FAILSAFE;
                    self.try_mutate_with_condcheck(nevery, &mut every,
                        nevery_key, "an integer greater than 0", |dur| *dur > 0);
                    self.try_mutate(natmost, &mut atmost, natmost_key,
                        "a positive integer. 0 indicates that all snapshots will be kept");
                    self.try_mutate(nfailsafe, &mut failsafe, nfailsafe_key,
                        "true/false");
                    self.cfg.snapshot =
                        SnapshotConfig::Enabled(SnapshotPref::new(every, atmost,
                                failsafe));
                }
                (false, true) | (true, false) => {
                    self.mutated();
                    self.estack.push({
                            let res =
                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["To use snapshots, pass values for both `",
                                                    "` and `", "`"],
                                        &[::core::fmt::ArgumentV1::new_display(&nevery_key),
                                                    ::core::fmt::ArgumentV1::new_display(&natmost_key)]));
                            res
                        })
                }
            }
        }
    }
    #[allow(clippy :: too_many_arguments)]
    impl Configset {
        pub fn tls_settings(&mut self, nkey: impl TryFromConfigSource<String>,
            nkey_key: StaticStr, ncert: impl TryFromConfigSource<String>,
            ncert_key: StaticStr, nport: impl TryFromConfigSource<u16>,
            nport_key: StaticStr, nonly: impl TryFromConfigSource<bool>,
            nonly_key: StaticStr, npass: impl TryFromConfigSource<OptString>,
            npass_key: StaticStr) {
            match (nkey.is_present(), ncert.is_present()) {
                (true, true) => {
                    let mut key = String::new();
                    let mut cert = String::new();
                    self.try_mutate(nkey, &mut key, nkey_key,
                        "path to private key file");
                    self.try_mutate(ncert, &mut cert, ncert_key,
                        "path to TLS certificate file");
                    let mut port = DEFAULT_SSL_PORT;
                    self.try_mutate(nport, &mut port, nport_key,
                        "a positive 16-bit integer");
                    let mut tls_only = false;
                    self.try_mutate(nonly, &mut tls_only, nonly_key,
                        "true/false");
                    let mut tls_pass = OptString::new_null();
                    self.try_mutate(npass, &mut tls_pass, npass_key,
                        "path to TLS cert passphrase");
                    let sslopts = SslOpts::new(key, cert, port, tls_pass.base);
                    if tls_only {
                            let host = self.cfg.ports.get_host();
                            self.cfg.ports = PortConfig::new_secure_only(host, sslopts)
                        } else { self.cfg.ports.upgrade_to_tls(sslopts); }
                }
                (true, false) | (false, true) => {
                    self.mutated();
                    self.estack.push({
                            let res =
                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["To use TLS, pass values for both `",
                                                    "` and `", "`"],
                                        &[::core::fmt::ArgumentV1::new_display(&nkey_key),
                                                    ::core::fmt::ArgumentV1::new_display(&ncert_key)]));
                            res
                        });
                }
                (false, false) => {
                    if nport.is_present() {
                            self.mutated();
                            self.wstack.push({
                                    let res =
                                        ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Specifying `",
                                                            "` is pointless when TLS is disabled"],
                                                &[::core::fmt::ArgumentV1::new_display(&nport_key)]));
                                    res
                                });
                        }
                    if nonly.is_present() {
                            self.mutated();
                            self.wstack.push({
                                    let res =
                                        ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Specifying `",
                                                            "` is pointless when TLS is disabled"],
                                                &[::core::fmt::ArgumentV1::new_display(&nonly_key)]));
                                    res
                                });
                        }
                    if npass.is_present() {
                            self.mutated();
                            self.wstack.push({
                                    let res =
                                        ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["Specifying `",
                                                            "` is pointless when TLS is disabled"],
                                                &[::core::fmt::ArgumentV1::new_display(&npass_key)]));
                                    res
                                });
                        }
                }
            }
        }
    }
    impl Configset {
        pub fn auth_settings(&mut self,
            nauth: impl TryFromConfigSource<AuthkeyWrapper>,
            nauth_key: StaticStr) {
            let mut def = AuthkeyWrapper::empty();
            self.try_mutate(nauth, &mut def, nauth_key,
                "A 40-byte long ASCII string");
            if def != AuthkeyWrapper::empty() {
                    self.cfg.auth = AuthSettings { origin_key: Some(def) };
                }
        }
    }
    pub fn get_config() -> Result<ConfigType, ConfigError> {
        let cfg_layout =
            &::clap::YamlLoader::load_from_str("name: Skytable Server\nversion: 0.8.0\nauthor: Sayan N. <ohsayan@outlook.com>\nabout: The Skytable Database server\nargs:\n  - config:\n      short: c\n      required: false\n      long: withconfig\n      value_name: cfgfile\n      help: Sets a configuration file to start skyd\n      takes_value: true\n  - restore:\n      short: r\n      required: false\n      long: restore\n      value_name: backupdir\n      help: Restores data from a previous snapshot made in the provided directory\n      takes_value: true\n  - host:\n      short: h\n      required: false\n      long: host\n      value_name: host\n      help: Sets the host to which the server will bind\n      takes_value: true\n  - port:\n      short: p\n      required: false\n      long: port\n      value_name: port\n      help: Sets the port to which the server will bind\n      takes_value: true\n  - noart:\n      required: false\n      long: noart\n      help: Disables terminal artwork\n      takes_value: false\n  - nosave:\n      required: false\n      long: nosave\n      help: Disables automated background saving\n      takes_value: false\n  - saveduration:\n      required: false\n      long: saveduration\n      value_name: duration\n      short: S\n      takes_value: true\n      help: Set the BGSAVE duration\n  - snapevery:\n      required: false\n      long: snapevery\n      value_name: duration\n      help: Set the periodic snapshot duration\n      takes_value: true\n  - snapkeep:\n      required: false\n      long: snapkeep\n      value_name: count\n      help: Sets the number of most recent snapshots to keep\n      takes_value: true\n  - sslkey:\n      required: false\n      long: sslkey\n      short: k\n      value_name: key\n      help: Sets the PEM key file to use for SSL/TLS\n      takes_value: true\n  - sslchain:\n      required: false\n      long: sslchain\n      short: z\n      value_name: chain\n      help: Sets the PEM chain file to use for SSL/TLS\n      takes_value: true\n  - sslonly:\n      required: false\n      long: sslonly\n      takes_value: false\n      help: Tells the server to only accept SSL connections and disables the non-SSL port\n  - sslport:\n      required: false\n      long: sslport\n      takes_value: true\n      value_name: sslport\n      help: Set a custom SSL port to bind to\n  - tlspassin:\n      required: false\n      long: tlspassin\n      takes_value: true\n      value_name: tlspassin\n      help: Path to the file containing the passphrase for the TLS certificate\n  - stopwriteonfail:\n      required: false\n      long: stop-write-on-fail\n      takes_value: true\n      help: Stop accepting writes if any persistence method except BGSAVE fails (defaults to true)\n  - maxcon:\n      required: false\n      long: maxcon\n      takes_value: true\n      help: Set the maximum number of connections\n      value_name: maxcon\n  - mode:\n      required: false\n      long: mode\n      takes_value: true\n      short: m\n      help: Sets the deployment type\n      value_name: mode\n  - authkey:\n      required: false\n      long: auth-origin-key\n      takes_value: true\n      help: Set the authentication origin key\n      value_name: origin_key\n  - protover:\n      required: false\n      long: protover\n      takes_value: true\n      help: Set the protocol version\n      value_name: protover\n").expect("failed to load YAML file")[0];
        let matches = App::from_yaml(cfg_layout).get_matches();
        let restore_file = matches.value_of("restore").map(|v| v.to_string());
        let cfg_from_file =
            if let Some(file) = matches.value_of("config") {
                    let file = fs::read(file)?;
                    let cfg_file: ConfigFile = toml::from_slice(&file)?;
                    Some(cfgfile::from_file(cfg_file))
                } else { None };
        let cfg_from_cli = cfgcli::parse_cli_args(matches);
        let cfg_from_env = cfgenv::parse_env_config();
        let cfg_degree =
            cfg_from_cli.is_mutated() as u8 + cfg_from_env.is_mutated() as u8
                + cfg_from_file.is_some() as u8;
        let has_conflict = cfg_degree > 1;
        if has_conflict { return Err(ConfigError::Conflict); }
        if cfg_degree == 0 {
                Ok(ConfigType::new_default(restore_file))
            } else {
               cfg_from_file.unwrap_or_else(||
                           cfg_from_env.and_then(cfg_from_cli)).into_result(restore_file)
           }
    }
}
mod corestore {
    use crate::actions::ActionResult;
    use crate::corestore::{
        memstore::{DdlError, Keyspace, Memstore, ObjectID, DEFAULT},
        table::{DescribeTable, Table},
    };
    use crate::protocol::interface::ProtocolSpec;
    use crate::queryengine::parser::{Entity, OwnedEntity};
    use crate::registry;
    use crate::storage;
    use crate::storage::v1::{
        error::StorageEngineResult, sengine::SnapshotEngine,
    };
    use crate::util::Unwrappable;
    use core::borrow::Borrow;
    use core::hash::Hash;
    pub use htable::Data;
    use std::sync::Arc;
    pub mod array {
        use bytes::Bytes;
        use core::any;
        use core::borrow::Borrow;
        use core::borrow::BorrowMut;
        use core::cmp::Ordering;
        use core::convert::TryFrom;
        use core::fmt;
        use core::hash::Hash;
        use core::hash::Hasher;
        use core::iter::FromIterator;
        use core::mem::ManuallyDrop;
        use core::mem::MaybeUninit;
        use core::ops;
        use core::ptr;
        use core::slice;
        use core::str;
        /// A compile-time, fixed size array that can have unintialized memory. This array is as
        /// efficient as you'd expect a normal array to be, but with the added benefit that you
        /// don't have to initialize all the elements. This was inspired by the arrayvec crate.
        /// Safe abstractions are made available enabling us to not enter uninitialized space and
        /// read the _available_ elements. The array size is limited to 16 bits or 2 bytes to
        /// prevent stack overflows.
        ///
        /// ## Panics
        /// To avoid stack corruption among other crazy things, several implementations like [`Extend`]
        /// can panic. There are _silently corrupting_ methods too which can be used if you can uphold
        /// the guarantees
        pub struct Array<T, const N : usize> {
            /// the maybe bad stack
            stack: [MaybeUninit<T>; N],
            /// the initialized length
            /// no stack should be more than 16 bytes
            init_len: u16,
        }
        /// The len scopeguard is like a scopeguard that provides panic safety incase an append-like
        /// operation involving iterators causes the iterator to panic. This makes sure that we still
        /// set the len on panic
        pub struct LenScopeGuard<'a, T: Copy> {
            real_ref: &'a mut T,
            temp: T,
        }
        impl<'a, T: ops::AddAssign + Copy> LenScopeGuard<'a, T> {
            pub fn new(real_ref: &'a mut T) -> Self {
                let ret = *real_ref;
                Self { real_ref, temp: ret }
            }
            pub fn incr(&mut self, val: T) { self.temp += val; }
            pub fn get_temp(&self) -> T { self.temp }
        }
        impl<'a, T: Copy> Drop for LenScopeGuard<'a, T> {
            fn drop(&mut self) { *self.real_ref = self.temp; }
        }
        macro_rules! impl_zeroed_nm {
            ($($ty : ty), * $(,) ?) =>
            {
                $(impl < const N : usize > Array < $ty, N >
                {
                    pub const fn new_zeroed() -> Self
                    {
                        Self
                        {
                            stack : [MaybeUninit :: new(0) ; N], init_len : N as u16,
                        }
                    }
                }) *
            } ;
        }
        impl<const N : usize> Array<u8, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<i8, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<u16, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<i16, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<u32, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<i32, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<u64, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<i64, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<u128, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<i128, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<usize, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<const N : usize> Array<isize, N> {
            pub const fn new_zeroed() -> Self {
                Self { stack: [MaybeUninit::new(0); N], init_len: N as u16 }
            }
        }
        impl<T, const N : usize> Array<T, N> {
            const VALUE: MaybeUninit<T> = MaybeUninit::uninit();
            const ARRAY: [MaybeUninit<T>; N] = [Self::VALUE; N];
            /// Create a new array
            pub const fn new() -> Self {
                Array { stack: Self::ARRAY, init_len: 0 }
            }
            /// This is very safe from the ctor point of view, but the correctness of `init_len`
            /// may be a bad assumption and might make us read garbage
            pub const unsafe fn from_const(array: [MaybeUninit<T>; N],
                init_len: u16) -> Self {
                Self { stack: array, init_len }
            }
            pub unsafe fn bump_init_len(&mut self, bump: u16) {
                self.init_len += bump
            }
            /// This literally turns [T; M] into [T; N]. How can you expect it to be safe?
            /// This function is extremely unsafe. I mean, I don't even know how to call it safe.
            /// There's one way though: make M == N. This will panic in debug mode if M > N. In
            /// release mode, good luck
            unsafe fn from_const_array<const M : usize>(arr: [T; M]) -> Self {
                if true {
                        if !(N >= M) {
                                ::core::panicking::panic_fmt(::core::fmt::Arguments::new_v1(&["Provided const array exceeds size limit of initialized array"],
                                        &[]))
                            };
                    };
                let array = ManuallyDrop::new(arr);
                let mut arr = Array::<T, N>::new();
                let ptr =
                    &*array as *const [T; M] as *const [MaybeUninit<T>; N];
                ptr.copy_to_nonoverlapping(&mut arr.stack as
                        *mut [MaybeUninit<T>; N], 1);
                arr.set_len(N);
                arr
            }
            /// Get the apparent length of the array
            pub const fn len(&self) -> usize { self.init_len as usize }
            /// Get the capacity of the array
            pub const fn capacity(&self) -> usize { N }
            /// Check if the array is full
            pub const fn is_full(&self) -> bool { N == self.len() }
            /// Get the remaining capacity of the array
            pub const fn remaining_cap(&self) -> usize {
                self.capacity() - self.len()
            }
            /// Set the length of the array
            ///
            /// ## Safety
            /// This is one of those, use to leak memory functions. If you change the length,
            /// you'll be reading random garbage from the memory and doing a double-free on drop
            pub unsafe fn set_len(&mut self, len: usize) {
                self.init_len = len as u16;
            }
            /// Get the array as a mut ptr
            unsafe fn as_mut_ptr(&mut self) -> *mut T {
                self.stack.as_mut_ptr() as *mut _
            }
            /// Get the array as a const ptr
            unsafe fn as_ptr(&self) -> *const T {
                self.stack.as_ptr() as *const _
            }
            /// Push an element into the array **without any bounds checking**.
            ///
            /// ## Safety
            /// This function is **so unsafe** that you possibly don't want to call it, or
            /// even think about calling it. You can end up corrupting your own stack or
            /// other's valuable data
            pub unsafe fn push_unchecked(&mut self, element: T) {
                let len = self.len();
                ptr::write(self.as_mut_ptr().add(len), element);
                self.set_len(len + 1);
            }
            /// This is a nice version of a push that does bound checks
            pub fn push_panic(&mut self, element: T) -> Result<(), ()> {
                if self.len() < N {
                        unsafe { self.push_unchecked(element) };
                        Ok(())
                    } else { Err(()) }
            }
            /// This is a _panicky_ but safer alternative to `push_unchecked` that panics on
            /// incorrect lengths
            pub fn push(&mut self, element: T) {
                self.push_panic(element).unwrap();
            }
            /// Pop an item off the array
            pub fn pop(&mut self) -> Option<T> {
                if self.len() == 0 {
                        None
                    } else {
                       unsafe {
                           let new_len = self.len() - 1;
                           self.set_len(new_len);
                           Some(ptr::read(self.as_ptr().add(new_len)))
                       }
                   }
            }
            /// Truncate the array to a given size. This is super safe and doesn't even panic
            /// if you provide a silly `new_len`.
            pub fn truncate(&mut self, new_len: usize) {
                let len = self.len();
                if new_len < len {
                        unsafe {
                            ptr::drop_in_place(slice::from_raw_parts_mut(self.as_mut_ptr().add(new_len),
                                    len - new_len))
                        }
                    }
            }
            /// Empty the internal array
            pub fn clear(&mut self) { self.truncate(0) }
            /// Extend self from a slice
            pub fn extend_from_slice(&mut self, slice: &[T]) -> Result<(), ()>
                where T: Copy {
                if self.remaining_cap() < slice.len() { return Err(()); }
                unsafe { self.extend_from_slice_unchecked(slice); }
                Ok(())
            }
            /// Extend self from a slice without doing a single check
            ///
            /// ## Safety
            /// This function is just very very and. You can write giant things into your own
            /// stack corrupting it, corrupting other people's things and creating undefined
            /// behavior like no one else.
            pub unsafe fn extend_from_slice_unchecked(&mut self,
                slice: &[T]) {
                let self_len = self.len();
                let other_len = slice.len();
                ptr::copy_nonoverlapping(slice.as_ptr(),
                    self.as_mut_ptr().add(self_len), other_len);
                self.set_len(self_len + other_len);
            }
            /// Returns self as a `[T; N]` array if it is fully initialized. Else it will again return
            /// itself
            pub fn into_array(self) -> Result<[T; N], Self> {
                if self.len() < self.capacity() {
                        Err(self)
                    } else { unsafe { Ok(self.into_array_unchecked()) } }
            }
            pub unsafe fn into_array_unchecked(self) -> [T; N] {
                let _self = ManuallyDrop::new(self);
                ptr::read(_self.as_ptr() as *const [T; N])
            }
            pub fn try_from_slice(slice: impl AsRef<[T]>) -> Option<Self> {
                let slice = slice.as_ref();
                if slice.len() > N {
                        None
                    } else { Some(unsafe { Self::from_slice(slice) }) }
            }
            /// Extend self from a slice
            ///
            /// ## Safety
            /// The same danger as in from_slice_unchecked
            pub unsafe fn from_slice(slice_ref: impl AsRef<[T]>) -> Self {
                let mut slf = Self::new();
                slf.extend_from_slice_unchecked(slice_ref.as_ref());
                slf
            }
            /// Get self as a slice. Super safe because we guarantee that all the other invarians
            /// are upheld
            pub fn as_slice(&self) -> &[T] {
                unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
            }
            /// Get self as a mutable slice. Super safe (see comment above)
            fn as_slice_mut(&mut self) -> &mut [T] {
                unsafe {
                    slice::from_raw_parts_mut(self.as_mut_ptr(), self.len())
                }
            }
        }
        impl<const N : usize> Array<u8, N> {
            /// This isn't _unsafe_ but it can cause functions expecting pure unicode to
            /// crash if the array contains invalid unicode
            pub unsafe fn as_str(&self) -> &str {
                str::from_utf8_unchecked(self)
            }
        }
        impl<T, const N : usize> ops::Deref for Array<T, N> {
            type Target = [T];
            fn deref(&self) -> &Self::Target { self.as_slice() }
        }
        impl<T, const N : usize> ops::DerefMut for Array<T, N> {
            fn deref_mut(&mut self) -> &mut [T] { self.as_slice_mut() }
        }
        impl<T, const N : usize> From<[T; N]> for Array<T, N> {
            fn from(array: [T; N]) -> Self {
                unsafe { Array::from_const_array::<N>(array) }
            }
        }
        impl<T, const N : usize> Drop for Array<T, N> {
            fn drop(&mut self) { self.clear() }
        }
        pub struct ArrayIntoIter<T, const N : usize> {
            state: usize,
            a: Array<T, N>,
        }
        impl<T, const N : usize> Iterator for ArrayIntoIter<T, N> {
            type Item = T;
            fn next(&mut self) -> Option<Self::Item> {
                if self.state == self.a.len() {
                        None
                    } else {
                       let idx = self.state;
                       self.state += 1;
                       Some(unsafe { ptr::read(self.a.as_ptr().add(idx)) })
                   }
            }
            fn size_hint(&self) -> (usize, Option<usize>) {
                let l = self.a.len() - self.state;
                (l, Some(l))
            }
        }
        impl<T, const N : usize> IntoIterator for Array<T, N> {
            type Item = T;
            type IntoIter = ArrayIntoIter<T, N>;
            fn into_iter(self) -> Self::IntoIter {
                ArrayIntoIter { state: 0, a: self }
            }
        }
        impl<T, const N : usize> Array<T, N> {
            /// Extend self using an iterator.
            ///
            /// ## Safety
            /// This function can cause undefined damage to your application's stack and/or other's
            /// data. Only use if you know what you're doing. If you don't use `extend_from_iter`
            /// instead
            pub unsafe fn extend_from_iter_unchecked<I>(&mut self,
                iterable: I) where I: IntoIterator<Item = T> {
                let mut ptr = Self::as_mut_ptr(self).add(self.len());
                let mut guard = LenScopeGuard::new(&mut self.init_len);
                let mut iter = iterable.into_iter();
                loop {
                    if let Some(element) = iter.next() {
                            ptr.write(element);
                            ptr = ptr.add(1);
                            guard.incr(1);
                        } else { return; }
                }
            }
            pub fn extend_from_iter<I>(&mut self, iterable: I) where
                I: IntoIterator<Item = T> {
                unsafe {
                    let mut ptr = Self::as_mut_ptr(self).add(self.len());
                    let end_ptr = Self::as_ptr(self).add(self.capacity());
                    let mut guard = LenScopeGuard::new(&mut self.init_len);
                    let mut iter = iterable.into_iter();
                    loop {
                        if let Some(element) = iter.next() {
                                ptr.write(element);
                                ptr = ptr.add(1);
                                guard.incr(1);
                                if end_ptr < ptr {
                                        ::core::panicking::panic_fmt(::core::fmt::Arguments::new_v1(&["Overflowed stack area."],
                                                &[]))
                                    }
                            } else { return; }
                    }
                }
            }
        }
        impl<T, const N : usize> Extend<T> for Array<T, N> {
            fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
                { self.extend_from_iter::<_>(iter) }
            }
        }
        impl<T, const N : usize> FromIterator<T> for Array<T, N> {
            fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
                let mut arr = Array::new();
                arr.extend(iter);
                arr
            }
        }
        impl<T, const N : usize> Clone for Array<T, N> where T: Clone {
            fn clone(&self) -> Self { self.iter().cloned().collect() }
        }
        impl<T, const N : usize> Hash for Array<T, N> where T: Hash {
            fn hash<H>(&self, hasher: &mut H) where H: Hasher {
                Hash::hash(&**self, hasher)
            }
        }
        impl<const N : usize> PartialEq<[u8]> for Array<u8, N> {
            fn eq(&self, oth: &[u8]) -> bool { **self == *oth }
        }
        impl<const N : usize> PartialEq<Array<u8, N>> for [u8] {
            fn eq(&self, oth: &Array<u8, N>) -> bool {
                oth.as_slice() == self
            }
        }
        impl<T, const N : usize> PartialEq for Array<T, N> where T: PartialEq
            {
            fn eq(&self, other: &Self) -> bool { **self == **other }
        }
        impl<T, const N : usize> Eq for Array<T, N> where T: Eq {}
        impl<T, const N : usize> PartialOrd for Array<T, N> where
            T: PartialOrd {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                (**self).partial_cmp(&**other)
            }
        }
        impl<T, const N : usize> Ord for Array<T, N> where T: Ord {
            fn cmp(&self, other: &Self) -> Ordering { (**self).cmp(&**other) }
        }
        impl<T, const CAP : usize> Borrow<[T]> for Array<T, CAP> {
            fn borrow(&self) -> &[T] { self }
        }
        impl<T, const CAP : usize> BorrowMut<[T]> for Array<T, CAP> {
            fn borrow_mut(&mut self) -> &mut [T] { self }
        }
        impl<T, const CAP : usize> AsRef<[T]> for Array<T, CAP> {
            fn as_ref(&self) -> &[T] { self }
        }
        impl<T, const CAP : usize> AsMut<[T]> for Array<T, CAP> {
            fn as_mut(&mut self) -> &mut [T] { self }
        }
        impl<T, const CAP : usize> fmt::Debug for Array<T, CAP> where
            T: fmt::Debug {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                if any::type_name::<T>().eq(any::type_name::<u8>()) {
                        let slf =
                            unsafe {
                                &*(self as *const Array<T, CAP> as *const Array<u8, CAP>)
                            };
                        match String::from_utf8(slf.to_vec()) {
                            Ok(st) =>
                                f.write_fmt(::core::fmt::Arguments::new_v1_formatted(&[""],
                                        &[::core::fmt::ArgumentV1::new_debug(&st)],
                                        &[::core::fmt::rt::v1::Argument {
                                                        position: 0usize,
                                                        format: ::core::fmt::rt::v1::FormatSpec {
                                                            fill: ' ',
                                                            align: ::core::fmt::rt::v1::Alignment::Unknown,
                                                            flags: 4u32,
                                                            precision: ::core::fmt::rt::v1::Count::Implied,
                                                            width: ::core::fmt::rt::v1::Count::Implied,
                                                        },
                                                    }], unsafe { ::core::fmt::UnsafeArg::new() })),
                            Err(_) => (**self).fmt(f),
                        }
                    } else { (**self).fmt(f) }
            }
        }
        impl<const N : usize> PartialEq<Bytes> for Array<u8, N> {
            fn eq(&self, oth: &Bytes) -> bool {
                self.as_ref() == oth.as_ref()
            }
        }
        impl<const N : usize> PartialEq<Array<u8, N>> for Bytes {
            fn eq(&self, oth: &Array<u8, N>) -> bool {
                self.as_ref() == oth.as_ref()
            }
        }
        impl<const N : usize> Borrow<str> for Array<u8, N> {
            fn borrow(&self) -> &str { unsafe { self.as_str() } }
        }
        unsafe impl<T, const N : usize> Send for Array<T, N> where T: Send {}
        unsafe impl<T, const N : usize> Sync for Array<T, N> where T: Sync {}
        impl<const N : usize> TryFrom<Bytes> for Array<u8, N> {
            type Error = ();
            fn try_from(oth: Bytes) -> Result<Self, Self::Error> {
                if oth.len() != N {
                        Err(())
                    } else { Ok(unsafe { Self::from_slice(oth) }) }
            }
        }
    }
    pub mod backoff {
        use std::cell::Cell;
        use std::hint::spin_loop;
        use std::thread;
        /// Type to perform exponential backoff
        pub struct Backoff {
            cur: Cell<u8>,
        }
        impl Backoff {
            const MAX_SPIN: u8 = 6;
            const MAX_YIELD: u8 = 8;
            pub fn new() -> Self { Self { cur: Cell::new(0) } }
            /// Spin a few times, giving way to the CPU but if we have spun too many times,
            /// then block by yielding to the OS scheduler. This will **eventually block**
            /// if we spin more than the set `MAX_SPIN`
            pub fn snooze(&self) {
                if self.cur.get() <= Self::MAX_SPIN {
                        for _ in 0..1 << self.cur.get() { spin_loop(); }
                    } else { thread::yield_now(); }
                if self.cur.get() <= Self::MAX_YIELD {
                        self.cur.set(self.cur.get() + 1)
                    }
            }
        }
    }
    pub mod booltable {
        use core::ops::Index;
        pub type BytesBoolTable = BoolTable<&'static [u8]>;
        pub type BytesNicheLUT = NicheLUT<&'static [u8]>;
        /// A two-value boolean LUT
        pub struct BoolTable<T> {
            base: [T; 2],
        }
        impl<T> BoolTable<T> {
            /// Supply values in the order: `if_true` and `if_false`
            pub const fn new(if_true: T, if_false: T) -> Self {
                Self { base: [if_false, if_true] }
            }
        }
        impl<T> Index<bool> for BoolTable<T> {
            type Output = T;
            fn index(&self, index: bool) -> &Self::Output {
                unsafe { &*self.base.as_ptr().add(index as usize) }
            }
        }
        /// A LUT based on niche values, especially built to support the `Option<bool>` optimized
        /// structure
        ///
        /// **Warning:** This is a terrible opt and only works on the Rust ABI
        pub struct NicheLUT<T> {
            base: [T; 3],
        }
        impl<T> NicheLUT<T> {
            /// Supply values in the following order: [`if_none`, `if_true`, `if_false`]
            pub const fn new(if_none: T, if_true: T, if_false: T) -> Self {
                Self { base: [if_false, if_true, if_none] }
            }
        }
        impl<T> Index<Option<bool>> for NicheLUT<T> {
            type Output = T;
            fn index(&self, idx: Option<bool>) -> &Self::Output {
                unsafe {
                    &*self.base.as_ptr().add(*(&idx as *const _ as *const u8) as
                                    usize)
                }
            }
        }
        /// A 2-bit indexed boolean LUT
        pub struct TwoBitLUT<T> {
            base: [T; 4],
        }
        type Bit = bool;
        type TwoBitIndex = (Bit, Bit);
        impl<T> TwoBitLUT<T> {
            /// Supply values in the following order:
            /// - 1st unset, 2nd unset
            /// - 1st unset, 2nd set
            /// - 1st set, 2nd unset
            /// - 1st set, 2nd set
            pub const fn new(ff: T, ft: T, tf: T, tt: T) -> Self {
                Self { base: [ff, ft, tf, tt] }
            }
        }
        impl<T> Index<TwoBitIndex> for TwoBitLUT<T> {
            type Output = T;
            fn index(&self, (bit_a, bit_b): TwoBitIndex) -> &Self::Output {
                unsafe {
                    &*self.base.as_ptr().add((((bit_a as u8) << 1) +
                                            (bit_b as u8)) as usize)
                }
            }
        }
    }
    pub mod buffers {
        use super::array::Array;
        use core::ops::Deref;
        use core::str;
        macro_rules! push_self {
            ($self : expr, $what : expr) =>
            { $self.inner_stack.push_unchecked($what) } ;
        }
        macro_rules! lut { ($e : expr) => { ucidx! (PAIR_MAP_LUT, $e) } ; }
        const PAIR_MAP_LUT: [u8; 200] =
            [0x30, 0x30, 0x30, 0x31, 0x30, 0x32, 0x30, 0x33, 0x30, 0x34, 0x30,
                    0x35, 0x30, 0x36, 0x30, 0x37, 0x30, 0x38, 0x30, 0x39, 0x31,
                    0x30, 0x31, 0x31, 0x31, 0x32, 0x31, 0x33, 0x31, 0x34, 0x31,
                    0x35, 0x31, 0x36, 0x31, 0x37, 0x31, 0x38, 0x31, 0x39, 0x32,
                    0x30, 0x32, 0x31, 0x32, 0x32, 0x32, 0x33, 0x32, 0x34, 0x32,
                    0x35, 0x32, 0x36, 0x32, 0x37, 0x32, 0x38, 0x32, 0x39, 0x33,
                    0x30, 0x33, 0x31, 0x33, 0x32, 0x33, 0x33, 0x33, 0x34, 0x33,
                    0x35, 0x33, 0x36, 0x33, 0x37, 0x33, 0x38, 0x33, 0x39, 0x34,
                    0x30, 0x34, 0x31, 0x34, 0x32, 0x34, 0x33, 0x34, 0x34, 0x34,
                    0x35, 0x34, 0x36, 0x34, 0x37, 0x34, 0x38, 0x34, 0x39, 0x35,
                    0x30, 0x35, 0x31, 0x35, 0x32, 0x35, 0x33, 0x35, 0x34, 0x35,
                    0x35, 0x35, 0x36, 0x35, 0x37, 0x35, 0x38, 0x35, 0x39, 0x36,
                    0x30, 0x36, 0x31, 0x36, 0x32, 0x36, 0x33, 0x36, 0x34, 0x36,
                    0x35, 0x36, 0x36, 0x36, 0x37, 0x36, 0x38, 0x36, 0x39, 0x37,
                    0x30, 0x37, 0x31, 0x37, 0x32, 0x37, 0x33, 0x37, 0x34, 0x37,
                    0x35, 0x37, 0x36, 0x37, 0x37, 0x37, 0x38, 0x37, 0x39, 0x38,
                    0x30, 0x38, 0x31, 0x38, 0x32, 0x38, 0x33, 0x38, 0x34, 0x38,
                    0x35, 0x38, 0x36, 0x38, 0x37, 0x38, 0x38, 0x38, 0x39, 0x39,
                    0x30, 0x39, 0x31, 0x39, 0x32, 0x39, 0x33, 0x39, 0x34, 0x39,
                    0x35, 0x39, 0x36, 0x39, 0x37, 0x39, 0x38, 0x39, 0x39];
        #[allow(dead_code)]
        /// A 32-bit integer buffer with one extra byte
        pub type Integer32Buffer = Integer32BufferRaw<11>;
        /// A buffer for unsigned 32-bit integers with one _extra byte_ of memory reserved for
        /// adding characters. On initialization (through [`Self::init`]), your integer will be
        /// encoded and stored into the _unsafe array_
        pub struct Integer32BufferRaw<const N : usize> {
            inner_stack: Array<u8, 11>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<const N : usize> ::core::fmt::Debug for Integer32BufferRaw<N> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Integer32BufferRaw { inner_stack: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "Integer32BufferRaw");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "inner_stack", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        #[allow(dead_code)]
        impl<const N : usize> Integer32BufferRaw<N> {
            /// Initialize a buffer
            pub fn init(integer: u32) -> Self {
                let mut slf = Self { inner_stack: Array::new() };
                unsafe { slf._init_integer(integer); }
                slf
            }
            /// Initialize an integer. This is unsafe to be called outside because you'll be
            /// pushing in another integer and might end up corrupting your own stack as all
            /// pushes are unchecked!
            unsafe fn _init_integer(&mut self, mut val: u32) {
                if val < 10_000 {
                        let d1 = (val / 100) << 1;
                        let d2 = (val % 100) << 1;
                        if val >= 1000 {
                                self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                                    as usize)));
                            }
                        if val >= 100 {
                                self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                                        + 1) as usize)));
                            }
                        if val >= 10 {
                                self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                                    as usize)));
                            }
                        self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                                + 1) as usize)));
                    } else if val < 100_000_000 {
                       let b = val / 10000;
                       let c = val % 10000;
                       let d1 = (b / 100) << 1;
                       let d2 = (b % 100) << 1;
                       let d3 = (c / 100) << 1;
                       let d4 = (c % 100) << 1;
                       if val > 10_000_000 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                                   as usize)));
                           }
                       if val > 1_000_000 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                                       + 1) as usize)));
                           }
                       if val > 100_000 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                                   as usize)));
                           }
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d3
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d3
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d4
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d4
                                               + 1) as usize)));
                   } else {
                       let a = val / 100000000;
                       val %= 100000000;
                       if a >= 10 {
                               let i = a << 1;
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(i
                                                   as usize)));
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((i
                                                       + 1) as usize)));
                           } else { self.inner_stack.push_unchecked(0x30); }
                       let b = val / 10000;
                       let c = val % 10000;
                       let d1 = (b / 100) << 1;
                       let d2 = (b % 100) << 1;
                       let d3 = (c / 100) << 1;
                       let d4 = (c % 100) << 1;
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d3
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d3
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d4
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d4
                                               + 1) as usize)));
                   }
            }
            /// **This is very unsafe** Only push something when you know that the capacity won't overflow
            /// your allowance of 11 bytes. Oh no, there's no panic for you because you'll silently
            /// corrupt your own memory (or others' :/)
            pub unsafe fn push(&mut self, val: u8) {
                self.inner_stack.push_unchecked(val)
            }
        }
        impl<const N : usize> Deref for Integer32BufferRaw<N> {
            type Target = str;
            fn deref(&self) -> &Self::Target {
                unsafe { str::from_utf8_unchecked(&self.inner_stack) }
            }
        }
        impl<const N : usize> AsRef<str> for Integer32BufferRaw<N> {
            fn as_ref(&self) -> &str { self }
        }
        impl<T, const N : usize> PartialEq<T> for Integer32BufferRaw<N> where
            T: AsRef<str> {
            fn eq(&self, other_str: &T) -> bool {
                self.as_ref() == other_str.as_ref()
            }
        }
        /// A 64-bit integer buffer with **no extra byte**
        pub type Integer64 = Integer64BufferRaw<20>;
        pub struct Integer64BufferRaw<const N : usize> {
            inner_stack: Array<u8, N>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<const N : usize> ::core::fmt::Debug for Integer64BufferRaw<N> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Integer64BufferRaw { inner_stack: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "Integer64BufferRaw");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "inner_stack", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        const Z_8: u64 = 100_000_000;
        const Z_9: u64 = Z_8 * 10;
        const Z_10: u64 = Z_9 * 10;
        const Z_11: u64 = Z_10 * 10;
        const Z_12: u64 = Z_11 * 10;
        const Z_13: u64 = Z_12 * 10;
        const Z_14: u64 = Z_13 * 10;
        const Z_15: u64 = Z_14 * 10;
        const Z_16: u64 = Z_15 * 10;
        impl<const N : usize> Integer64BufferRaw<N> {
            pub fn init(integer: u64) -> Self {
                let mut slf = Self { inner_stack: Array::new() };
                unsafe { slf._init_integer(integer); }
                slf
            }
            unsafe fn _init_integer(&mut self, mut int: u64) {
                if int < Z_8 {
                        if int < 10_000 {
                                let d1 = (int / 100) << 1;
                                let d2 = (int % 100) << 1;
                                if int >= 1_000 {
                                        self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                                            as usize)));
                                    }
                                if int >= 100 {
                                        self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                                                + 1) as usize)));
                                    }
                                if int >= 10 {
                                        self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                                            as usize)));
                                    }
                                self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                                        + 1) as usize)));
                            } else {
                               let b = int / 10000;
                               let c = int % 10000;
                               let d1 = (b / 100) << 1;
                               let d2 = (b % 100) << 1;
                               let d3 = (c / 100) << 1;
                               let d4 = (c % 100) << 1;
                               if int >= 10_000_000 {
                                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                                           as usize)));
                                   }
                               if int >= 1_000_000 {
                                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                                               + 1) as usize)));
                                   }
                               if int >= 100_000 {
                                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                                           as usize)));
                                   }
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                                       + 1) as usize)));
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d3
                                                   as usize)));
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d3
                                                       + 1) as usize)));
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d4
                                                   as usize)));
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d4
                                                       + 1) as usize)));
                           }
                    } else if int < Z_16 {
                       let v0 = int / Z_8;
                       let v1 = int & Z_8;
                       let b0 = v0 / 10000;
                       let c0 = v0 % 10000;
                       let d1 = (b0 / 100) << 1;
                       let d2 = (b0 % 100) << 1;
                       let d3 = (c0 / 100) << 1;
                       let d4 = (c0 % 100) << 1;
                       let b1 = v1 / 10000;
                       let c1 = v1 % 10000;
                       let d5 = (b1 / 100) << 1;
                       let d6 = (b1 % 100) << 1;
                       let d7 = (c1 / 100) << 1;
                       let d8 = (c1 % 100) << 1;
                       if int >= Z_15 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                                   as usize)));
                           }
                       if int >= Z_14 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                                       + 1) as usize)));
                           }
                       if int >= Z_13 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                                   as usize)));
                           }
                       if int >= Z_12 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                                       + 1) as usize)));
                           }
                       if int >= Z_11 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d3
                                                   as usize)));
                           }
                       if int >= Z_10 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d3
                                                       + 1) as usize)));
                           }
                       if int >= Z_9 {
                               self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d4
                                                   as usize)));
                           }
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d4
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d5
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d5
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d6
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d6
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d7
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d7
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d8
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d8
                                               + 1) as usize)));
                   } else {
                       let a = int / Z_16;
                       int %= Z_16;
                       if a < 10 {
                               self.inner_stack.push_unchecked(0x30 + a as u8);
                           } else if a < 100 {
                              let i = a << 1;
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(i
                                                  as usize)));
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((i
                                                      + 1) as usize)));
                          } else if a < 1000 {
                              self.inner_stack.push_unchecked(0x30 + (a / 100) as u8);
                              let i = (a % 100) << 1;
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(i
                                                  as usize)));
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((i
                                                      + 1) as usize)));
                          } else {
                              let i = (a / 100) << 1;
                              let j = (a % 100) << 1;
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(i
                                                  as usize)));
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((i
                                                      + 1) as usize)));
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(j
                                                  as usize)));
                              self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((j
                                                      + 1) as usize)));
                          }
                       let v0 = int / Z_8;
                       let v1 = int % Z_8;
                       let b0 = v0 / 10000;
                       let c0 = v0 % 10000;
                       let d1 = (b0 / 100) << 1;
                       let d2 = (b0 % 100) << 1;
                       let d3 = (c0 / 100) << 1;
                       let d4 = (c0 % 100) << 1;
                       let b1 = v1 / 10000;
                       let c1 = v1 % 10000;
                       let d5 = (b1 / 100) << 1;
                       let d6 = (b1 % 100) << 1;
                       let d7 = (c1 / 100) << 1;
                       let d8 = (c1 % 100) << 1;
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d1
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d1
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d2
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d2
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d3
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d3
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d4
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d4
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d5
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d5
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d6
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d6
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d7
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d7
                                               + 1) as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add(d8
                                           as usize)));
                       self.inner_stack.push_unchecked(*(PAIR_MAP_LUT.as_ptr().add((d8
                                               + 1) as usize)));
                   }
            }
        }
        impl<const N : usize> From<usize> for Integer64BufferRaw<N> {
            fn from(val: usize) -> Self { Self::init(val as u64) }
        }
        impl<const N : usize> From<u64> for Integer64BufferRaw<N> {
            fn from(val: u64) -> Self { Self::init(val) }
        }
        impl<const N : usize> Deref for Integer64BufferRaw<N> {
            type Target = [u8];
            fn deref(&self) -> &Self::Target { &self.inner_stack }
        }
        impl<const N : usize> AsRef<str> for Integer64BufferRaw<N> {
            fn as_ref(&self) -> &str {
                unsafe { str::from_utf8_unchecked(&self.inner_stack) }
            }
        }
        impl<T, const N : usize> PartialEq<T> for Integer64BufferRaw<N> where
            T: AsRef<str> {
            fn eq(&self, other_str: &T) -> bool {
                self.as_ref() == other_str.as_ref()
            }
        }
    }
    pub mod heap_array {
        use core::{
            alloc::Layout, fmt, marker::PhantomData, mem::ManuallyDrop,
            ops::Deref, ptr, slice,
        };
        use std::alloc::dealloc;
        /// A heap-allocated array
        pub struct HeapArray<T> {
            ptr: *const T,
            len: usize,
            _marker: PhantomData<T>,
        }
        pub struct HeapArrayWriter<T> {
            base: Vec<T>,
        }
        impl<T> HeapArrayWriter<T> {
            pub fn with_capacity(cap: usize) -> Self {
                Self { base: Vec::with_capacity(cap) }
            }
            /// ## Safety
            /// Caller must ensure that `idx <= cap`. If not, you'll corrupt your
            /// memory
            pub unsafe fn write_to_index(&mut self, idx: usize, element: T) {
                if true {
                        if !(idx <= self.base.capacity()) {
                                ::core::panicking::panic("assertion failed: idx <= self.base.capacity()")
                            };
                    };
                ptr::write(self.base.as_mut_ptr().add(idx), element);
                self.base.set_len(self.base.len() + 1);
            }
            /// ## Safety
            /// This function can lead to memory unsafety in two ways:
            /// - Excess capacity: In that case, it will leak memory
            /// - Uninitialized elements: In that case, it will segfault while attempting to call
            /// `T`'s dtor
            pub unsafe fn finish(self) -> HeapArray<T> {
                let base = ManuallyDrop::new(self.base);
                HeapArray::new(base.as_ptr(), base.len())
            }
        }
        impl<T> HeapArray<T> {
            pub unsafe fn new(ptr: *const T, len: usize) -> Self {
                Self { ptr, len, _marker: PhantomData }
            }
            pub fn new_writer(cap: usize) -> HeapArrayWriter<T> {
                HeapArrayWriter::with_capacity(cap)
            }
        }
        impl<T> Drop for HeapArray<T> {
            fn drop(&mut self) {
                unsafe {
                    ptr::drop_in_place(ptr::slice_from_raw_parts_mut(self.ptr as
                                *mut T, self.len));
                    let layout = Layout::array::<T>(self.len).unwrap();
                    dealloc(self.ptr as *mut T as *mut u8, layout);
                }
            }
        }
        unsafe impl<T: Send> Send for HeapArray<T> {}
        unsafe impl<T: Sync> Sync for HeapArray<T> {}
        impl<T> Deref for HeapArray<T> {
            type Target = [T];
            fn deref(&self) -> &Self::Target {
                unsafe { slice::from_raw_parts(self.ptr, self.len) }
            }
        }
        impl<T: fmt::Debug> fmt::Debug for HeapArray<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.iter()).finish()
            }
        }
        impl<T: PartialEq> PartialEq for HeapArray<T> {
            fn eq(&self, other: &Self) -> bool { self == other }
        }
    }
    pub mod htable {
        #![allow(unused)]
        use crate::corestore::map::{
            bref::{Entry, OccupiedEntry, Ref, VacantEntry},
            iter::{BorrowedIter, OwnedIter},
            Skymap,
        };
        use ahash::RandomState;
        use bytes::Bytes;
        use std::borrow::Borrow;
        use std::hash::Hash;
        use std::iter::FromIterator;
        use std::ops::Deref;
        type HashTable<K, V> = Skymap<K, V, RandomState>;
        /// The Coremap contains the actual key/value pairs along with additional fields for data safety
        /// and protection
        pub struct Coremap<K, V> {
            pub(crate) inner: HashTable<K, V>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<K: ::core::fmt::Debug, V: ::core::fmt::Debug> ::core::fmt::Debug
            for Coremap<K, V> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Coremap { inner: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Coremap");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "inner", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl<K, V> Default for Coremap<K, V> {
            fn default() -> Self { Coremap { inner: HashTable::new_ahash() } }
        }
        impl<K: Eq + Hash, V> Coremap<K, V> {
            /// Create an empty coremap
            pub fn new() -> Self { Self::default() }
            pub fn with_capacity(cap: usize) -> Self {
                Coremap { inner: HashTable::with_capacity(cap) }
            }
            pub fn try_with_capacity(cap: usize) -> Result<Self, ()> {
                if cap > (isize::MAX as usize) {
                        Err(())
                    } else { Ok(Self::with_capacity(cap)) }
            }
            /// Returns the total number of key value pairs
            pub fn len(&self) -> usize { self.inner.len() }
            /// Clears the inner table!
            pub fn clear(&self) { self.inner.clear() }
        }
        impl<K, V> Coremap<K, V> where K: Eq + Hash {
            /// Returns the removed value for key, it it existed
            pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                self.inner.remove(key)
            }
            /// Returns true if an existent key was removed
            pub fn true_if_removed<Q>(&self, key: &Q) -> bool where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                self.inner.remove(key).is_some()
            }
            /// Check if a table contains a key
            pub fn contains_key<Q>(&self, key: &Q) -> bool where K: Borrow<Q>,
                Q: Hash + Eq + ?Sized {
                self.inner.contains_key(key)
            }
            /// Return a non-consuming iterator
            pub fn iter(&self) -> BorrowedIter<'_, K, V, RandomState> {
                self.inner.get_iter()
            }
            /// Get a reference to the value of a key, if it exists
            pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                self.inner.get(key)
            }
            /// Returns true if the non-existent key was assigned to a value
            pub fn true_if_insert(&self, k: K, v: V) -> bool {
                if let Entry::Vacant(ve) = self.inner.entry(k) {
                        ve.insert(v);
                        true
                    } else { false }
            }
            pub fn true_remove_if<Q>(&self, key: &Q,
                exec: impl FnOnce(&K, &V) -> bool) -> bool where K: Borrow<Q>,
                Q: Hash + Eq + ?Sized {
                self.remove_if(key, exec).is_some()
            }
            pub fn remove_if<Q>(&self, key: &Q,
                exec: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                self.inner.remove_if(key, exec)
            }
            /// Update or insert
            pub fn upsert(&self, k: K, v: V) {
                let _ = self.inner.insert(k, v);
            }
            /// Returns true if the value was updated
            pub fn true_if_update(&self, k: K, v: V) -> bool {
                if let Entry::Occupied(mut oe) = self.inner.entry(k) {
                        oe.insert(v);
                        true
                    } else { false }
            }
            pub fn mut_entry(&self, key: K)
                -> Option<OccupiedEntry<K, V, RandomState>> {
                if let Entry::Occupied(oe) = self.inner.entry(key) {
                        Some(oe)
                    } else { None }
            }
            pub fn fresh_entry(&self, key: K)
                -> Option<VacantEntry<K, V, RandomState>> {
                if let Entry::Vacant(ve) = self.inner.entry(key) {
                        Some(ve)
                    } else { None }
            }
        }
        impl<K: Eq + Hash, V: Clone> Coremap<K, V> {
            pub fn get_cloned<Q>(&self, key: &Q) -> Option<V> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                self.inner.get_cloned(key)
            }
        }
        impl<K: Eq + Hash + Clone, V> Coremap<K, V> {
            /// Returns atleast `count` number of keys from the hashtable
            pub fn get_keys(&self, count: usize) -> Vec<K> {
                let mut v = Vec::with_capacity(count);
                self.iter().take(count).map(|kv|
                            kv.key().clone()).for_each(|key| v.push(key));
                v
            }
        }
        impl<K: Eq + Hash, V> IntoIterator for Coremap<K, V> {
            type Item = (K, V);
            type IntoIter = OwnedIter<K, V, RandomState>;
            fn into_iter(self) -> Self::IntoIter {
                self.inner.get_owned_iter()
            }
        }
        impl Deref for Data {
            type Target = [u8];
            fn deref(&self) -> &<Self>::Target { &self.blob }
        }
        impl Borrow<[u8]> for Data {
            fn borrow(&self) -> &[u8] { self.blob.borrow() }
        }
        impl Borrow<Bytes> for Data {
            fn borrow(&self) -> &Bytes { &self.blob }
        }
        impl AsRef<[u8]> for Data {
            fn as_ref(&self) -> &[u8] { &self.blob }
        }
        impl<K, V> FromIterator<(K, V)> for Coremap<K, V> where K: Eq + Hash {
            fn from_iter<T>(iter: T) -> Self where
                T: IntoIterator<Item = (K, V)> {
                Coremap { inner: Skymap::from_iter(iter) }
            }
        }
        /// A wrapper for `Bytes`
        pub struct Data {
            /// The blob of data
            blob: Bytes,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Data {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Data { blob: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Data");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "blob",
                                &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::clone::Clone for Data {
            #[inline]
            fn clone(&self) -> Data {
                match *self {
                    Data { blob: ref __self_0_0 } =>
                        Data { blob: ::core::clone::Clone::clone(&(*__self_0_0)) },
                }
            }
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::hash::Hash for Data {
            fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
                match *self {
                    Data { blob: ref __self_0_0 } => {
                        ::core::hash::Hash::hash(&(*__self_0_0), state)
                    }
                }
            }
        }
        impl PartialEq<str> for Data {
            fn eq(&self, oth: &str) -> bool { self.blob.eq(oth) }
        }
        impl<T: AsRef<[u8]>> PartialEq<T> for Data {
            fn eq(&self, oth: &T) -> bool { self.blob.eq(oth.as_ref()) }
        }
        impl Data {
            /// Create a new blob from a string
            pub fn from_string(val: String) -> Self {
                Data { blob: Bytes::from(val.into_bytes()) }
            }
            /// Create a new blob from an existing `Bytes` instance
            pub const fn from_blob(blob: Bytes) -> Self { Data { blob } }
            /// Get the inner blob (raw `Bytes`)
            pub const fn get_blob(&self) -> &Bytes { &self.blob }
            pub fn into_inner(self) -> Bytes { self.blob }
            #[allow(clippy :: needless_lifetimes)]
            pub fn copy_from_slice<'a>(slice: &'a [u8]) -> Self {
                Self { blob: Bytes::copy_from_slice(slice) }
            }
        }
        impl Eq for Data {}
        impl<T> From<T> for Data where T: Into<Bytes> {
            fn from(dat: T) -> Self { Self { blob: dat.into() } }
        }
    }
    pub mod iarray {
        #![allow(dead_code)]
        use crate::corestore::array::LenScopeGuard;
        use core::alloc::Layout;
        use core::borrow::Borrow;
        use core::borrow::BorrowMut;
        use core::cmp;
        use core::fmt;
        use core::hash::{self, Hash};
        use core::iter::FromIterator;
        use core::mem;
        use core::mem::ManuallyDrop;
        use core::mem::MaybeUninit;
        use core::ops;
        use core::ptr;
        use core::ptr::NonNull;
        use core::slice;
        use std::alloc as std_alloc;
        pub const fn new_const_iarray<T, const N : usize>()
            -> IArray<[T; N]> {
            IArray {
                cap: 0,
                store: InlineArray {
                    stack: ManuallyDrop::new(MaybeUninit::uninit()),
                },
            }
        }
        /// An arbitrary trait used for identifying something as a contiguous block of memory
        pub trait MemoryBlock {
            /// The type that will be used for the memory layout
            type LayoutItem;
            /// The number of _units_ this memory block has
            fn size()
            -> usize;
        }
        impl<T, const N : usize> MemoryBlock for [T; N] {
            type LayoutItem = T;
            fn size() -> usize { N }
        }
        /// An union that either holds a stack (ptr) or a heap
        ///
        /// ## Safety
        /// If you're trying to access a field without knowing the most recently created one,
        /// behavior is undefined.
        pub union InlineArray<A: MemoryBlock> {
            /// the stack
            stack: ManuallyDrop<MaybeUninit<A>>,
            /// a pointer to the heap allocation and the allocation size
            heap_ptr_len: (*mut A::LayoutItem, usize),
        }
        impl<A: MemoryBlock> InlineArray<A> {
            /// Get's the stack pointer. This is unsafe because it is not guranteed that the
            /// stack pointer field is valid and the caller has to uphold this gurantee
            unsafe fn stack_ptr(&self) -> *const A::LayoutItem {
                self.stack.as_ptr() as *const _
            }
            /// Safe as `stack_ptr`, but returns a mutable pointer
            unsafe fn stack_ptr_mut(&mut self) -> *mut A::LayoutItem {
                self.stack.as_mut_ptr() as *mut _
            }
            /// Create a new union from a stack
            fn from_stack(stack: MaybeUninit<A>) -> Self {
                Self { stack: ManuallyDrop::new(stack) }
            }
            /// Create a new union from a heap (allocated).
            fn from_heap_ptr(start_ptr: *mut A::LayoutItem, len: usize)
                -> Self {
                Self { heap_ptr_len: (start_ptr, len) }
            }
            /// Returns the allocation size of the heap
            unsafe fn heap_size(&self) -> usize { self.heap_ptr_len.1 }
            /// Returns a raw ptr to the heap
            unsafe fn heap_ptr(&self) -> *const A::LayoutItem {
                self.heap_ptr_len.0
            }
            /// Returns a mut ptr to the heap
            unsafe fn heap_ptr_mut(&mut self) -> *mut A::LayoutItem {
                self.heap_ptr_len.0 as *mut _
            }
            /// Returns a mut ref to the heap allocation size
            unsafe fn heap_size_mut(&mut self) -> &mut usize {
                &mut self.heap_ptr_len.1
            }
            /// Returns the entire heap field
            unsafe fn heap(&self) -> (*mut A::LayoutItem, usize) {
                self.heap_ptr_len
            }
            /// Returns a mutable reference to the entire heap field
            unsafe fn heap_mut(&mut self)
                -> (*mut A::LayoutItem, &mut usize) {
                (self.heap_ptr_mut(), &mut self.heap_ptr_len.1)
            }
        }
        /// An utility tool for calculating the memory layout for a given `T`. Handles
        /// any possible overflows
        pub fn calculate_memory_layout<T>(count: usize)
            -> Result<Layout, ()> {
            let size = mem::size_of::<T>().checked_mul(count).ok_or(())?;
            let alignment = mem::align_of::<T>();
            Layout::from_size_align(size, alignment).map_err(|_| ())
        }
        /// Use the global allocator to deallocate the memory block for the given starting ptr
        /// upto the given capacity
        unsafe fn dealloc<T>(start_ptr: *mut T, capacity: usize) {
            std_alloc::dealloc(start_ptr as *mut u8,
                calculate_memory_layout::<T>(capacity).expect("Memory capacity overflow"))
        }
        type DataptrLenptrCapacity<T> = (*const T, usize, usize);
        type DataptrLenptrCapacityMut<'a, T> = (*mut T, &'a mut usize, usize);
        /// A stack optimized backing store
        ///
        /// An [`IArray`] is heavily optimized for storing items on the stack and will
        /// not perform very well (but of course will) when the object overflows its
        /// stack and is moved to the heap. Optimizations are made to mark overflows
        /// as branches that are unlikely to be called. The IArray is like a smallvec,
        /// but with extremely aggressive optimizations for items stored on the stack,
        /// for example to avoid the maneuvers with speculative execution.
        /// This makes the [`IArray`] extremely performant for operations on the stack,
        /// but a little expensive when operations are done on the heap
        pub struct IArray<A: MemoryBlock> {
            cap: usize,
            store: InlineArray<A>,
        }
        impl<A: MemoryBlock> IArray<A> {
            pub fn new() -> IArray<A> {
                Self {
                    cap: 0,
                    store: InlineArray::from_stack(MaybeUninit::uninit()),
                }
            }
            pub fn from_vec(mut vec: Vec<A::LayoutItem>) -> Self {
                if vec.capacity() <= Self::stack_capacity() {
                        let mut store =
                            InlineArray::<A>::from_stack(MaybeUninit::uninit());
                        let len = vec.len();
                        unsafe {
                            ptr::copy_nonoverlapping(vec.as_ptr(),
                                store.stack_ptr_mut(), len);
                        }
                        Self { cap: len, store }
                    } else {
                       let (start_ptr, cap, len) =
                           (vec.as_mut_ptr(), vec.capacity(), vec.len());
                       mem::forget(vec);
                       IArray {
                           cap,
                           store: InlineArray::from_heap_ptr(start_ptr, len),
                       }
                   }
            }
            /// Returns the total capacity of the inline stack
            fn stack_capacity() -> usize {
                if mem::size_of::<A::LayoutItem>() > 0 {
                        A::size()
                    } else { usize::MAX }
            }
            /// Helper function that returns a ptr to the data, the len and the capacity
            fn meta_triple(&self) -> DataptrLenptrCapacity<A::LayoutItem> {
                unsafe {
                    if self.went_off_stack() {
                            let (data_ptr, len_ref) = self.store.heap();
                            (data_ptr, len_ref, self.cap)
                        } else {
                           (self.store.stack_ptr(), self.cap, Self::stack_capacity())
                       }
                }
            }
            /// Mutable version of `meta_triple`
            fn meta_triple_mut(&mut self)
                -> DataptrLenptrCapacityMut<A::LayoutItem> {
                unsafe {
                    if self.went_off_stack() {
                            let (data_ptr, len_ref) = self.store.heap_mut();
                            (data_ptr, len_ref, self.cap)
                        } else {
                           (self.store.stack_ptr_mut(), &mut self.cap,
                               Self::stack_capacity())
                       }
                }
            }
            /// Returns a raw ptr to the data
            fn get_data_ptr_mut(&mut self) -> *mut A::LayoutItem {
                if self.went_off_stack() {
                        unsafe { self.store.heap_ptr_mut() }
                    } else { unsafe { self.store.stack_ptr_mut() } }
            }
            /// Returns true if the allocation is now on the heap
            fn went_off_stack(&self) -> bool {
                self.cap > Self::stack_capacity()
            }
            /// Returns the length
            pub fn len(&self) -> usize {
                if self.went_off_stack() {
                        unsafe { self.store.heap_size() }
                    } else { self.cap }
            }
            /// Returns true if the IArray is empty
            pub fn is_empty(&self) -> bool { self.len() == 0 }
            /// Returns the capacity
            fn get_capacity(&self) -> usize {
                if self.went_off_stack() {
                        self.cap
                    } else { Self::stack_capacity() }
            }
            /// Grow the allocation, if required, to make space for a total of `new_cap`
            /// elements
            fn grow_block(&mut self, new_cap: usize) {
                unsafe {
                    let (data_ptr, &mut len, cap) = self.meta_triple_mut();
                    let still_on_stack = !self.went_off_stack();
                    if !(new_cap > len) {
                            ::core::panicking::panic("assertion failed: new_cap > len")
                        };
                    if new_cap <= Self::stack_capacity() {
                            if still_on_stack { return; }
                            self.store = InlineArray::from_stack(MaybeUninit::uninit());
                            ptr::copy_nonoverlapping(data_ptr,
                                self.store.stack_ptr_mut(), len);
                            self.cap = len;
                            dealloc(data_ptr, cap);
                        } else if new_cap != cap {
                           let layout =
                               calculate_memory_layout::<A::LayoutItem>(new_cap).expect("Capacity overflow");
                           if !(layout.size() > 0) {
                                   ::core::panicking::panic("assertion failed: layout.size() > 0")
                               };
                           let new_alloc;
                           if still_on_stack {
                                   new_alloc =
                                       NonNull::new(std_alloc::alloc(layout).cast()).expect("Allocation error").as_ptr();
                                   ptr::copy_nonoverlapping(data_ptr, new_alloc, len);
                               } else {
                                  let old_layout =
                                      calculate_memory_layout::<A::LayoutItem>(cap).expect("Capacity overflow");
                                  let new_memory_block_ptr =
                                      std_alloc::realloc(data_ptr as *mut _, old_layout,
                                          layout.size());
                                  new_alloc =
                                      NonNull::new(new_memory_block_ptr.cast()).expect("Allocation error").as_ptr();
                              }
                           self.store = InlineArray::from_heap_ptr(new_alloc, len);
                           self.cap = new_cap;
                       }
                }
            }
            /// Reserve space for `additional` elements
            fn reserve(&mut self, additional: usize) {
                let (_, &mut len, cap) = self.meta_triple_mut();
                if cap - len >= additional { return; }
                let new_cap =
                    len.checked_add(additional).map(usize::next_power_of_two).expect("Capacity overflow");
                self.grow_block(new_cap)
            }
            /// Push an element into this IArray
            pub fn push(&mut self, val: A::LayoutItem) {
                unsafe {
                    let (mut data_ptr, mut len, cap) = self.meta_triple_mut();
                    if (*len).eq(&cap) {
                            self.reserve(1);
                            let (heap_ptr, heap_len) = self.store.heap_mut();
                            data_ptr = heap_ptr;
                            len = heap_len;
                        }
                    ptr::write(data_ptr.add(*len), val);
                    *len += 1;
                }
            }
            /// Pop an element off this IArray
            pub fn pop(&mut self) -> Option<A::LayoutItem> {
                unsafe {
                    let (data_ptr, len_mut, _cap) = self.meta_triple_mut();
                    if *len_mut == 0 {
                            None
                        } else {
                           let last_index = *len_mut - 1;
                           *len_mut = last_index;
                           Some(ptr::read(data_ptr.add(last_index)))
                       }
                }
            }
            /// This is amazingly dangerous if `idx` doesn't exist. You can potentially
            /// corrupt a bunch of things
            pub unsafe fn remove(&mut self, idx: usize) -> A::LayoutItem {
                let (mut ptr, len_ref, _) = self.meta_triple_mut();
                let len = *len_ref;
                *len_ref = len - 1;
                ptr = ptr.add(idx);
                let item = ptr::read(ptr);
                ptr::copy(ptr.add(1), ptr, len - idx - 1);
                item
            }
            /// Shrink this IArray so that it only occupies the required space and not anything
            /// more
            pub fn shrink(&mut self) {
                if self.went_off_stack() { return; }
                let current_len = self.len();
                if Self::stack_capacity() >= current_len {
                        unsafe {
                            let (data_ptr, len) = self.store.heap();
                            self.store = InlineArray::from_stack(MaybeUninit::uninit());
                            ptr::copy_nonoverlapping(data_ptr,
                                self.store.stack_ptr_mut(), len);
                            dealloc(data_ptr, self.cap);
                            self.cap = len;
                        }
                    } else if self.get_capacity() > current_len {
                       self.grow_block(current_len);
                   }
            }
            /// Truncate the IArray to a given length. This **will** call the destructors
            pub fn truncate(&mut self, target_len: usize) {
                unsafe {
                    let (data_ptr, len_mut, _cap) = self.meta_triple_mut();
                    while target_len < *len_mut {
                        let last_index = *len_mut - 1;
                        ptr::drop_in_place(data_ptr.add(last_index));
                        *len_mut = last_index;
                    }
                }
            }
            /// Clear the internal store
            pub fn clear(&mut self) { self.truncate(0); }
            /// Set the len, **without calling the destructor**. This is the ultimate function
            /// to make valgrind unhappy, that is, **you can create memory leaks** if you don't
            /// destroy the elements yourself
            unsafe fn set_len(&mut self, new_len: usize) {
                let (_dataptr, len_mut, _cap) = self.meta_triple_mut();
                *len_mut = new_len;
            }
        }
        impl<A: MemoryBlock> IArray<A> where A::LayoutItem: Copy {
            /// Create an IArray from a slice by copying the elements of the slice into
            /// the IArray
            pub fn from_slice(slice: &[A::LayoutItem]) -> Self {
                let slice_len = slice.len();
                if slice_len <= Self::stack_capacity() {
                        let mut new_stack = MaybeUninit::uninit();
                        unsafe {
                            ptr::copy_nonoverlapping(slice.as_ptr(),
                                new_stack.as_mut_ptr() as *mut A::LayoutItem, slice_len);
                        }
                        Self {
                            cap: slice_len,
                            store: InlineArray::from_stack(new_stack),
                        }
                    } else {
                       let mut v = slice.to_vec();
                       let (ptr, cap) = (v.as_mut_ptr(), v.capacity());
                       mem::forget(v);
                       Self {
                           cap,
                           store: InlineArray::from_heap_ptr(ptr, slice_len),
                       }
                   }
            }
            /// Insert a slice at the given index
            pub fn insert_slice_at_index(&mut self, slice: &[A::LayoutItem],
                index: usize) {
                self.reserve(slice.len());
                let len = self.len();
                if true {
                        if !(index <= len) {
                                ::core::panicking::panic("assertion failed: index <= len")
                            };
                    };
                unsafe {
                    let slice_ptr = slice.as_ptr();
                    let data_ptr_start = self.get_data_ptr_mut().add(len);
                    ptr::copy(data_ptr_start, data_ptr_start.add(slice.len()),
                        len - index);
                    ptr::copy_nonoverlapping(slice_ptr, data_ptr_start,
                        slice.len());
                    self.set_len(len + slice.len());
                }
            }
            /// Extend the IArray by using a slice
            pub fn extend_from_slice(&mut self, slice: &[A::LayoutItem]) {
                self.insert_slice_at_index(slice, self.len())
            }
            /// Create a new IArray from a pre-defined stack
            pub fn from_stack(stack: A) -> Self {
                Self {
                    cap: A::size(),
                    store: InlineArray::from_stack(MaybeUninit::new(stack)),
                }
            }
        }
        impl<A: MemoryBlock> ops::Deref for IArray<A> {
            type Target = [A::LayoutItem];
            fn deref(&self) -> &Self::Target {
                unsafe {
                    let (start_ptr, len, _) = self.meta_triple();
                    slice::from_raw_parts(start_ptr, len)
                }
            }
        }
        impl<A: MemoryBlock> ops::DerefMut for IArray<A> {
            fn deref_mut(&mut self) -> &mut [A::LayoutItem] {
                unsafe {
                    let (start_ptr, &mut len, _) = self.meta_triple_mut();
                    slice::from_raw_parts_mut(start_ptr, len)
                }
            }
        }
        impl<A: MemoryBlock> AsRef<[A::LayoutItem]> for IArray<A> {
            fn as_ref(&self) -> &[A::LayoutItem] { self }
        }
        impl<A: MemoryBlock> AsMut<[A::LayoutItem]> for IArray<A> {
            fn as_mut(&mut self) -> &mut [A::LayoutItem] { self }
        }
        impl<A: MemoryBlock> Borrow<[A::LayoutItem]> for IArray<A> {
            fn borrow(&self) -> &[A::LayoutItem] { self }
        }
        impl<A: MemoryBlock> BorrowMut<[A::LayoutItem]> for IArray<A> {
            fn borrow_mut(&mut self) -> &mut [A::LayoutItem] { self }
        }
        impl<A: MemoryBlock> Drop for IArray<A> {
            fn drop(&mut self) {
                unsafe {
                    if self.went_off_stack() {
                            let (ptr, len) = self.store.heap();
                            mem::drop(Vec::from_raw_parts(ptr, len, self.cap));
                        } else { ptr::drop_in_place(&mut self[..]); }
                }
            }
        }
        impl<A: MemoryBlock> Extend<A::LayoutItem> for IArray<A> {
            fn extend<I: IntoIterator<Item =
                A::LayoutItem>>(&mut self, iterable: I) {
                let mut iter = iterable.into_iter();
                let (lower_bound, _upper_bound) = iter.size_hint();
                self.reserve(lower_bound);
                unsafe {
                    let (data_ptr, len_ref, cap) = self.meta_triple_mut();
                    let mut len = LenScopeGuard::new(len_ref);
                    while len.get_temp() < cap {
                        if let Some(out) = iter.next() {
                                ptr::write(data_ptr.add(len.get_temp()), out);
                                len.incr(1);
                            } else { return; }
                    }
                }
                for elem in iter { self.push(elem); }
            }
        }
        impl<A: MemoryBlock> fmt::Debug for IArray<A> where
            A::LayoutItem: fmt::Debug {
            fn fmt(&self, f: &mut fmt::Formatter<'_>)
                -> Result<(), fmt::Error> {
                f.debug_list().entries(self.iter()).finish()
            }
        }
        impl<A: MemoryBlock, B: MemoryBlock> PartialEq<IArray<B>> for
            IArray<A> where A::LayoutItem: PartialEq<B::LayoutItem> {
            fn eq(&self, rhs: &IArray<B>) -> bool { self[..] == rhs[..] }
        }
        impl<A: MemoryBlock> Eq for IArray<A> where A::LayoutItem: Eq {}
        impl<A: MemoryBlock> PartialOrd for IArray<A> where
            A::LayoutItem: PartialOrd {
            fn partial_cmp(&self, rhs: &IArray<A>) -> Option<cmp::Ordering> {
                PartialOrd::partial_cmp(&**self, &**rhs)
            }
        }
        impl<A: MemoryBlock> Ord for IArray<A> where A::LayoutItem: Ord {
            fn cmp(&self, rhs: &IArray<A>) -> cmp::Ordering {
                Ord::cmp(&**self, &**rhs)
            }
        }
        impl<A: MemoryBlock> Hash for IArray<A> where A::LayoutItem: Hash {
            fn hash<H>(&self, hasher: &mut H) where H: hash::Hasher {
                (**self).hash(hasher)
            }
        }
        impl<A: MemoryBlock> FromIterator<A::LayoutItem> for IArray<A> {
            fn from_iter<I: IntoIterator<Item = A::LayoutItem>>(iter: I)
                -> Self {
                let mut iarray = IArray::new();
                iarray.extend(iter);
                iarray
            }
        }
        impl<'a, A: MemoryBlock> From<&'a [A::LayoutItem]> for IArray<A> where
            A::LayoutItem: Clone {
            fn from(slice: &'a [A::LayoutItem]) -> Self {
                slice.iter().cloned().collect()
            }
        }
        unsafe impl<A: MemoryBlock> Send for IArray<A> where
            A::LayoutItem: Send {}
        unsafe impl<A: MemoryBlock> Sync for IArray<A> where
            A::LayoutItem: Sync {}
    }
    pub mod lazy {
        use super::backoff::Backoff;
        use core::mem;
        use core::ops::Deref;
        use core::ptr;
        use core::sync::atomic::AtomicBool;
        use core::sync::atomic::AtomicPtr;
        use core::sync::atomic::Ordering;
        const ORD_ACQ: Ordering = Ordering::Acquire;
        const ORD_SEQ: Ordering = Ordering::SeqCst;
        const ORD_REL: Ordering = Ordering::Release;
        const ORD_RLX: Ordering = Ordering::Relaxed;
        /// A lazily intialized, or _call by need_ value
        pub struct Lazy<T, F> {
            /// the value (null at first)
            value: AtomicPtr<T>,
            /// the function that will init the value
            init_func: F,
            /// is some thread trying to initialize the value
            init_state: AtomicBool,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug, F: ::core::fmt::Debug> ::core::fmt::Debug
            for Lazy<T, F> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Lazy {
                        value: ref __self_0_0,
                        init_func: ref __self_0_1,
                        init_state: ref __self_0_2 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Lazy");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "value", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "init_func", &&(*__self_0_1));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "init_state", &&(*__self_0_2));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl<T, F> Lazy<T, F> {
            pub const fn new(init_func: F) -> Self {
                Self {
                    value: AtomicPtr::new(ptr::null_mut()),
                    init_func,
                    init_state: AtomicBool::new(false),
                }
            }
        }
        impl<T, F> Deref for Lazy<T, F> where F: Fn() -> T {
            type Target = T;
            fn deref(&self) -> &Self::Target {
                let value_ptr = self.value.load(ORD_ACQ);
                if !value_ptr.is_null() { unsafe { return &*value_ptr; } }
                let backoff = Backoff::new();
                while self.init_state.compare_exchange(false, true, ORD_SEQ,
                            ORD_SEQ).is_err() {
                    backoff.snooze();
                }
                let value_ptr = self.value.load(ORD_ACQ);
                if !value_ptr.is_null() {
                        if !self.init_state.swap(false, ORD_SEQ) {
                                ::core::panicking::panic("assertion failed: self.init_state.swap(false, ORD_SEQ)")
                            };
                        unsafe { &*value_ptr }
                    } else {
                       let value = (self.init_func)();
                       let value_ptr = Box::into_raw(Box::new(value));
                       if !self.value.swap(value_ptr, ORD_SEQ).is_null() {
                               ::core::panicking::panic("assertion failed: self.value.swap(value_ptr, ORD_SEQ).is_null()")
                           };
                       if !self.init_state.swap(false, ORD_SEQ) {
                               ::core::panicking::panic("assertion failed: self.init_state.swap(false, ORD_SEQ)")
                           };
                       unsafe { &*value_ptr }
                   }
            }
        }
        impl<T, F> Drop for Lazy<T, F> {
            fn drop(&mut self) {
                if mem::needs_drop::<T>() {
                        let value_ptr = self.value.load(ORD_ACQ);
                        if !value_ptr.is_null() {
                                unsafe { mem::drop(Box::from_raw(value_ptr)) }
                            }
                    }
            }
        }
        /// A "cell" that can be initialized once using a single atomic
        pub struct Once<T> {
            value: AtomicPtr<T>,
        }
        #[allow(dead_code)]
        impl<T> Once<T> {
            pub const fn new() -> Self {
                Self { value: AtomicPtr::new(ptr::null_mut()) }
            }
            pub fn with_value(val: T) -> Self {
                Self { value: AtomicPtr::new(Box::into_raw(Box::new(val))) }
            }
            pub fn get(&self) -> Option<&T> {
                let ptr = self.value.load(ORD_ACQ);
                if ptr.is_null() {
                        None
                    } else { unsafe { Some(&*self.value.load(ORD_ACQ)) } }
            }
            pub fn set(&self, val: T) -> bool {
                let snapshot = self.value.load(ORD_ACQ);
                if snapshot.is_null() {
                        let vptr = Box::into_raw(Box::new(val));
                        let r =
                            self.value.compare_exchange(snapshot, vptr, ORD_REL,
                                ORD_RLX);
                        r.is_ok()
                    } else { false }
            }
        }
        impl<T> Drop for Once<T> {
            fn drop(&mut self) {
                let snapshot = self.value.load(ORD_ACQ);
                if !snapshot.is_null() {
                        unsafe { mem::drop(Box::from_raw(snapshot)) }
                    }
            }
        }
        impl<T> From<Option<T>> for Once<T> {
            fn from(v: Option<T>) -> Self {
                match v {
                    Some(v) => Self::with_value(v),
                    None => Self::new(),
                }
            }
        }
    }
    pub mod lock {
        //! # Locks
        //!
        //! In several scenarios, we may find `std`'s or other crates' implementations of synchronization
        //! primitives to be either _too sophisticated_ or _not what we want_. For these cases, we use
        //! the primitives that are defined here
        //!
        use super::backoff::Backoff;
        use std::cell::UnsafeCell;
        use std::ops::Deref;
        use std::ops::DerefMut;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;
        const ORD_ACQUIRE: Ordering = Ordering::Acquire;
        const ORD_RELEASE: Ordering = Ordering::Release;
        /// An extremely simple lock without the extra fuss: just the raw data and an atomic bool
        pub struct QuickLock<T> {
            rawdata: UnsafeCell<T>,
            lock_state: AtomicBool,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug> ::core::fmt::Debug for QuickLock<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    QuickLock {
                        rawdata: ref __self_0_0, lock_state: ref __self_0_1 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "QuickLock");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "rawdata", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "lock_state", &&(*__self_0_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        unsafe impl<T: Send> Sync for QuickLock<T> {}
        unsafe impl<T: Send> Send for QuickLock<T> {}
        /// A lock guard created by [`QuickLock`]
        pub struct QLGuard<'a, T> {
            lck: &'a QuickLock<T>,
        }
        impl<'a, T> QLGuard<'a, T> {
            const fn init(lck: &'a QuickLock<T>) -> Self { Self { lck } }
        }
        impl<T> QuickLock<T> {
            pub const fn new(rawdata: T) -> Self {
                Self {
                    lock_state: AtomicBool::new(false),
                    rawdata: UnsafeCell::new(rawdata),
                }
            }
            /// Try to acquire a lock
            pub fn try_lock(&self) -> Option<QLGuard<'_, T>> {
                let ret =
                    self.lock_state.compare_exchange(false, true, ORD_ACQUIRE,
                        ORD_ACQUIRE);
                if ret.is_ok() { Some(QLGuard::init(self)) } else { None }
            }
            /// Enter a _busy loop_ waiting to get an unlock. Behold, this is blocking!
            pub fn lock(&self) -> QLGuard<'_, T> {
                let backoff = Backoff::new();
                loop {
                    let ret =
                        self.lock_state.compare_exchange_weak(false, true,
                            Ordering::SeqCst, Ordering::Relaxed);
                    match ret {
                        Ok(_) => break QLGuard::init(self),
                        Err(is_locked) => {
                            if !is_locked { break QLGuard::init(self); }
                        }
                    }
                    backoff.snooze();
                }
            }
        }
        impl<'a, T> Drop for QLGuard<'a, T> {
            fn drop(&mut self) {
                #[cfg(not(test))]
                let _ = self.lck.lock_state.swap(false, ORD_RELEASE);
            }
        }
        impl<'a, T> Deref for QLGuard<'a, T> {
            type Target = T;
            fn deref(&self) -> &Self::Target {
                unsafe { &*self.lck.rawdata.get() }
            }
        }
        impl<'a, T> DerefMut for QLGuard<'a, T> {
            fn deref_mut(&mut self) -> &mut T {
                unsafe { &mut *self.lck.rawdata.get() }
            }
        }
    }
    pub mod map {
        #![allow(clippy :: manual_map)]
        #![allow(unused)]
        use crate::util::compiler;
        use core::borrow::Borrow;
        use core::fmt;
        use core::hash::BuildHasher;
        use core::hash::Hash;
        use core::hash::Hasher;
        use core::iter::FromIterator;
        use core::mem;
        use parking_lot::RwLock;
        use parking_lot::RwLockReadGuard;
        use parking_lot::RwLockWriteGuard;
        use std::collections::hash_map::RandomState;
        use std::num::NonZeroUsize;
        use std::thread::available_parallelism;
        pub mod bref {
            use super::LowMap;
            use crate::util::compiler;
            use crate::util::Unwrappable;
            use core::hash::BuildHasher;
            use core::hash::Hash;
            use core::mem;
            use core::ops::Deref;
            use core::ops::DerefMut;
            use parking_lot::RwLockReadGuard;
            use parking_lot::RwLockWriteGuard;
            use std::collections::hash_map::RandomState;
            use std::sync::Arc;
            /// A read-only reference to a bucket
            pub struct Ref<'a, K, V> {
                _g: RwLockReadGuard<'a, LowMap<K, V>>,
                k: &'a K,
                v: &'a V,
            }
            impl<'a, K, V> Ref<'a, K, V> {
                /// Create a new reference
                pub(super) const fn new(_g: RwLockReadGuard<'a, LowMap<K, V>>,
                    k: &'a K, v: &'a V) -> Self {
                    Self { _g, k, v }
                }
                /// Get a ref to the key
                pub const fn key(&self) -> &K { self.k }
                /// Get a ref to the value
                pub const fn value(&self) -> &V { self.v }
            }
            impl<'a, K, V> Deref for Ref<'a, K, V> {
                type Target = V;
                fn deref(&self) -> &Self::Target { self.value() }
            }
            unsafe impl<'a, K: Send, V: Send> Send for Ref<'a, K, V> {}
            unsafe impl<'a, K: Sync, V: Sync> Sync for Ref<'a, K, V> {}
            /// A r/w ref to a bucket
            pub struct RefMut<'a, K, V> {
                _g: RwLockWriteGuard<'a, LowMap<K, V>>,
                _k: &'a K,
                v: &'a mut V,
            }
            impl<'a, K, V> RefMut<'a, K, V> {
                /// Create a new ref
                pub(super) fn new(_g: RwLockWriteGuard<'a, LowMap<K, V>>,
                    k: &'a K, v: &'a mut V) -> Self {
                    Self { _g, _k: k, v }
                }
                /// Get a ref to the value
                pub const fn value(&self) -> &V { self.v }
                /// Get a mutable ref to the value
                pub fn value_mut(&mut self) -> &mut V { self.v }
            }
            impl<'a, K, V> Deref for RefMut<'a, K, V> {
                type Target = V;
                fn deref(&self) -> &Self::Target { self.value() }
            }
            impl<'a, K, V> DerefMut for RefMut<'a, K, V> {
                fn deref_mut(&mut self) -> &mut V { self.value_mut() }
            }
            unsafe impl<'a, K: Send, V: Send> Send for RefMut<'a, K, V> {}
            unsafe impl<'a, K: Sync, V: Sync> Sync for RefMut<'a, K, V> {}
            /// A reference to an occupied entry
            pub struct OccupiedEntry<'a, K, V, S> {
                guard: RwLockWriteGuard<'a, LowMap<K, V>>,
                elem: (&'a K, &'a mut V),
                key: K,
                hasher: S,
            }
            impl<'a, K: Hash + Eq, V, S: BuildHasher>
                OccupiedEntry<'a, K, V, S> {
                /// Create a new occupied entry ref
                pub(super) fn new(guard: RwLockWriteGuard<'a, LowMap<K, V>>,
                    key: K, elem: (&'a K, &'a mut V), hasher: S) -> Self {
                    Self { guard, elem, key, hasher }
                }
                /// Get a ref to the value
                pub fn value(&self) -> &V { self.elem.1 }
                /// Insert a value into this bucket
                pub fn insert(&mut self, other: V) -> V {
                    mem::replace(self.elem.1, other)
                }
                /// Remove this element from the map
                pub fn remove(mut self) -> V {
                    let hash =
                        super::make_hash::<K, _, S>(&self.hasher, &self.key);
                    unsafe {
                            self.guard.remove_entry(hash,
                                    super::ceq(self.elem.0)).unsafe_unwrap()
                        }.1
                }
            }
            unsafe impl<'a, K: Send, V: Send, S> Send for
                OccupiedEntry<'a, K, V, S> {}
            unsafe impl<'a, K: Sync, V: Sync, S> Sync for
                OccupiedEntry<'a, K, V, S> {}
            /// A ref to a vacant entry
            pub struct VacantEntry<'a, K, V, S> {
                guard: RwLockWriteGuard<'a, LowMap<K, V>>,
                key: K,
                hasher: S,
            }
            impl<'a, K: Hash + Eq, V, S: BuildHasher> VacantEntry<'a, K, V, S>
                {
                /// Create a vacant entry ref
                pub(super) fn new(guard: RwLockWriteGuard<'a, LowMap<K, V>>,
                    key: K, hasher: S) -> Self {
                    Self { guard, key, hasher }
                }
                /// Insert a value into this bucket
                pub fn insert(mut self, value: V) -> RefMut<'a, K, V> {
                    unsafe {
                        let hash =
                            super::make_insert_hash::<K, S>(&self.hasher, &self.key);
                        let &mut (ref mut k, ref mut v) =
                            self.guard.insert_entry(hash, (self.key, value),
                                super::make_hasher::<K, _, V, S>(&self.hasher));
                        let kptr = compiler::extend_lifetime(k);
                        let vptr = compiler::extend_lifetime_mut(v);
                        RefMut::new(self.guard, kptr, vptr)
                    }
                }
            }
            /// An entry, either occupied or vacant
            pub enum Entry<'a, K, V, S = RandomState> {
                Occupied(OccupiedEntry<'a, K, V, S>),
                Vacant(VacantEntry<'a, K, V, S>),
            }
            /// A shared ref to a key
            pub struct RefMulti<'a, K, V> {
                _g: Arc<RwLockReadGuard<'a, LowMap<K, V>>>,
                k: &'a K,
                v: &'a V,
            }
            impl<'a, K, V> RefMulti<'a, K, V> {
                /// Create a new shared ref
                pub const fn new(_g: Arc<RwLockReadGuard<'a, LowMap<K, V>>>,
                    k: &'a K, v: &'a V) -> Self {
                    Self { _g, k, v }
                }
                /// Get a ref to the key
                pub const fn key(&self) -> &K { self.k }
                /// Get a ref to the value
                pub const fn value(&self) -> &V { self.v }
            }
            impl<'a, K, V> Deref for RefMulti<'a, K, V> {
                type Target = V;
                fn deref(&self) -> &Self::Target { self.value() }
            }
            unsafe impl<'a, K: Sync, V: Sync> Sync for RefMulti<'a, K, V> {}
            unsafe impl<'a, K: Send, V: Send> Send for RefMulti<'a, K, V> {}
            /// A shared r/w ref to a bucket
            pub struct RefMultiMut<'a, K, V> {
                _g: Arc<RwLockWriteGuard<'a, LowMap<K, V>>>,
                _k: &'a K,
                v: &'a mut V,
            }
            impl<'a, K, V> RefMultiMut<'a, K, V> {
                /// Create a new shared r/w ref
                pub fn new(_g: Arc<RwLockWriteGuard<'a, LowMap<K, V>>>,
                    k: &'a K, v: &'a mut V) -> Self {
                    Self { _g, _k: k, v }
                }
                /// Get a ref to the value
                pub const fn value(&self) -> &V { self.v }
                /// Get a mutable ref to the value
                pub fn value_mut(&mut self) -> &mut V { self.v }
            }
            impl<'a, K, V> Deref for RefMultiMut<'a, K, V> {
                type Target = V;
                fn deref(&self) -> &Self::Target { self.value() }
            }
            impl<'a, K, V> DerefMut for RefMultiMut<'a, K, V> {
                fn deref_mut(&mut self) -> &mut V { self.value_mut() }
            }
            unsafe impl<'a, K: Sync, V: Sync> Sync for RefMultiMut<'a, K, V>
                {}
            unsafe impl<'a, K: Send, V: Send> Send for RefMultiMut<'a, K, V>
                {}
        }
        use iter::{BorrowedIter, OwnedIter};
        pub mod iter {
            use super::bref::RefMulti;
            use super::LowMap;
            use super::Skymap;
            use core::mem;
            use hashbrown::raw::RawIntoIter;
            use hashbrown::raw::RawIter;
            use parking_lot::RwLockReadGuard;
            use std::collections::hash_map::RandomState;
            use std::sync::Arc;
            /// An owned iterator for a [`Skymap`]
            pub struct OwnedIter<K, V, S = RandomState> {
                map: Skymap<K, V, S>,
                cs: usize,
                current: Option<RawIntoIter<(K, V)>>,
            }
            impl<K, V, S> OwnedIter<K, V, S> {
                pub fn new(map: Skymap<K, V, S>) -> Self {
                    Self { map, cs: 0usize, current: None }
                }
            }
            impl<K, V, S> Iterator for OwnedIter<K, V, S> {
                type Item = (K, V);
                fn next(&mut self) -> Option<Self::Item> {
                    loop {
                        if let Some(current) = self.current.as_mut() {
                                if let Some(bucket) = current.next() {
                                        return Some(bucket);
                                    }
                            }
                        if self.cs == self.map.shards().len() { return None; }
                        let mut wshard =
                            unsafe { self.map.get_wshard_unchecked(self.cs) };
                        let current_map = mem::replace(&mut *wshard, LowMap::new());
                        drop(wshard);
                        let iter = current_map.into_iter();
                        self.current = Some(iter);
                        self.cs += 1;
                    }
                }
            }
            unsafe impl<K: Send, V: Send, S> Send for OwnedIter<K, V, S> {}
            unsafe impl<K: Sync, V: Sync, S> Sync for OwnedIter<K, V, S> {}
            type BorrowedIterGroup<'a, K, V> =
                (RawIter<(K, V)>, Arc<RwLockReadGuard<'a, LowMap<K, V>>>);
            /// A borrowed iterator for a [`Skymap`]
            pub struct BorrowedIter<'a, K, V, S = ahash::RandomState> {
                map: &'a Skymap<K, V, S>,
                cs: usize,
                citer: Option<BorrowedIterGroup<'a, K, V>>,
            }
            impl<'a, K, V, S> BorrowedIter<'a, K, V, S> {
                pub const fn new(map: &'a Skymap<K, V, S>) -> Self {
                    Self { map, cs: 0usize, citer: None }
                }
            }
            impl<'a, K, V, S> Iterator for BorrowedIter<'a, K, V, S> {
                type Item = RefMulti<'a, K, V>;
                fn next(&mut self) -> Option<Self::Item> {
                    loop {
                        if let Some(current) = self.citer.as_mut() {
                                if let Some(bucket) = current.0.next() {
                                        let (kptr, vptr) = unsafe { bucket.as_ref() };
                                        let guard = current.1.clone();
                                        return Some(RefMulti::new(guard, kptr, vptr));
                                    }
                            }
                        if self.cs == self.map.shards().len() { return None; }
                        let rshard =
                            unsafe { self.map.get_rshard_unchecked(self.cs) };
                        let iter = unsafe { rshard.iter() };
                        self.citer = Some((iter, Arc::new(rshard)));
                        self.cs += 1;
                    }
                }
            }
            unsafe impl<'a, K: Send, V: Send, S> Send for
                BorrowedIter<'a, K, V, S> {}
            unsafe impl<'a, K: Sync, V: Sync, S> Sync for
                BorrowedIter<'a, K, V, S> {}
        }
        use bref::{Entry, OccupiedEntry, Ref, RefMut, VacantEntry};
        type LowMap<K, V> = hashbrown::raw::RawTable<(K, V)>;
        type ShardSlice<K, V> = [RwLock<LowMap<K, V>>];
        type SRlock<'a, K, V> =
            RwLockReadGuard<'a, hashbrown::raw::RawTable<(K, V)>>;
        type SWlock<'a, K, V> =
            RwLockWriteGuard<'a, hashbrown::raw::RawTable<(K, V)>>;
        const BITS_IN_USIZE: usize = mem::size_of::<usize>() * 8;
        const DEFAULT_CAP: usize = 128;
        fn make_hash<K, Q, S>(hash_builder: &S, val: &Q) -> u64 where
            K: Borrow<Q>, Q: Hash + ?Sized, S: BuildHasher {
            let mut state = hash_builder.build_hasher();
            val.hash(&mut state);
            state.finish()
        }
        fn make_insert_hash<K, S>(hash_builder: &S, val: &K) -> u64 where
            K: Hash, S: BuildHasher {
            let mut state = hash_builder.build_hasher();
            val.hash(&mut state);
            state.finish()
        }
        fn make_hasher<K, Q, V, S>(hash_builder: &S)
            -> impl Fn(&(Q, V)) -> u64 + '_ where K: Borrow<Q>, Q: Hash,
            S: BuildHasher {
            move |val| make_hash::<K, Q, S>(hash_builder, &val.0)
        }
        fn ceq<Q, K, V>(k: &Q) -> impl Fn(&(K, V)) -> bool + '_ where
            K: Borrow<Q>, Q: ?Sized + Eq {
            move |x| k.eq(x.0.borrow())
        }
        fn get_shard_count() -> usize {
            (available_parallelism().map_or(1, usize::from) *
                        16).next_power_of_two()
        }
        const fn cttz(amount: usize) -> usize {
            amount.trailing_zeros() as usize
        }
        /// A striped in-memory map
        pub struct Skymap<K, V, S = RandomState> {
            shards: Box<ShardSlice<K, V>>,
            hasher: S,
            shift: usize,
        }
        impl<K, V> Default for Skymap<K, V, RandomState> {
            fn default() -> Self { Self::with_hasher(RandomState::default()) }
        }
        impl<K: fmt::Debug, V: fmt::Debug, S: BuildHasher + Default>
            fmt::Debug for Skymap<K, V, S> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut map = f.debug_map();
                for s in self.get_iter() { map.entry(s.key(), s.value()); }
                map.finish()
            }
        }
        impl<K, V, S> FromIterator<(K, V)> for Skymap<K, V, S> where K: Eq +
            Hash, S: BuildHasher + Default + Clone {
            fn from_iter<T>(iter: T) -> Self where
                T: IntoIterator<Item = (K, V)> {
                let map = Skymap::new();
                iter.into_iter().for_each(|(k, v)|
                        { let _ = map.insert(k, v); });
                map
            }
        }
        impl<K, V> Skymap<K, V, ahash::RandomState> {
            /// Get a Skymap with the ahash hasher
            pub fn new_ahash() -> Self { Skymap::new() }
        }
        impl<K, V, S> Skymap<K, V, S> where S: BuildHasher + Default {
            /// Create a new Skymap with the default state (or seed) of the hasher
            pub fn new() -> Self { Self::with_hasher(S::default()) }
            /// Create a new Skymap with the provided capacity
            pub fn with_capacity(cap: usize) -> Self {
                Self::with_capacity_and_hasher(cap, S::default())
            }
            /// Create a new Skymap with the provided cap and hasher
            pub fn with_capacity_and_hasher(mut cap: usize, hasher: S)
                -> Self {
                let shard_count = get_shard_count();
                let shift = BITS_IN_USIZE - cttz(shard_count);
                if cap != 0 {
                        cap = (cap + (shard_count - 1)) & !(shard_count - 1);
                    }
                let cap_per_shard = cap / shard_count;
                Self {
                    shards: (0..shard_count).map(|_|
                                RwLock::new(LowMap::with_capacity(cap_per_shard))).collect(),
                    hasher,
                    shift,
                }
            }
            /// Create a new Skymap with the provided hasher
            pub fn with_hasher(hasher: S) -> Self {
                Self::with_capacity_and_hasher(DEFAULT_CAP, hasher)
            }
            /// Get the len of the Skymap
            pub fn len(&self) -> usize {
                self.shards.iter().map(|s| s.read().len()).sum()
            }
            /// Get the capacity of the Skymap
            pub fn capacity(&self) -> usize {
                self.shards.iter().map(|s| s.read().capacity()).sum()
            }
            /// Check if the Skymap is empty
            pub fn is_empty(&self) -> bool { self.len() == 0 }
            /// Get a borrowed iterator for the Skymap. Bound to the lifetime
            pub fn get_iter(&self) -> BorrowedIter<K, V, S> {
                BorrowedIter::new(self)
            }
            /// Get an owned iterator to the Skymap
            pub fn get_owned_iter(self) -> OwnedIter<K, V, S> {
                OwnedIter::new(self)
            }
        }
        impl<K, V, S> Skymap<K, V, S> {
            /// Get a ref to the stripes
            const fn shards(&self) -> &ShardSlice<K, V> { &self.shards }
            /// Determine the shard
            const fn determine_shard(&self, hash: usize) -> usize {
                (hash << 7) >> self.shift
            }
            /// Get a ref to the underlying hasher
            const fn h(&self) -> &S { &self.hasher }
        }
        impl<K, V, S> Skymap<K, V, S> where K: Eq + Hash, S: BuildHasher +
            Clone {
            /// Insert a key/value into the Skymap
            pub fn insert(&self, k: K, v: V) -> Option<V> {
                let hash = make_insert_hash::<K, S>(&self.hasher, &k);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let mut lowtable = self.get_wshard_unchecked(idx);
                    if let Some((_, item)) = lowtable.get_mut(hash, ceq(&k)) {
                            Some(mem::replace(item, v))
                        } else {
                           lowtable.insert(hash, (k, v),
                               make_hasher::<K, _, V, S>(self.h()));
                           None
                       }
                }
            }
            /// Remove a key/value from the Skymap
            pub fn remove<Q>(&self, k: &Q) -> Option<(K, V)> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                let hash = make_hash::<K, Q, S>(self.h(), k);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let mut lowtable = self.get_wshard_unchecked(idx);
                    match lowtable.remove_entry(hash, ceq(k)) {
                        Some(kv) => Some(kv),
                        None => None,
                    }
                }
            }
            /// Remove a key/value from the Skymap if it satisfies a certain condition
            pub fn remove_if<Q>(&self, k: &Q, f: impl FnOnce(&K, &V) -> bool)
                -> Option<(K, V)> where K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                let hash = make_hash::<K, Q, S>(self.h(), k);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let mut lowtable = self.get_wshard_unchecked(idx);
                    match lowtable.find(hash, ceq(k)) {
                        Some(bucket) => {
                            let (kptr, vptr) = bucket.as_ref();
                            if f(kptr, vptr) {
                                    Some(lowtable.remove(bucket))
                                } else { None }
                        }
                        None => None,
                    }
                }
            }
        }
        impl<'a, K: 'a + Hash + Eq, V: 'a, S: BuildHasher + Clone>
            Skymap<K, V, S> {
            /// Get a ref to an entry in the Skymap
            pub fn get<Q>(&'a self, k: &Q) -> Option<Ref<'a, K, V>> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                let hash = make_hash::<K, Q, S>(self.h(), k);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let lowtable = self.get_rshard_unchecked(idx);
                    match lowtable.get(hash, ceq(k)) {
                        Some((ref kptr, ref vptr)) => {
                            let kptr = compiler::extend_lifetime(kptr);
                            let vptr = compiler::extend_lifetime(vptr);
                            Some(Ref::new(lowtable, kptr, vptr))
                        }
                        None => None,
                    }
                }
            }
            /// Get a mutable ref to an entry in the Skymap
            pub fn get_mut<Q>(&'a self, k: &Q) -> Option<RefMut<'a, K, V>>
                where K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                let hash = make_hash::<K, Q, S>(self.h(), k);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let mut lowtable = self.get_wshard_unchecked(idx);
                    match lowtable.get_mut(hash, ceq(k)) {
                        Some(&mut (ref kptr, ref mut vptr)) => {
                            let kptr = compiler::extend_lifetime(kptr);
                            let vptr = compiler::extend_lifetime_mut(vptr);
                            Some(RefMut::new(lowtable, kptr, vptr))
                        }
                        None => None,
                    }
                }
            }
            /// Get an entry for in-place mutation
            pub fn entry(&'a self, key: K) -> Entry<'a, K, V, S> {
                let hash = make_insert_hash::<K, S>(self.h(), &key);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let lowtable = self.get_wshard_unchecked(idx);
                    if let Some(elem) = lowtable.find(hash, ceq(&key)) {
                            let (kptr, vptr) = elem.as_mut();
                            let kptr = compiler::extend_lifetime(kptr);
                            let vptr = compiler::extend_lifetime_mut(vptr);
                            Entry::Occupied(OccupiedEntry::new(lowtable, key,
                                    (kptr, vptr), self.hasher.clone()))
                        } else {
                           Entry::Vacant(VacantEntry::new(lowtable, key,
                                   self.hasher.clone()))
                       }
                }
            }
            /// Check if the Skymap contains the provided key
            pub fn contains_key<Q>(&self, key: &Q) -> bool where K: Borrow<Q>,
                Q: Hash + Eq + ?Sized {
                self.get(key).is_some()
            }
            /// Clear out all the entries in the Skymap
            pub fn clear(&self) {
                self.shards().iter().for_each(|shard| shard.write().clear())
            }
        }
        impl<'a, K, V: Clone, S: BuildHasher> Skymap<K, V, S> {
            pub fn get_cloned<Q>(&'a self, k: &Q) -> Option<V> where
                K: Borrow<Q>, Q: Hash + Eq + ?Sized {
                let hash = make_hash::<K, Q, S>(self.h(), k);
                let idx = self.determine_shard(hash as usize);
                unsafe {
                    let lowtable = self.get_rshard_unchecked(idx);
                    match lowtable.get(hash, ceq(k)) {
                        Some((_kptr, ref vptr)) => Some(vptr.clone()),
                        None => None,
                    }
                }
            }
        }
        impl<'a, K: 'a, V: 'a, S> Skymap<K, V, S> {
            /// Get a rlock to a certain stripe
            unsafe fn get_rshard_unchecked(&'a self, shard: usize)
                -> SRlock<'a, K, V> {
                (*(self.shards.as_ptr().add(shard as usize))).read()
            }
            /// Get a wlock to a certain stripe
            unsafe fn get_wshard_unchecked(&'a self, shard: usize)
                -> SWlock<'a, K, V> {
                (*(self.shards.as_ptr().add(shard as usize))).write()
            }
        }
    }
    pub mod memstore {
        //! # In-memory store
        //!
        //! This is what things look like:
        //! ```text
        //! -----------------------------------------------------
        //! |                                                   |
        //! |  |-------------------|     |-------------------|  |
        //! |  |-------------------|     |-------------------|  |
        //! |  | | TABLE | TABLE | |     | | TABLE | TABLE | |  |
        //! |  | |-------|-------| |     | |-------|-------| |  |
        //! |  |      Keyspace     |     |      Keyspace     |  |
        //! |  |-------------------|     |-------------------|  |
        //!                                                     |
        //! |  |-------------------|     |-------------------|  |
        //! |  | |-------|-------| |     | |-------|-------| |  |
        //! |  | | TABLE | TABLE | |     | | TABLE | TABLE | |  |
        //! |  | |-------|-------| |     | |-------|-------| |  |
        //! |  |      Keyspace     |     |      Keyspace     |  |
        //! |  |-------------------|     |-------------------|  |
        //! |                                                   |
        //! |                                                   |
        //! |                                                   |
        //! -----------------------------------------------------
        //! |                         NODE                      |
        //! |---------------------------------------------------|
        //! ```
        //!
        //! So, all your data is at the mercy of [`Memstore`]'s constructor
        //! and destructor.
        use super::KeyspaceResult;
        use crate::auth::Authmap;
        use crate::corestore::array::Array;
        use crate::corestore::htable::Coremap;
        use crate::corestore::table::Table;
        use crate::corestore::table::{SystemDataModel, SystemTable};
        use crate::registry;
        use crate::util::Wrapper;
        use core::borrow::Borrow;
        use core::hash::Hash;
        use std::sync::Arc;
        const DEFAULT_ARRAY: [::core::mem::MaybeUninit<u8>; 64] =
            {
                let mut ret = [::core::mem::MaybeUninit::uninit(); 64];
                let mut idx = 0;
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'd');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'e');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'f');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'a');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'u');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'l');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b't');
                ret
            };
        const SYSTEM_ARRAY: [::core::mem::MaybeUninit<u8>; 64] =
            {
                let mut ret = [::core::mem::MaybeUninit::uninit(); 64];
                let mut idx = 0;
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b's');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'y');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b's');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b't');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'e');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'm');
                ret
            };
        const SYSTEM_AUTH_ARRAY: [::core::mem::MaybeUninit<u8>; 64] =
            {
                let mut ret = [::core::mem::MaybeUninit::uninit(); 64];
                let mut idx = 0;
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'a');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'u');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b't');
                idx += 1;
                ret[idx - 1] = ::core::mem::MaybeUninit::new(b'h');
                ret
            };
        /// typedef for the keyspace/table IDs. We don't need too much fancy here,
        /// no atomic pointers and all. Just a nice array. With amazing gurantees
        pub type ObjectID = Array<u8, 64>;
        /// The `DEFAULT` array (with the rest uninit)
        pub const DEFAULT: ObjectID =
            unsafe { Array::from_const(DEFAULT_ARRAY, 7) };
        pub const SYSTEM: ObjectID =
            unsafe { Array::from_const(SYSTEM_ARRAY, 6) };
        pub const AUTH: ObjectID =
            unsafe { Array::from_const(SYSTEM_AUTH_ARRAY, 4) };
        mod cluster {
            /// This is for the future where every node will be allocated a shard
            pub enum ClusterShardRange { SingleNode, }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for ClusterShardRange {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match (&*self,) {
                        (&ClusterShardRange::SingleNode,) => {
                            ::core::fmt::Formatter::write_str(f, "SingleNode")
                        }
                    }
                }
            }
            impl Default for ClusterShardRange {
                fn default() -> Self { Self::SingleNode }
            }
            /// This is for the future for determining the replication strategy
            pub enum ReplicationStrategy {

                /// Single node, no replica sets
                Default,
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for ReplicationStrategy {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match (&*self,) {
                        (&ReplicationStrategy::Default,) => {
                            ::core::fmt::Formatter::write_str(f, "Default")
                        }
                    }
                }
            }
            impl Default for ReplicationStrategy {
                fn default() -> Self { Self::Default }
            }
        }
        /// Errors arising from trying to modify/access containers
        #[allow(dead_code)]
        pub enum DdlError {

            /// The object is still in use
            StillInUse,

            /// The object couldn't be found
            ObjectNotFound,

            /// The object is not user-accessible
            ProtectedObject,

            /// The default object wasn't found
            DefaultNotFound,

            /// Incorrect data model semantics were used on a data model
            WrongModel,

            /// The object already exists
            AlreadyExists,

            /// The target object is not ready
            NotReady,

            /// The target object is not empty
            NotEmpty,

            /// The DDL transaction failed
            DdlTransactionFailure,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        #[allow(dead_code)]
        impl ::core::fmt::Debug for DdlError {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&DdlError::StillInUse,) => {
                        ::core::fmt::Formatter::write_str(f, "StillInUse")
                    }
                    (&DdlError::ObjectNotFound,) => {
                        ::core::fmt::Formatter::write_str(f, "ObjectNotFound")
                    }
                    (&DdlError::ProtectedObject,) => {
                        ::core::fmt::Formatter::write_str(f, "ProtectedObject")
                    }
                    (&DdlError::DefaultNotFound,) => {
                        ::core::fmt::Formatter::write_str(f, "DefaultNotFound")
                    }
                    (&DdlError::WrongModel,) => {
                        ::core::fmt::Formatter::write_str(f, "WrongModel")
                    }
                    (&DdlError::AlreadyExists,) => {
                        ::core::fmt::Formatter::write_str(f, "AlreadyExists")
                    }
                    (&DdlError::NotReady,) => {
                        ::core::fmt::Formatter::write_str(f, "NotReady")
                    }
                    (&DdlError::NotEmpty,) => {
                        ::core::fmt::Formatter::write_str(f, "NotEmpty")
                    }
                    (&DdlError::DdlTransactionFailure,) => {
                        ::core::fmt::Formatter::write_str(f,
                            "DdlTransactionFailure")
                    }
                }
            }
        }
        #[allow(dead_code)]
        impl ::core::marker::StructuralPartialEq for DdlError { }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        #[allow(dead_code)]
        impl ::core::cmp::PartialEq for DdlError {
            #[inline]
            fn eq(&self, other: &DdlError) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) { _ => true, }
                        } else { false }
                }
            }
        }
        /// The core in-memory table
        ///
        /// This in-memory table that houses all keyspaces along with other node properties.
        /// This is the structure that you should clone in an atomic RC wrapper. This object
        /// handles no sort of persistence
        pub struct Memstore {
            /// the keyspaces
            pub keyspaces: Coremap<ObjectID, Arc<Keyspace>>,
            /// the system keyspace with the system tables
            pub system: SystemKeyspace,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Memstore {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Memstore { keyspaces: ref __self_0_0, system: ref __self_0_1
                        } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Memstore");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "keyspaces", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "system", &&(*__self_0_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl Memstore {
            pub fn init_with_all(keyspaces: Coremap<ObjectID, Arc<Keyspace>>,
                system: SystemKeyspace) -> Self {
                Self { keyspaces, system }
            }
            /// Create a new in-memory table with the default keyspace and the default
            /// tables. So, whenever you're calling this, this is what you get:
            /// ```json
            /// "YOURNODE": {
            ///     "KEYSPACES": [
            ///         "default" : {
            ///             "TABLES": ["default"]
            ///         },
            ///         "system": {
            ///             "TABLES": []
            ///         }
            ///     ]
            /// }
            /// ```
            ///
            /// When you connect a client without any information about the keyspace you're planning to
            /// use, you'll be connected to `ks:default/table:default`. The `ks:default/table:_system` is not
            /// for you. It's for the system
            pub fn new_default() -> Self {
                Self {
                    keyspaces: {
                        let n = Coremap::new();
                        n.true_if_insert(DEFAULT,
                            Arc::new(Keyspace::empty_default()));
                        n.true_if_insert(SYSTEM, Arc::new(Keyspace::empty()));
                        n
                    },
                    system: SystemKeyspace::new(Coremap::new()),
                }
            }
            pub fn setup_auth(&self) -> Authmap {
                match self.system.tables.fresh_entry(AUTH) {
                    Some(fresh) => {
                        let r = Authmap::default();
                        fresh.insert(Wrapper::new(SystemTable::new_auth(r.clone())));
                        r
                    }
                    None =>
                        match self.system.tables.get(&AUTH).unwrap().data {
                            SystemDataModel::Auth(ref am) =>
                                am.clone(),
                                #[allow(unreachable_patterns)]
                                _ => unsafe {
                                core::hint::unreachable_unchecked()
                            },
                        },
                }
            }
            /// Get an atomic reference to a keyspace
            pub fn get_keyspace_atomic_ref<Q>(&self, keyspace_identifier: &Q)
                -> Option<Arc<Keyspace>> where ObjectID: Borrow<Q>, Q: Hash +
                Eq + ?Sized {
                self.keyspaces.get(keyspace_identifier).map(|ns| ns.clone())
            }
            /// Returns true if a new keyspace was created
            pub fn create_keyspace(&self, keyspace_identifier: ObjectID)
                -> bool {
                self.keyspaces.true_if_insert(keyspace_identifier,
                    Arc::new(Keyspace::empty()))
            }
            /// Drop a keyspace only if it is empty and has no clients connected to it
            ///
            /// The invariants maintained here are:
            /// 1. The keyspace is not referenced to
            /// 2. There are no tables in the keyspace
            ///
            /// **Trip switch handled:** Yes
            pub fn drop_keyspace(&self, ksid: ObjectID)
                -> KeyspaceResult<()> {
                if ksid.eq(&SYSTEM) || ksid.eq(&DEFAULT) {
                        Err(DdlError::ProtectedObject)
                    } else if !self.keyspaces.contains_key(&ksid) {
                       Err(DdlError::ObjectNotFound)
                   } else {
                       let removed_keyspace = self.keyspaces.mut_entry(ksid);
                       match removed_keyspace {
                           Some(ks) => {
                               let no_one_is_using_keyspace =
                                   Arc::strong_count(ks.value()) == 1;
                               let no_tables_are_in_keyspace =
                                   ks.value().table_count() == 0;
                               if no_one_is_using_keyspace && no_tables_are_in_keyspace {
                                       ks.remove();
                                       registry::get_preload_tripswitch().trip();
                                       registry::get_cleanup_tripswitch().trip();
                                       Ok(())
                                   } else if !no_tables_are_in_keyspace {
                                      Err(DdlError::NotEmpty)
                                  } else { Err(DdlError::StillInUse) }
                           }
                           None => Err(DdlError::ObjectNotFound),
                       }
                   }
            }
            /// Force remove a keyspace along with all its tables. This force however only
            /// removes tables if they aren't in use and iff the keyspace is not currently
            /// in use to avoid the problem of having "ghost tables"
            ///
            /// The invariants maintained here are:
            /// 1. The keyspace is not referenced to
            /// 2. The tables in the keyspace are not referenced to
            ///
            /// **Trip switch handled:** Yes
            pub fn force_drop_keyspace(&self, ksid: ObjectID)
                -> KeyspaceResult<()> {
                if ksid.eq(&SYSTEM) || ksid.eq(&DEFAULT) {
                        Err(DdlError::ProtectedObject)
                    } else if !self.keyspaces.contains_key(&ksid) {
                       Err(DdlError::ObjectNotFound)
                   } else {
                       let removed_keyspace = self.keyspaces.mut_entry(ksid);
                       match removed_keyspace {
                           Some(keyspace) => {
                               let no_tables_in_use =
                                   Arc::strong_count(keyspace.value()) == 1 &&
                                       keyspace.value().tables.iter().all(|table|
                                               Arc::strong_count(table.value()) == 1);
                               if no_tables_in_use {
                                       keyspace.remove();
                                       registry::get_preload_tripswitch().trip();
                                       registry::get_cleanup_tripswitch().trip();
                                       Ok(())
                                   } else { Err(DdlError::StillInUse) }
                           }
                           None => Err(DdlError::ObjectNotFound),
                       }
                   }
            }
        }
        /// System keyspace
        pub struct SystemKeyspace {
            pub tables: Coremap<ObjectID, Wrapper<SystemTable>>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for SystemKeyspace {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    SystemKeyspace { tables: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f,
                                    "SystemKeyspace");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "tables", &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl SystemKeyspace {
            pub const fn new(tables: Coremap<ObjectID, Wrapper<SystemTable>>)
                -> Self {
                Self { tables }
            }
        }
        /// A keyspace houses all the other tables
        pub struct Keyspace {
            /// the tables
            pub tables: Coremap<ObjectID, Arc<Table>>,
            /// the replication strategy for this keyspace
            #[allow(dead_code)]
            replication_strategy: cluster::ReplicationStrategy,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Keyspace {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Keyspace {
                        tables: ref __self_0_0, replication_strategy: ref __self_0_1
                        } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Keyspace");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "tables", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "replication_strategy", &&(*__self_0_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl Keyspace {
            /// Create a new empty keyspace with the default tables: a `default` table
            pub fn empty_default() -> Self {
                Self {
                    tables: {
                        let ht = Coremap::new();
                        ht.true_if_insert(DEFAULT,
                            Arc::new(Table::new_default_kve()));
                        ht
                    },
                    replication_strategy: cluster::ReplicationStrategy::default(),
                }
            }
            pub fn init_with_all_def_strategy(tables:
                    Coremap<ObjectID, Arc<Table>>) -> Self {
                Self {
                    tables,
                    replication_strategy: cluster::ReplicationStrategy::default(),
                }
            }
            /// Create a new empty keyspace with zero tables
            pub fn empty() -> Self {
                Self {
                    tables: Coremap::new(),
                    replication_strategy: cluster::ReplicationStrategy::default(),
                }
            }
            pub fn table_count(&self) -> usize { self.tables.len() }
            /// Get an atomic reference to a table in this keyspace if it exists
            pub fn get_table_atomic_ref<Q>(&self, table_identifier: &Q)
                -> Option<Arc<Table>> where ObjectID: Borrow<Q>, Q: Hash +
                Eq + PartialEq<ObjectID> + ?Sized {
                self.tables.get(table_identifier).map(|v| v.clone())
            }
            /// Create a new table
            pub fn create_table(&self, tableid: ObjectID, table: Table)
                -> bool {
                self.tables.true_if_insert(tableid, Arc::new(table))
            }
            /// Drop a table if it exists, if it is not forbidden and if no one references
            /// back to it. We don't want any looming table references i.e table gets deleted
            /// for the current connection and newer connections, but older instances still
            /// refer to the table.
            ///
            /// **Trip switch handled:** Yes
            pub fn drop_table<Q>(&self, table_identifier: &Q)
                -> KeyspaceResult<()> where ObjectID: Borrow<Q>, Q: Hash +
                Eq + PartialEq<ObjectID> + ?Sized {
                if table_identifier.eq(&DEFAULT) {
                        Err(DdlError::ProtectedObject)
                    } else if !self.tables.contains_key(table_identifier) {
                       Err(DdlError::ObjectNotFound)
                   } else {
                       let did_remove =
                           self.tables.true_remove_if(table_identifier,
                               |_table_id, table_atomic_ref|
                                   { Arc::strong_count(table_atomic_ref) == 1 });
                       if did_remove {
                               registry::get_preload_tripswitch().trip();
                               registry::get_cleanup_tripswitch().trip();
                               Ok(())
                           } else { Err(DdlError::StillInUse) }
                   }
            }
        }
    }
    pub mod table {
        use crate::actions::ActionResult;
        use crate::auth::Authmap;
        use crate::corestore::htable::Coremap;
        use crate::corestore::Data;
        use crate::dbnet::connection::prelude::Corestore;
        use crate::kvengine::{KVEListmap, KVEStandard, LockedVec};
        use crate::protocol::interface::ProtocolSpec;
        use crate::util;
        pub trait DescribeTable {
            type Table;
            fn try_get(table: &Table)
            -> Option<&Self::Table>;
            fn get<P: ProtocolSpec>(store: &Corestore)
                -> ActionResult<&Self::Table> {
                match store.estate.table {
                    Some((_, ref table)) => {
                        match Self::try_get(table) {
                            Some(tbl) => Ok(tbl),
                            None => util::err(P::RSTRING_WRONG_MODEL),
                        }
                    }
                    None => util::err(P::RSTRING_DEFAULT_UNSET),
                }
            }
        }
        pub struct KVEBlob;
        impl DescribeTable for KVEBlob {
            type Table = KVEStandard;
            fn try_get(table: &Table) -> Option<&Self::Table> {
                if let DataModel::KV(ref kve) = table.model_store {
                        Some(kve)
                    } else { None }
            }
        }
        pub struct KVEList;
        impl DescribeTable for KVEList {
            type Table = KVEListmap;
            fn try_get(table: &Table) -> Option<&Self::Table> {
                if let DataModel::KVExtListmap(ref kvl) = table.model_store {
                        Some(kvl)
                    } else { None }
            }
        }
        pub enum SystemDataModel { Auth(Authmap), }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for SystemDataModel {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&SystemDataModel::Auth(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "Auth");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                }
            }
        }
        pub struct SystemTable {
            /// data storage
            pub data: SystemDataModel,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for SystemTable {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    SystemTable { data: ref __self_0_0 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "SystemTable");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "data",
                                &&(*__self_0_0));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl SystemTable {
            pub const fn get_model_ref(&self) -> &SystemDataModel {
                &self.data
            }
            pub fn new(data: SystemDataModel) -> Self { Self { data } }
            pub fn new_auth(authmap: Authmap) -> Self {
                Self::new(SystemDataModel::Auth(authmap))
            }
        }
        pub enum DataModel { KV(KVEStandard), KVExtListmap(KVEListmap), }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for DataModel {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match (&*self,) {
                    (&DataModel::KV(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "KV");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                    (&DataModel::KVExtListmap(ref __self_0),) => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_tuple(f, "KVExtListmap");
                        let _ =
                            ::core::fmt::DebugTuple::field(debug_trait_builder,
                                &&(*__self_0));
                        ::core::fmt::DebugTuple::finish(debug_trait_builder)
                    }
                }
            }
        }
        /// The underlying table type. This is the place for the other data models (soon!)
        pub struct Table {
            /// a key/value store
            model_store: DataModel,
            /// is the table volatile
            volatile: bool,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for Table {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    Table {
                        model_store: ref __self_0_0, volatile: ref __self_0_1 } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "Table");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "model_store", &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "volatile", &&(*__self_0_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl Table {
            pub fn count(&self) -> usize {
                match &self.model_store {
                    DataModel::KV(kv) => kv.len(),
                    DataModel::KVExtListmap(kv) => kv.len(),
                }
            }
            /// Returns this table's _description_
            pub fn describe_self(&self) -> &'static str {
                match self.get_model_code() {
                    0 if self.is_volatile() =>
                        "Keymap { data:(binstr,binstr), volatile:true }",
                    0 if !self.is_volatile() =>
                        "Keymap { data:(binstr,binstr), volatile:false }",
                    1 if self.is_volatile() =>
                        "Keymap { data:(binstr,str), volatile:true }",
                    1 if !self.is_volatile() =>
                        "Keymap { data:(binstr,str), volatile:false }",
                    2 if self.is_volatile() =>
                        "Keymap { data:(str,str), volatile:true }",
                    2 if !self.is_volatile() =>
                        "Keymap { data:(str,str), volatile:false }",
                    3 if self.is_volatile() =>
                        "Keymap { data:(str,binstr), volatile:true }",
                    3 if !self.is_volatile() =>
                        "Keymap { data:(str,binstr), volatile:false }",
                    4 if self.is_volatile() =>
                        "Keymap { data:(binstr,list<binstr>), volatile:true }",
                    4 if !self.is_volatile() =>
                        "Keymap { data:(binstr,list<binstr>), volatile:false }",
                    5 if self.is_volatile() =>
                        "Keymap { data:(binstr,list<str>), volatile:true }",
                    5 if !self.is_volatile() =>
                        "Keymap { data:(binstr,list<str>), volatile:false }",
                    6 if self.is_volatile() =>
                        "Keymap { data:(str,list<binstr>), volatile:true }",
                    6 if !self.is_volatile() =>
                        "Keymap { data:(str,list<binstr>), volatile:false }",
                    7 if self.is_volatile() =>
                        "Keymap { data:(str,list<str>), volatile:true }",
                    7 if !self.is_volatile() =>
                        "Keymap { data:(str,list<str>), volatile:false }",
                    _ => unsafe { core::hint::unreachable_unchecked() },
                }
            }
            pub fn truncate_table(&self) {
                match self.model_store {
                    DataModel::KV(ref kv) => kv.truncate_table(),
                    DataModel::KVExtListmap(ref kv) => kv.truncate_table(),
                }
            }
            /// Returns the storage type as an 8-bit uint
            pub const fn storage_type(&self) -> u8 { self.volatile as u8 }
            /// Returns the volatility of the table
            pub const fn is_volatile(&self) -> bool { self.volatile }
            /// Create a new KVEBlob Table with the provided settings
            pub fn new_pure_kve_with_data(data: Coremap<Data, Data>,
                volatile: bool, k_enc: bool, v_enc: bool) -> Self {
                Self {
                    volatile,
                    model_store: DataModel::KV(KVEStandard::new(k_enc, v_enc,
                            data)),
                }
            }
            pub fn new_kve_listmap_with_data(data: Coremap<Data, LockedVec>,
                volatile: bool, k_enc: bool, payload_enc: bool) -> Self {
                Self {
                    volatile,
                    model_store: DataModel::KVExtListmap(KVEListmap::new(k_enc,
                            payload_enc, data)),
                }
            }
            pub fn from_model_code(code: u8, volatile: bool) -> Option<Self> {
                macro_rules! pkve {
                    ($kenc : expr, $venc : expr) =>
                    {
                        Self ::
                        new_pure_kve_with_data(Coremap :: new(), volatile, $kenc,
                        $venc)
                    } ;
                }
                macro_rules! listmap {
                    ($kenc : expr, $penc : expr) =>
                    {
                        Self ::
                        new_kve_listmap_with_data(Coremap :: new(), volatile, $kenc,
                        $penc)
                    } ;
                }
                let ret =
                    match code {
                        0 =>
                            Self::new_pure_kve_with_data(Coremap::new(), volatile,
                                false, false),
                        1 =>
                            Self::new_pure_kve_with_data(Coremap::new(), volatile,
                                false, true),
                        2 =>
                            Self::new_pure_kve_with_data(Coremap::new(), volatile, true,
                                true),
                        3 =>
                            Self::new_pure_kve_with_data(Coremap::new(), volatile, true,
                                false),
                        4 =>
                            Self::new_kve_listmap_with_data(Coremap::new(), volatile,
                                false, false),
                        5 =>
                            Self::new_kve_listmap_with_data(Coremap::new(), volatile,
                                false, true),
                        6 =>
                            Self::new_kve_listmap_with_data(Coremap::new(), volatile,
                                true, false),
                        7 =>
                            Self::new_kve_listmap_with_data(Coremap::new(), volatile,
                                true, true),
                        _ => return None,
                    };
                Some(ret)
            }
            /// Returns the default kve:
            /// - `k_enc`: `false`
            /// - `v_enc`: `false`
            /// - `volatile`: `false`
            pub fn new_default_kve() -> Self {
                Self::new_pure_kve_with_data(Coremap::new(), false, false,
                    false)
            }
            /// Returns the model code. See [`bytemarks`] for more info
            pub fn get_model_code(&self) -> u8 {
                match self.model_store {
                    DataModel::KV(ref kvs) => {
                        let (kenc, venc) = kvs.get_encoding_tuple();
                        let ret = kenc as u8 + venc as u8;
                        (ret & 1) + ((kenc as u8) << 1)
                    }
                    DataModel::KVExtListmap(ref kvlistmap) => {
                        let (kenc, venc) = kvlistmap.get_encoding_tuple();
                        ((kenc as u8) << 1) + (venc as u8) + 4
                    }
                }
            }
            /// Returns the inner data model
            pub fn get_model_ref(&self) -> &DataModel { &self.model_store }
        }
    }
    pub(super) type KeyspaceResult<T> = Result<T, DdlError>;
    struct ConnectionEntityState {
        /// the current table for a connection
        table: Option<(ObjectID, Arc<Table>)>,
        /// the current keyspace for a connection
        ks: Option<(ObjectID, Arc<Keyspace>)>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for ConnectionEntityState {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                ConnectionEntityState {
                    table: ref __self_0_0, ks: ref __self_0_1 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f,
                                "ConnectionEntityState");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "table", &&(*__self_0_0));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "ks",
                            &&(*__self_0_1));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for ConnectionEntityState {
        #[inline]
        fn clone(&self) -> ConnectionEntityState {
            match *self {
                ConnectionEntityState {
                    table: ref __self_0_0, ks: ref __self_0_1 } =>
                    ConnectionEntityState {
                        table: ::core::clone::Clone::clone(&(*__self_0_0)),
                        ks: ::core::clone::Clone::clone(&(*__self_0_1)),
                    },
            }
        }
    }
    impl ConnectionEntityState {
        fn default(ks: Arc<Keyspace>, tbl: Arc<Table>) -> Self {
            Self { table: Some((DEFAULT, tbl)), ks: Some((DEFAULT, ks)) }
        }
        fn set_ks(&mut self, ks: Arc<Keyspace>, ksid: ObjectID) {
            self.ks = Some((ksid, ks));
            self.table = None;
        }
        fn set_table(&mut self, ks: Arc<Keyspace>, ksid: ObjectID,
            tbl: Arc<Table>, tblid: ObjectID) {
            self.ks = Some((ksid, ks));
            self.table = Some((tblid, tbl));
        }
        fn get_id_pack(&self) -> (Option<&ObjectID>, Option<&ObjectID>) {
            (self.ks.as_ref().map(|(id, _)| id),
                self.table.as_ref().map(|(id, _)| id))
        }
    }
    /// The top level abstraction for the in-memory store. This is free to be shared across
    /// threads, cloned and well, whatever. Most importantly, clones have an independent container
    /// state that is the state of one connection and its container state preferences are never
    /// synced across instances. This is important (see the impl for more info)
    pub struct Corestore {
        estate: ConnectionEntityState,
        /// an atomic reference to the actual backing storage
        store: Arc<Memstore>,
        /// the snapshot engine
        sengine: Arc<SnapshotEngine>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for Corestore {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                Corestore {
                    estate: ref __self_0_0,
                    store: ref __self_0_1,
                    sengine: ref __self_0_2 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "Corestore");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "estate", &&(*__self_0_0));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "store", &&(*__self_0_1));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder,
                            "sengine", &&(*__self_0_2));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for Corestore {
        #[inline]
        fn clone(&self) -> Corestore {
            match *self {
                Corestore {
                    estate: ref __self_0_0,
                    store: ref __self_0_1,
                    sengine: ref __self_0_2 } =>
                    Corestore {
                        estate: ::core::clone::Clone::clone(&(*__self_0_0)),
                        store: ::core::clone::Clone::clone(&(*__self_0_1)),
                        sengine: ::core::clone::Clone::clone(&(*__self_0_2)),
                    },
            }
        }
    }
    impl Corestore {
        /// This is the only function you'll ever need to either create a new database instance
        /// or restore from an earlier instance
        pub fn init_with_snapcfg(sengine: Arc<SnapshotEngine>)
            -> StorageEngineResult<Self> {
            let store = storage::unflush::read_full()?;
            Ok(Self::default_with_store(store, sengine))
        }
        pub fn clone_store(&self) -> Arc<Memstore> { self.store.clone() }
        pub fn default_with_store(store: Memstore,
            sengine: Arc<SnapshotEngine>) -> Self {
            let cks =
                unsafe {
                    store.get_keyspace_atomic_ref(&DEFAULT).unsafe_unwrap()
                };
            let ctable =
                unsafe { cks.get_table_atomic_ref(&DEFAULT).unsafe_unwrap() };
            Self {
                estate: ConnectionEntityState::default(cks, ctable),
                store: Arc::new(store),
                sengine,
            }
        }
        pub fn get_engine(&self) -> &SnapshotEngine { &self.sengine }
        pub fn get_store(&self) -> &Memstore { &self.store }
        /// Swap out the current table with a different one
        ///
        /// If the table is non-existent or the default keyspace was unset, then
        /// false is returned. Else true is returned
        pub fn swap_entity(&mut self, entity: Entity<'_>)
            -> KeyspaceResult<()> {
            match entity {
                Entity::Single(ks) =>
                    match self.store.get_keyspace_atomic_ref(ks) {
                        Some(ksref) =>
                            self.estate.set_ks(ksref,
                                unsafe { ObjectID::from_slice(ks) }),
                        None => return Err(DdlError::ObjectNotFound),
                    },
                Entity::Full(ks, tbl) =>
                    match self.store.get_keyspace_atomic_ref(ks) {
                        Some(kspace) =>
                            match kspace.get_table_atomic_ref(tbl) {
                                Some(tblref) => unsafe {
                                    self.estate.set_table(kspace, ObjectID::from_slice(ks),
                                        tblref, ObjectID::from_slice(tbl))
                                },
                                None => return Err(DdlError::ObjectNotFound),
                            },
                        None => return Err(DdlError::ObjectNotFound),
                    },
                Entity::Partial(tbl) =>
                    match &self.estate.ks {
                        Some((_, ks)) =>
                            match ks.get_table_atomic_ref(tbl) {
                                Some(tblref) => {
                                    self.estate.table =
                                        Some((unsafe { ObjectID::from_slice(tbl) }, tblref));
                                }
                                None => return Err(DdlError::ObjectNotFound),
                            },
                        None => return Err(DdlError::DefaultNotFound),
                    },
            }
            Ok(())
        }
        /// Returns the current keyspace, if set
        pub fn get_cks(&self) -> KeyspaceResult<&Keyspace> {
            match self.estate.ks {
                Some((_, ref cks)) => Ok(cks),
                _ => Err(DdlError::DefaultNotFound),
            }
        }
        pub fn get_keyspace<Q>(&self, ksid: &Q) -> Option<Arc<Keyspace>> where
            ObjectID: Borrow<Q>, Q: Hash + Eq + ?Sized {
            self.store.get_keyspace_atomic_ref(ksid)
        }
        /// Get an atomic reference to a table
        pub fn get_table(&self, entity: Entity<'_>)
            -> KeyspaceResult<Arc<Table>> {
            match entity {
                Entity::Full(ksid, table) =>
                    match self.store.get_keyspace_atomic_ref(ksid) {
                        Some(ks) =>
                            match ks.get_table_atomic_ref(table) {
                                Some(tbl) => Ok(tbl),
                                None => Err(DdlError::ObjectNotFound),
                            },
                        None => Err(DdlError::ObjectNotFound),
                    },
                Entity::Single(tbl) | Entity::Partial(tbl) =>
                    match &self.estate.ks {
                        Some((_, ks)) =>
                            match ks.get_table_atomic_ref(tbl) {
                                Some(tbl) => Ok(tbl),
                                None => Err(DdlError::ObjectNotFound),
                            },
                        None => Err(DdlError::DefaultNotFound),
                    },
            }
        }
        pub fn get_ctable(&self) -> Option<Arc<Table>> {
            self.estate.table.as_ref().map(|(_, tbl)| tbl.clone())
        }
        pub fn get_table_result(&self) -> KeyspaceResult<&Table> {
            match self.estate.table {
                Some((_, ref table)) => Ok(table),
                _ => Err(DdlError::DefaultNotFound),
            }
        }
        pub fn get_ctable_ref(&self) -> Option<&Table> {
            self.estate.table.as_ref().map(|(_, tbl)| tbl.as_ref())
        }
        /// Returns a table with the provided specification
        pub fn get_table_with<P: ProtocolSpec, T: DescribeTable>(&self)
            -> ActionResult<&T::Table> {
            T::get::<P>(self)
        }
        /// Create a table: in-memory; **no transactional guarantees**. Two tables can be created
        /// simultaneously, but are never flushed unless we are very lucky. If the global flush
        /// system is close to a flush cycle -- then we are in luck: we pause the flush cycle
        /// through a global flush lock and then allow it to resume once we're done adding the table.
        /// This enables the flush routine to permanently write the table to disk. But it's all about
        /// luck -- the next mutual access may be yielded to the next `create table` command
        ///
        /// **Trip switch handled:** Yes
        pub fn create_table(&self, entity: Entity<'_>, modelcode: u8,
            volatile: bool) -> KeyspaceResult<()> {
            let entity = entity.into_owned();
            let flush_lock = registry::lock_flush_state();
            let ret =
                match entity {
                    OwnedEntity::Single(tblid) | OwnedEntity::Partial(tblid) =>
                        {
                        match &self.estate.ks {
                            Some((_, ks)) => {
                                let tbl = Table::from_model_code(modelcode, volatile);
                                if let Some(tbl) = tbl {
                                        if ks.create_table(tblid, tbl) {
                                                registry::get_preload_tripswitch().trip();
                                                Ok(())
                                            } else { Err(DdlError::AlreadyExists) }
                                    } else { Err(DdlError::WrongModel) }
                            }
                            None => Err(DdlError::DefaultNotFound),
                        }
                    }
                    OwnedEntity::Full(ksid, tblid) => {
                        match self.store.get_keyspace_atomic_ref(&ksid) {
                            Some(kspace) => {
                                let tbl = Table::from_model_code(modelcode, volatile);
                                if let Some(tbl) = tbl {
                                        if kspace.create_table(tblid, tbl) {
                                                registry::get_preload_tripswitch().trip();
                                                Ok(())
                                            } else { Err(DdlError::AlreadyExists) }
                                    } else { Err(DdlError::WrongModel) }
                            }
                            None => Err(DdlError::ObjectNotFound),
                        }
                    }
                };
            drop(flush_lock);
            ret
        }
        /// Drop a table
        pub fn drop_table(&self, entity: Entity<'_>) -> KeyspaceResult<()> {
            match entity {
                Entity::Single(tblid) | Entity::Partial(tblid) =>
                    match &self.estate.ks {
                        Some((_, ks)) => ks.drop_table(tblid),
                        None => Err(DdlError::DefaultNotFound),
                    },
                Entity::Full(ksid, tblid) =>
                    match self.store.get_keyspace_atomic_ref(ksid) {
                        Some(ks) => ks.drop_table(tblid),
                        None => Err(DdlError::ObjectNotFound),
                    },
            }
        }
        /// Create a keyspace **without any transactional guarantees**
        ///
        /// **Trip switch handled:** Yes
        pub fn create_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
            let flush_lock = registry::lock_flush_state();
            let ret =
                if self.store.create_keyspace(ksid) {
                        registry::get_preload_tripswitch().trip();
                        Ok(())
                    } else { Err(DdlError::AlreadyExists) };
            drop(flush_lock);
            ret
        }
        /// Drop a keyspace
        pub fn drop_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
            self.store.drop_keyspace(ksid)
        }
        /// Force drop a keyspace
        pub fn force_drop_keyspace(&self, ksid: ObjectID)
            -> KeyspaceResult<()> {
            self.store.force_drop_keyspace(ksid)
        }
        pub fn strong_count(&self) -> usize { Arc::strong_count(&self.store) }
        pub fn get_ids(&self) -> (Option<&ObjectID>, Option<&ObjectID>) {
            self.estate.get_id_pack()
        }
    }
}
mod dbnet {
    //! # `DBNET` - Database Networking
    //! This module provides low-level interaction with sockets. It handles the creation of
    //! a task for an incoming connection, handling errors if required and finally processing an incoming
    //! query.
    //!
    //! ## Typical flow
    //! This is how connections are handled:
    //! 1. A remote client creates a TCP connection to the server
    //! 2. An asynchronous is spawned on the Tokio runtime
    //! 3. Data from the socket is asynchronously read into an 8KB read buffer
    //! 4. Once the data is read completely (i.e the source sends an EOF byte), the `protocol` module
    //! is used to parse the stream
    //! 5. Now errors are handled if they occur. Otherwise, the query is executed by `Corestore::execute_query()`
    //!
    use {
        self::{
            tcp::{Listener, ListenerV1},
            tls::{SslListener, SslListenerV1},
        },
        crate::{
            auth::AuthProvider,
            config::{PortConfig, ProtocolVersion, SslOpts},
            corestore::Corestore, util::error::{Error, SkyResult},
            IoResult,
        },
        core::future::Future, std::{net::IpAddr, sync::Arc},
        tokio::{net::TcpListener, sync::{broadcast, mpsc, Semaphore}},
    };
    pub mod connection {
        //! # Generic connection traits
        //! The `con` module defines the generic connection traits `RawConnection` and `ProtocolRead`.
        //! These two traits can be used to interface with sockets that are used for communication through the Skyhash
        //! protocol.
        //!
        //! The `RawConnection` trait provides a basic set of methods that are required by prospective connection
        //! objects to be eligible for higher level protocol interactions (such as interactions with high-level query objects).
        //! Once a type implements this trait, it automatically gets a free `ProtocolRead` implementation. This immediately
        //! enables this connection object/type to use methods like read_query enabling it to read and interact with queries and write
        //! respones in compliance with the Skyhash protocol.
        use crate::{
            actions::{ActionError, ActionResult},
            auth::AuthProvider, corestore::Corestore,
            dbnet::{
                connection::prelude::FutureResult,
                tcp::{BufferedSocketStream, Connection},
                Terminator,
            },
            protocol::{
                interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
                Query,
            },
            queryengine, IoResult,
        };
        use bytes::{Buf, BytesMut};
        use std::{marker::PhantomData, sync::Arc};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt, BufWriter},
            sync::{mpsc, Semaphore},
        };
        pub type QueryWithAdvance = (Query, usize);
        pub enum QueryResult {
            Q(QueryWithAdvance),
            E(&'static [u8]),
            Wrongtype,
            Disconnected,
        }
        pub struct AuthProviderHandle<'a, P, T, Strm> {
            provider: &'a mut AuthProvider,
            executor: &'a mut ExecutorFn<P, T, Strm>,
            _phantom: PhantomData<(T, Strm)>,
        }
        impl<'a, P, T, Strm> AuthProviderHandle<'a, P, T, Strm> where
            T: ClientConnection<P, Strm>, Strm: Stream, P: ProtocolSpec {
            pub fn new(provider: &'a mut AuthProvider,
                executor: &'a mut ExecutorFn<P, T, Strm>) -> Self {
                Self { provider, executor, _phantom: PhantomData }
            }
            pub fn provider_mut(&mut self) -> &mut AuthProvider {
                self.provider
            }
            pub fn provider(&self) -> &AuthProvider { self.provider }
            pub fn swap_executor_to_anonymous(&mut self) {
                *self.executor = ConnectionHandler::execute_unauth;
            }
            pub fn swap_executor_to_authenticated(&mut self) {
                *self.executor = ConnectionHandler::execute_auth;
            }
        }
        pub mod prelude {
            //! A 'prelude' for callers that would like to use the `RawConnection` and `ProtocolRead` traits
            //!
            //! This module is hollow itself, it only re-exports from `dbnet::con` and `tokio::io`
            pub use super::{AuthProviderHandle, ClientConnection, Stream};
            pub use crate::{
                actions::{
                    ensure_boolean_or_aerr, ensure_cond_or_err, ensure_length,
                    translate_ddl_error,
                },
                corestore::{
                    table::{KVEBlob, KVEList},
                    Corestore,
                },
                get_tbl, handle_entity, is_lowbit_set,
                protocol::interface::ProtocolSpec, queryengine::ActionIter,
                registry,
                util::{self, FutureResult, UnwrapActionError, Unwrappable},
            };
            pub use tokio::io::{AsyncReadExt, AsyncWriteExt};
        }
        /// # The `RawConnection` trait
        ///
        /// The `RawConnection` trait has low-level methods that can be used to interface with raw sockets. Any type
        /// that successfully implements this trait will get an implementation for `ProtocolRead` and `ProtocolWrite`
        /// provided that it uses a protocol that implements the `ProtocolSpec` trait.
        ///
        /// ## Example of a `RawConnection` object
        /// Ideally a `RawConnection` object should look like (the generic parameter just exists for doc-tests, just think that
        /// there is a type `Strm`):
        /// ```no_run
        /// struct Connection<Strm> {
        ///     pub buffer: bytes::BytesMut,
        ///     pub stream: Strm,
        /// }
        /// ```
        ///
        /// `Strm` should be a stream, i.e something like an SSL connection/TCP connection.
        pub trait RawConnection<P: ProtocolSpec, Strm>: Send + Sync {
            /// Returns an **immutable** reference to the underlying read buffer
            fn get_buffer(&self)
            -> &BytesMut;
            /// Returns an **immutable** reference to the underlying stream
            fn get_stream(&self)
            -> &BufWriter<Strm>;
            /// Returns a **mutable** reference to the underlying read buffer
            fn get_mut_buffer(&mut self)
            -> &mut BytesMut;
            /// Returns a **mutable** reference to the underlying stream
            fn get_mut_stream(&mut self)
            -> &mut BufWriter<Strm>;
            /// Returns a **mutable** reference to (buffer, stream)
            ///
            /// This is to avoid double mutable reference errors
            fn get_mut_both(&mut self)
            -> (&mut BytesMut, &mut BufWriter<Strm>);
            /// Advance the read buffer by `forward_by` positions
            fn advance_buffer(&mut self, forward_by: usize) {
                self.get_mut_buffer().advance(forward_by)
            }
            /// Clear the internal buffer completely
            fn clear_buffer(&mut self) { self.get_mut_buffer().clear() }
        }
        impl<T, P> RawConnection<P, T> for Connection<T> where
            T: BufferedSocketStream + Sync + Send, P: ProtocolSpec {
            fn get_buffer(&self) -> &BytesMut { &self.buffer }
            fn get_stream(&self) -> &BufWriter<T> { &self.stream }
            fn get_mut_buffer(&mut self) -> &mut BytesMut { &mut self.buffer }
            fn get_mut_stream(&mut self) -> &mut BufWriter<T> {
                &mut self.stream
            }
            fn get_mut_both(&mut self) -> (&mut BytesMut, &mut BufWriter<T>) {
                (&mut self.buffer, &mut self.stream)
            }
        }
        pub(super) type ExecutorFn<P, T, Strm> =
            for<'s> fn(&'s mut ConnectionHandler<P, T, Strm>, Query)
                -> FutureResult<'s, ActionResult<()>>;
        /// # A generic connection handler
        ///
        /// A [`ConnectionHandler`] object is a generic connection handler for any object that implements the [`RawConnection`] trait (or
        /// the [`ProtocolRead`] trait). This function will accept such a type `T`, possibly a listener object and then use it to read
        /// a query, parse it and return an appropriate response through [`corestore::Corestore::execute_query`]
        pub struct ConnectionHandler<P, T, Strm> {
            db: Corestore,
            con: T,
            climit: Arc<Semaphore>,
            auth: AuthProvider,
            executor: ExecutorFn<P, T, Strm>,
            terminator: Terminator,
            _term_sig_tx: mpsc::Sender<()>,
            _marker: PhantomData<Strm>,
        }
        impl<P, T, Strm> ConnectionHandler<P, T, Strm> where
            T: ProtocolRead<P, Strm> + ProtocolWrite<P, Strm> + Send + Sync,
            Strm: Stream, P: ProtocolSpec {
            pub fn new(db: Corestore, con: T, auth: AuthProvider,
                executor: ExecutorFn<P, T, Strm>, climit: Arc<Semaphore>,
                terminator: Terminator, _term_sig_tx: mpsc::Sender<()>)
                -> Self {
                Self {
                    db,
                    con,
                    auth,
                    climit,
                    executor,
                    terminator,
                    _term_sig_tx,
                    _marker: PhantomData,
                }
            }
            pub async fn run(&mut self) -> IoResult<()> {
                while !self.terminator.is_termination_signal() {
                    let try_df =
                        {
                            #[doc(hidden)]
                            mod __tokio_select_util {
                                pub(super) enum Out<_0, _1> { _0(_0), _1(_1), Disabled, }
                                pub(super) type Mask = u8;
                            }
                            use ::tokio::macros::support::Future;
                            use ::tokio::macros::support::Pin;
                            use ::tokio::macros::support::Poll::{Ready, Pending};
                            const BRANCHES: u32 = 2;
                            let mut disabled: __tokio_select_util::Mask =
                                Default::default();
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 0;
                                    disabled |= mask;
                                }
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 1;
                                    disabled |= mask;
                                }
                            let mut output =
                                {
                                    let mut futures =
                                        (self.con.read_query(), self.terminator.receive_signal());
                                    ::tokio::macros::support::poll_fn(|cx|
                                                {
                                                    let mut is_pending = false;
                                                    let start =
                                                        { ::tokio::macros::support::thread_rng_n(BRANCHES) };
                                                    for i in 0..BRANCHES {
                                                        let branch;

                                                        #[allow(clippy :: modulo_one)]
                                                        { branch = (start + i) % BRANCHES; }
                                                        match branch
                                                            {
                                                                #[allow(unreachable_code)]
                                                                0 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { tdf => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_0(out));
                                                            }
                                                                #[allow(unreachable_code)]
                                                                1 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (_, fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { _ => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_1(out));
                                                            }
                                                            _ =>
                                                                ::core::panicking::unreachable_display(&"reaching this means there probably is an off by one bug"),
                                                        }
                                                    }
                                                    if is_pending {
                                                            Pending
                                                        } else { Ready(__tokio_select_util::Out::Disabled) }
                                                }).await
                                };
                            match output {
                                __tokio_select_util::Out::_0(tdf) => tdf,
                                __tokio_select_util::Out::_1(_) => { return Ok(()); }
                                __tokio_select_util::Out::Disabled => {
                                    ::std::rt::begin_panic("all branches are disabled and there is no else branch")
                                }
                                _ =>
                                    ::core::panicking::unreachable_display(&"failed to match bind"),
                            }
                        };
                    match try_df {
                        Ok(QueryResult::Q((query, advance_by))) => {
                            #[cfg(debug_assertions)]
                            let len_at_start = self.con.get_buffer().len();
                            #[cfg(debug_assertions)]
                            let sptr_at_start = self.con.get_buffer().as_ptr() as usize;
                            #[cfg(debug_assertions)]
                            let eptr_at_start = sptr_at_start + len_at_start;
                            {
                                match self.execute_query(query).await {
                                    Ok(()) => {}
                                    Err(ActionError::ActionError(e)) => {
                                        self.con.close_conn_with_error(e).await?;
                                    }
                                    Err(ActionError::IoError(e)) => { return Err(e); }
                                }
                            }
                            {
                                if true {
                                        match (&self.con.get_buffer().len(), &len_at_start) {
                                            (left_val, right_val) => {
                                                if !(*left_val == *right_val) {
                                                        let kind = ::core::panicking::AssertKind::Eq;
                                                        ::core::panicking::assert_failed(kind, &*left_val,
                                                            &*right_val, ::core::option::Option::None);
                                                    }
                                            }
                                        };
                                    };
                                if true {
                                        match (&(self.con.get_buffer().as_ptr() as usize),
                                                &sptr_at_start) {
                                            (left_val, right_val) => {
                                                if !(*left_val == *right_val) {
                                                        let kind = ::core::panicking::AssertKind::Eq;
                                                        ::core::panicking::assert_failed(kind, &*left_val,
                                                            &*right_val, ::core::option::Option::None);
                                                    }
                                            }
                                        };
                                    };
                                if true {
                                        match (&(unsafe {
                                                            self.con.get_buffer().as_ptr().add(len_at_start)
                                                        } as usize), &eptr_at_start) {
                                            (left_val, right_val) => {
                                                if !(*left_val == *right_val) {
                                                        let kind = ::core::panicking::AssertKind::Eq;
                                                        ::core::panicking::assert_failed(kind, &*left_val,
                                                            &*right_val, ::core::option::Option::None);
                                                    }
                                            }
                                        };
                                    };
                                self.con.advance_buffer(advance_by);
                            }
                        }
                        Ok(QueryResult::E(r)) =>
                            self.con.close_conn_with_error(r).await?,
                        Ok(QueryResult::Wrongtype) => {
                            self.con.close_conn_with_error(P::RCODE_WRONGTYPE_ERR).await?
                        }
                        Ok(QueryResult::Disconnected) =>
                            return Ok(()),
                            #[cfg(not(windows))]
                            Err(e) => return Err(e),
                    }
                }
                Ok(())
            }
            /// Execute queries for an unauthenticated user
            pub(super) fn execute_unauth(&mut self, query: Query)
                -> FutureResult<'_, ActionResult<()>> {
                Box::pin(async move
                        {
                        let con = &mut self.con;
                        let db = &mut self.db;
                        let mut auth_provider =
                            AuthProviderHandle::new(&mut self.auth, &mut self.executor);
                        match query {
                            Query::Simple(sq) => {
                                con.write_simple_query_header().await?;
                                queryengine::execute_simple_noauth(db, con,
                                            &mut auth_provider, sq).await?;
                            }
                            Query::Pipelined(_) => {
                                con.write_simple_query_header().await?;
                                con._write_raw(P::AUTH_CODE_BAD_CREDENTIALS).await?;
                            }
                        }
                        Ok(())
                    })
            }
            /// Execute queries for an authenticated user
            pub(super) fn execute_auth(&mut self, query: Query)
                -> FutureResult<'_, ActionResult<()>> {
                Box::pin(async move
                        {
                        let con = &mut self.con;
                        let db = &mut self.db;
                        let mut auth_provider =
                            AuthProviderHandle::new(&mut self.auth, &mut self.executor);
                        match query {
                            Query::Simple(q) => {
                                con.write_simple_query_header().await?;
                                queryengine::execute_simple(db, con, &mut auth_provider,
                                            q).await?;
                            }
                            Query::Pipelined(pipeline) => {
                                con.write_pipelined_query_header(pipeline.len()).await?;
                                queryengine::execute_pipeline(db, con, &mut auth_provider,
                                            pipeline).await?;
                            }
                        }
                        Ok(())
                    })
            }
            /// Execute a query that has already been validated by `Connection::read_query`
            async fn execute_query(&mut self, query: Query)
                -> ActionResult<()> {
                (self.executor)(self, query).await?;
                self.con._flush_stream().await?;
                Ok(())
            }
        }
        impl<P, T, Strm> Drop for ConnectionHandler<P, T, Strm> {
            fn drop(&mut self) { self.climit.add_permits(1); }
        }
        /// A simple _shorthand trait_ for the insanely long definition of the TCP-based stream generic type
        pub trait Stream: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync {
        }
        impl<T> Stream for T where T: AsyncReadExt + AsyncWriteExt + Unpin +
            Send + Sync {}
        /// A simple _shorthand trait_ for the insanely long definition of the connection generic type
        pub trait ClientConnection<P: ProtocolSpec,
            Strm: Stream>: ProtocolWrite<P, Strm> + ProtocolRead<P, Strm> +
            Send + Sync {
        }
        impl<P, T, Strm> ClientConnection<P, Strm> for T where
            T: ProtocolWrite<P, Strm> + ProtocolRead<P, Strm> + Send + Sync,
            Strm: Stream, P: ProtocolSpec {}
    }
    #[macro_use]
    mod macros {
        macro_rules! skip_loop_err {
            ($expr : expr) =>
            { match $expr { Ok(ret) => ret, Err(_) => continue, } } ;
        }
    }
    mod tcp {
        pub use protocol::{ParseResult, Query};
        use {
            crate::{
                dbnet::{
                    connection::{ConnectionHandler, ExecutorFn},
                    BaseListener, Terminator,
                },
                protocol::{
                    self,
                    interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
                    Skyhash1, Skyhash2,
                },
                IoResult,
            },
            bytes::BytesMut, libsky::BUF_CAP,
            std::{cell::Cell, time::Duration},
            tokio::{
                io::{AsyncWrite, BufWriter},
                net::TcpStream, time,
            },
        };
        pub trait BufferedSocketStream: AsyncWrite {}
        impl BufferedSocketStream for TcpStream {}
        type TcpExecutorFn<P> =
            ExecutorFn<P, Connection<TcpStream>, TcpStream>;
        /// A TCP/SSL connection wrapper
        pub struct Connection<T> where T: BufferedSocketStream {
            /// The connection to the remote socket, wrapped in a buffer to speed
            /// up writing
            pub stream: BufWriter<T>,
            /// The in-memory read buffer. The size is given by `BUF_CAP`
            pub buffer: BytesMut,
        }
        impl<T> Connection<T> where T: BufferedSocketStream {
            /// Initiailize a new `Connection` instance
            pub fn new(stream: T) -> Self {
                Connection {
                    stream: BufWriter::new(stream),
                    buffer: BytesMut::with_capacity(BUF_CAP),
                }
            }
        }
        pub struct TcpBackoff {
            current: Cell<u8>,
        }
        impl TcpBackoff {
            const MAX_BACKOFF: u8 = 64;
            pub const fn new() -> Self { Self { current: Cell::new(1) } }
            pub async fn spin(&self) {
                time::sleep(Duration::from_secs(self.current.get() as
                                u64)).await;
                self.current.set(self.current.get() << 1);
            }
            pub fn should_disconnect(&self) -> bool {
                self.current.get() > Self::MAX_BACKOFF
            }
        }
        pub type Listener = RawListener<Skyhash2>;
        pub type ListenerV1 = RawListener<Skyhash1>;
        /// A listener
        pub struct RawListener<P> {
            pub base: BaseListener,
            executor_fn: TcpExecutorFn<P>,
        }
        impl<P: ProtocolSpec + 'static> RawListener<P> where
            Connection<TcpStream>: ProtocolRead<P, TcpStream> +
            ProtocolWrite<P, TcpStream> {
            pub fn new(base: BaseListener) -> Self {
                Self {
                    executor_fn: if base.auth.is_enabled() {
                            ConnectionHandler::execute_unauth
                        } else { ConnectionHandler::execute_auth },
                    base,
                }
            }
            /// Accept an incoming connection
            async fn accept(&mut self) -> IoResult<TcpStream> {
                let backoff = TcpBackoff::new();
                loop {
                    match self.base.listener.accept().await {
                        Ok((stream, _)) => return Ok(stream),
                        Err(e) => {
                            if backoff.should_disconnect() { return Err(e); }
                        }
                    }
                    backoff.spin().await;
                }
            }
            /// Run the server
            pub async fn run(&mut self) -> IoResult<()> {
                loop {
                    self.base.climit.acquire().await.unwrap().forget();
                    let stream =
                        match self.accept().await {
                            Ok(ret) => ret,
                            Err(_) => continue,
                        };
                    let mut chandle =
                        ConnectionHandler::new(self.base.db.clone(),
                            Connection::new(stream), self.base.auth.clone(),
                            self.executor_fn, self.base.climit.clone(),
                            Terminator::new(self.base.signal.subscribe()),
                            self.base.terminate_tx.clone());
                    tokio::spawn(async move
                            {
                            if let Err(e) = chandle.run().await {
                                    {
                                        let lvl = ::log::Level::Error;
                                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                    lvl <= ::log::max_level() {
                                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Error: "],
                                                        &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                    &("skyd::dbnet::tcp", "skyd::dbnet::tcp",
                                                            "server/src/dbnet/tcp.rs", 171u32),
                                                    ::log::__private_api::Option::None);
                                            }
                                    };
                                }
                        });
                }
            }
        }
    }
    mod tls {
        use {
            crate::{
                dbnet::{
                    connection::{ConnectionHandler, ExecutorFn},
                    tcp::{BufferedSocketStream, Connection, TcpBackoff},
                    BaseListener, Terminator,
                },
                protocol::{
                    interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
                    Skyhash1, Skyhash2,
                },
                util::error::{Error, SkyResult},
                IoResult,
            },
            openssl::{
                pkey::PKey, rsa::Rsa,
                ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod},
            },
            std::{fs, pin::Pin},
            tokio::net::TcpStream, tokio_openssl::SslStream,
        };
        impl BufferedSocketStream for SslStream<TcpStream> {}
        type SslExecutorFn<P> =
            ExecutorFn<P, Connection<SslStream<TcpStream>>,
            SslStream<TcpStream>>;
        pub type SslListener = SslListenerRaw<Skyhash2>;
        pub type SslListenerV1 = SslListenerRaw<Skyhash1>;
        pub struct SslListenerRaw<P> {
            pub base: BaseListener,
            acceptor: SslAcceptor,
            executor_fn: SslExecutorFn<P>,
        }
        impl<P: ProtocolSpec + 'static> SslListenerRaw<P> where
            Connection<SslStream<TcpStream>>: ProtocolRead<P,
            SslStream<TcpStream>> + ProtocolWrite<P, SslStream<TcpStream>> {
            pub fn new_pem_based_ssl_connection(key_file: String,
                chain_file: String, base: BaseListener,
                tls_passfile: Option<String>)
                -> SkyResult<SslListenerRaw<P>> {
                let mut acceptor_builder =
                    SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
                acceptor_builder.set_certificate_chain_file(chain_file)?;
                if let Some(tls_passfile) = tls_passfile {
                        let tls_private_key =
                            fs::read(key_file).map_err(|e|
                                        Error::ioerror_extra(e, "reading TLS private key"))?;
                        let tls_keyfile_stream =
                            fs::read(tls_passfile).map_err(|e|
                                        Error::ioerror_extra(e, "reading TLS password file"))?;
                        let pkey =
                            Rsa::private_key_from_pem_passphrase(&tls_private_key,
                                    &tls_keyfile_stream)?;
                        let pkey = PKey::from_rsa(pkey)?;
                        acceptor_builder.set_private_key(&pkey)?;
                    } else {
                       acceptor_builder.set_private_key_file(key_file,
                               SslFiletype::PEM)?;
                   }
                Ok(Self {
                        acceptor: acceptor_builder.build(),
                        executor_fn: if base.auth.is_enabled() {
                                ConnectionHandler::execute_unauth
                            } else { ConnectionHandler::execute_auth },
                        base,
                    })
            }
            async fn accept(&mut self) -> SkyResult<SslStream<TcpStream>> {
                let backoff = TcpBackoff::new();
                loop {
                    match self.base.listener.accept().await {
                        Ok((stream, _)) => {
                            let ssl = Ssl::new(self.acceptor.context())?;
                            let mut stream = SslStream::new(ssl, stream)?;
                            Pin::new(&mut stream).accept().await?;
                            return Ok(stream);
                        }
                        Err(e) => {
                            if backoff.should_disconnect() { return Err(e.into()); }
                        }
                    }
                    backoff.spin().await;
                }
            }
            pub async fn run(&mut self) -> IoResult<()> {
                loop {
                    self.base.climit.acquire().await.unwrap().forget();
                    let stream =
                        match self.accept().await {
                            Ok(ret) => ret,
                            Err(_) => continue,
                        };
                    let mut sslhandle =
                        ConnectionHandler::new(self.base.db.clone(),
                            Connection::new(stream), self.base.auth.clone(),
                            self.executor_fn, self.base.climit.clone(),
                            Terminator::new(self.base.signal.subscribe()),
                            self.base.terminate_tx.clone());
                    tokio::spawn(async move
                            {
                            if let Err(e) = sslhandle.run().await {
                                    {
                                        let lvl = ::log::Level::Error;
                                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                    lvl <= ::log::max_level() {
                                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Error: "],
                                                        &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                    &("skyd::dbnet::tls", "skyd::dbnet::tls",
                                                            "server/src/dbnet/tls.rs", 152u32),
                                                    ::log::__private_api::Option::None);
                                            }
                                    };
                                }
                        });
                }
            }
        }
    }
    pub const MAXIMUM_CONNECTION_LIMIT: usize = 50000;
    /// Responsible for gracefully shutting down the server instead of dying randomly
    pub struct Terminator {
        terminate: bool,
        signal: broadcast::Receiver<()>,
    }
    impl Terminator {
        /// Create a new `Terminator` instance
        pub const fn new(signal: broadcast::Receiver<()>) -> Self {
            Terminator { terminate: false, signal }
        }
        /// Check if the signal is a termination signal
        pub const fn is_termination_signal(&self) -> bool { self.terminate }
        /// Wait to receive a shutdown signal
        pub async fn receive_signal(&mut self) {
            if self.terminate { return; }
            let _ = self.signal.recv().await;
            self.terminate = true;
        }
    }
    /// The base TCP listener
    pub struct BaseListener {
        /// An atomic reference to the coretable
        pub db: Corestore,
        /// The auth provider
        pub auth: AuthProvider,
        /// The incoming connection listener (binding)
        pub listener: TcpListener,
        /// The maximum number of connections
        pub climit: Arc<Semaphore>,
        /// The shutdown broadcaster
        pub signal: broadcast::Sender<()>,
        pub terminate_tx: mpsc::Sender<()>,
        pub terminate_rx: mpsc::Receiver<()>,
    }
    impl BaseListener {
        pub async fn init(db: &Corestore, auth: AuthProvider, host: IpAddr,
            port: u16, semaphore: Arc<Semaphore>,
            signal: broadcast::Sender<()>) -> SkyResult<Self> {
            let (terminate_tx, terminate_rx) = mpsc::channel(1);
            let listener =
                TcpListener::bind((host,
                                    port)).await.map_err(|e|
                            Error::ioerror_extra(e,
                                {
                                    let res =
                                        ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["binding to port "],
                                                &[::core::fmt::ArgumentV1::new_display(&port)]));
                                    res
                                }))?;
            Ok(Self {
                    db: db.clone(),
                    auth,
                    listener,
                    climit: semaphore,
                    signal,
                    terminate_tx,
                    terminate_rx,
                })
        }
        pub async fn release_self(self) {
            let Self { mut terminate_rx, terminate_tx, signal, .. } = self;
            drop(signal);
            drop(terminate_tx);
            let _ = terminate_rx.recv().await;
        }
    }
    /// Multiple Listener Interface
    ///
    /// A `MultiListener` is an abstraction over an `SslListener` or a `Listener` to facilitate
    /// easier asynchronous listening on multiple ports.
    ///
    /// - The `SecureOnly` variant holds an `SslListener`
    /// - The `InsecureOnly` variant holds a `Listener`
    /// - The `Multi` variant holds both an `SslListener` and a `Listener`
    ///     This variant enables listening to both secure and insecure sockets at the same time
    ///     asynchronously
    #[allow(clippy :: large_enum_variant)]
    pub enum MultiListener {
        SecureOnly(SslListener),
        SecureOnlyV1(SslListenerV1),
        InsecureOnly(Listener),
        InsecureOnlyV1(ListenerV1),
        Multi(Listener, SslListener),
        MultiV1(ListenerV1, SslListenerV1),
    }
    async fn wait_on_port_futures(a: impl Future<Output = IoResult<()>>,
        b: impl Future<Output = IoResult<()>>) -> IoResult<()> {
        let (e1, e2) =
            {
                use ::tokio::macros::support::{
                    maybe_done, poll_fn, Future, Pin,
                };
                use ::tokio::macros::support::Poll::{Ready, Pending};
                let mut futures = (maybe_done(a), maybe_done(b));
                let mut skip_next_time: u32 = 0;
                poll_fn(move |cx|
                            {
                                const COUNT: u32 = 0 + 1 + 1;
                                let mut is_pending = false;
                                let mut to_run = COUNT;
                                let mut skip = skip_next_time;
                                skip_next_time =
                                    if skip + 1 == COUNT { 0 } else { skip + 1 };
                                loop {
                                    if skip == 0 {
                                            if to_run == 0 { break; }
                                            to_run -= 1;
                                            let (fut, ..) = &mut futures;
                                            let mut fut = unsafe { Pin::new_unchecked(fut) };
                                            if fut.poll(cx).is_pending() { is_pending = true; }
                                        } else { skip -= 1; }
                                    if skip == 0 {
                                            if to_run == 0 { break; }
                                            to_run -= 1;
                                            let (_, fut, ..) = &mut futures;
                                            let mut fut = unsafe { Pin::new_unchecked(fut) };
                                            if fut.poll(cx).is_pending() { is_pending = true; }
                                        } else { skip -= 1; }
                                }
                                if is_pending {
                                        Pending
                                    } else {
                                       Ready(({
                                                   let (fut, ..) = &mut futures;
                                                   let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                   fut.take_output().expect("expected completed future")
                                               },
                                               {
                                                   let (_, fut, ..) = &mut futures;
                                                   let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                   fut.take_output().expect("expected completed future")
                                               }))
                                   }
                            }).await
            };
        if let Err(e) = e1 {
                {
                    let lvl = ::log::Level::Error;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Insecure listener failed with: "],
                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                &("skyd::dbnet", "skyd::dbnet", "server/src/dbnet/mod.rs",
                                        181u32), ::log::__private_api::Option::None);
                        }
                };
            }
        if let Err(e) = e2 {
                {
                    let lvl = ::log::Level::Error;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Secure listener failed with: "],
                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                &("skyd::dbnet", "skyd::dbnet", "server/src/dbnet/mod.rs",
                                        184u32), ::log::__private_api::Option::None);
                        }
                };
            }
        Ok(())
    }
    impl MultiListener {
        /// Create a new `InsecureOnly` listener
        pub fn new_insecure_only(base: BaseListener,
            protocol: ProtocolVersion) -> Self {
            match protocol {
                ProtocolVersion::V2 =>
                    MultiListener::InsecureOnly(Listener::new(base)),
                ProtocolVersion::V1 =>
                    MultiListener::InsecureOnlyV1(ListenerV1::new(base)),
            }
        }
        /// Create a new `SecureOnly` listener
        pub fn new_secure_only(base: BaseListener, ssl: SslOpts,
            protocol: ProtocolVersion) -> SkyResult<Self> {
            let listener =
                match protocol {
                    ProtocolVersion::V2 => {
                        let listener =
                            SslListener::new_pem_based_ssl_connection(ssl.key,
                                    ssl.chain, base, ssl.passfile)?;
                        MultiListener::SecureOnly(listener)
                    }
                    ProtocolVersion::V1 => {
                        let listener =
                            SslListenerV1::new_pem_based_ssl_connection(ssl.key,
                                    ssl.chain, base, ssl.passfile)?;
                        MultiListener::SecureOnlyV1(listener)
                    }
                };
            Ok(listener)
        }
        /// Create a new `Multi` listener that has both a secure and an insecure listener
        pub async fn new_multi(ssl_base_listener: BaseListener,
            tcp_base_listener: BaseListener, ssl: SslOpts,
            protocol: ProtocolVersion) -> SkyResult<Self> {
            let mls =
                match protocol {
                    ProtocolVersion::V2 => {
                        let secure_listener =
                            SslListener::new_pem_based_ssl_connection(ssl.key,
                                    ssl.chain, ssl_base_listener, ssl.passfile)?;
                        let insecure_listener = Listener::new(tcp_base_listener);
                        MultiListener::Multi(insecure_listener, secure_listener)
                    }
                    ProtocolVersion::V1 => {
                        let secure_listener =
                            SslListenerV1::new_pem_based_ssl_connection(ssl.key,
                                    ssl.chain, ssl_base_listener, ssl.passfile)?;
                        let insecure_listener = ListenerV1::new(tcp_base_listener);
                        MultiListener::MultiV1(insecure_listener, secure_listener)
                    }
                };
            Ok(mls)
        }
        /// Start the server
        ///
        /// The running of single and/or parallel listeners is handled by this function by
        /// exploiting the working of async functions
        pub async fn run_server(&mut self) -> IoResult<()> {
            match self {
                MultiListener::SecureOnly(secure_listener) =>
                    secure_listener.run().await,
                MultiListener::SecureOnlyV1(secure_listener) =>
                    secure_listener.run().await,
                MultiListener::InsecureOnly(insecure_listener) =>
                    insecure_listener.run().await,
                MultiListener::InsecureOnlyV1(insecure_listener) =>
                    insecure_listener.run().await,
                MultiListener::Multi(insecure_listener, secure_listener) => {
                    wait_on_port_futures(insecure_listener.run(),
                            secure_listener.run()).await
                }
                MultiListener::MultiV1(insecure_listener, secure_listener) =>
                    {
                    wait_on_port_futures(insecure_listener.run(),
                            secure_listener.run()).await
                }
            }
        }
        /// Signal the ports to shut down and only return after they have shut down
        ///
        /// **Do note:** This function doesn't flush the `Corestore` object! The **caller has to
        /// make sure that the data is saved!**
        pub async fn finish_with_termsig(self) {
            match self {
                MultiListener::InsecureOnly(Listener { base, .. }) |
                    MultiListener::SecureOnly(SslListener { base, .. }) |
                    MultiListener::InsecureOnlyV1(ListenerV1 { base, .. }) |
                    MultiListener::SecureOnlyV1(SslListenerV1 { base, .. }) =>
                    base.release_self().await,
                MultiListener::Multi(insecure, secure) => {
                    insecure.base.release_self().await;
                    secure.base.release_self().await;
                }
                MultiListener::MultiV1(insecure, secure) => {
                    insecure.base.release_self().await;
                    secure.base.release_self().await;
                }
            }
        }
    }
    /// Initialize the database networking
    pub async fn connect(ports: PortConfig, protocol: ProtocolVersion,
        maxcon: usize, db: Corestore, auth: AuthProvider,
        signal: broadcast::Sender<()>) -> SkyResult<MultiListener> {
        let climit = Arc::new(Semaphore::new(maxcon));
        let base_listener_init =
            |host, port|
                {
                    BaseListener::init(&db, auth.clone(), host, port,
                        climit.clone(), signal.clone())
                };
        let description = ports.get_description();
        let server =
            match ports {
                PortConfig::InsecureOnly { host, port } => {
                    MultiListener::new_insecure_only(base_listener_init(host,
                                    port).await?, protocol)
                }
                PortConfig::SecureOnly { host, ssl } =>
                    MultiListener::new_secure_only(base_listener_init(host,
                                        ssl.port).await?, ssl, protocol)?,
                PortConfig::Multi { host, port, ssl } => {
                    let secure_listener =
                        base_listener_init(host, ssl.port).await?;
                    let insecure_listener =
                        base_listener_init(host, port).await?;
                    MultiListener::new_multi(secure_listener, insecure_listener,
                                ssl, protocol).await?
                }
            };
        {
            let lvl = ::log::Level::Info;
            if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                    ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Server started on "],
                            &[::core::fmt::ArgumentV1::new_display(&description)]), lvl,
                        &("skyd::dbnet", "skyd::dbnet", "server/src/dbnet/mod.rs",
                                332u32), ::log::__private_api::Option::None);
                }
        };
        Ok(server)
    }
}
mod diskstore {
    //! This module provides tools for handling persistently stored data
    pub mod flock {
        //! # File Locking
        //!
        //! This module provides the `FileLock` struct that can be used for locking and/or unlocking files on
        //! unix-based systems and Windows systems
        #![allow(dead_code)]
        use std::{
            fs::{File, OpenOptions},
            io::{Result, Seek, SeekFrom, Write},
            path::Path,
        };
        /// # File Lock
        /// A file lock object holds a `std::fs::File` that is used to `lock()` and `unlock()` a file with a given
        /// `filename` passed into the `lock()` method. The file lock is **not configured** to drop the file lock when the
        /// object is dropped. The `file` field is essentially used to get the raw file descriptor for passing to
        /// the platform-specific lock/unlock methods.
        ///
        /// **Note:** You need to lock a file first using this object before unlocking it!
        ///
        /// ## Suggestions
        ///
        /// It is always a good idea to attempt a lock release (unlock) explicitly than leaving it to the operating
        /// system. If you manually run unlock, another unlock won't be called to avoid an extra costly (is it?)
        /// syscall; this is achieved with the `unlocked` flag (field) which is set to true when the `unlock()` function
        /// is called.
        ///
        pub struct FileLock {
            file: File,
            unlocked: bool,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::fmt::Debug for FileLock {
            fn fmt(&self, f: &mut ::core::fmt::Formatter)
                -> ::core::fmt::Result {
                match *self {
                    FileLock { file: ref __self_0_0, unlocked: ref __self_0_1 }
                        => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "FileLock");
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder, "file",
                                &&(*__self_0_0));
                        let _ =
                            ::core::fmt::DebugStruct::field(debug_trait_builder,
                                "unlocked", &&(*__self_0_1));
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        impl FileLock {
            /// Initialize a new `FileLock` by locking a file
            ///
            /// This function will create and lock a file if it doesn't exist or it
            /// will lock the existing file
            /// **This will immediately fail if locking fails, i.e it is non-blocking**
            pub fn lock(filename: impl AsRef<Path>) -> Result<Self> {
                let file =
                    OpenOptions::new().create(true).read(true).write(true).open(filename.as_ref())?;
                Self::_lock(&file)?;
                Ok(Self { file, unlocked: false })
            }
            /// The internal lock function
            ///
            /// This is the function that actually locks the file and is kept separate only for purposes
            /// of maintainability
            fn _lock(file: &File) -> Result<()> { __sys::try_lock_ex(file) }
            /// Unlock the file
            ///
            /// This sets the `unlocked` flag to true
            pub fn unlock(&mut self) -> Result<()> {
                if !self.unlocked {
                        __sys::unlock_file(&self.file)?;
                        self.unlocked = true;
                        Ok(())
                    } else { Ok(()) }
            }
            /// Write something to this file
            pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
                self.file.set_len(0)?;
                self.file.seek(SeekFrom::Start(0))?;
                self.file.write_all(bytes)
            }
            /// Sync all metadata and flush buffers before returning
            pub fn fsync(&self) -> Result<()> { self.file.sync_all() }
        }
        #[cfg(all(not(target_os = "solaris"), unix))]
        mod __sys {
            //! # Unix platform-specific file locking
            //! This module contains methods used by the `FileLock` object in this module to lock and/or
            //! unlock files.
            use libc::c_int;
            use std::fs::File;
            use std::io::Error;
            use std::io::Result;
            use std::os::unix::io::AsRawFd;
            use std::os::unix::io::FromRawFd;
            extern "C" {
                /// Block and acquire an exclusive lock with `libc`'s `flock`
                fn lock_exclusive(fd: i32)
                -> c_int;
                /// Attempt to acquire an exclusive lock in a non-blocking manner with `libc`'s `flock`
                fn try_lock_exclusive(fd: i32)
                -> c_int;
                /// Attempt to unlock a file with `libc`'s flock
                fn unlock(fd: i32)
                -> c_int;
            }
            /// Obtain an exclusive lock and **block** until we acquire it
            pub fn lock_ex(file: &File) -> Result<()> {
                let errno = unsafe { lock_exclusive(file.as_raw_fd()) };
                match errno {
                    0 => Ok(()),
                    x => Err(Error::from_raw_os_error(x)),
                }
            }
            /// Try to obtain an exclusive lock and **immediately return an error if this is blocking**
            pub fn try_lock_ex(file: &File) -> Result<()> {
                let errno = unsafe { try_lock_exclusive(file.as_raw_fd()) };
                match errno {
                    0 => Ok(()),
                    x => Err(Error::from_raw_os_error(x)),
                }
            }
            /// Attempt to unlock a file
            pub fn unlock_file(file: &File) -> Result<()> {
                let errno = unsafe { unlock(file.as_raw_fd()) };
                match errno {
                    0 => Ok(()),
                    x => Err(Error::from_raw_os_error(x)),
                }
            }
            /// Duplicate a file
            ///
            /// Good ol' libc dup() calls
            pub fn duplicate(file: &File) -> Result<File> {
                unsafe {
                    let fd = libc::dup(file.as_raw_fd());
                    if fd < 0 {
                            Err(Error::last_os_error())
                        } else { Ok(File::from_raw_fd(fd)) }
                }
            }
        }
    }
}
mod kvengine {
    #![allow(dead_code)]
    pub mod encoding {
        use crate::corestore::booltable::BoolTable;
        use crate::corestore::booltable::TwoBitLUT;
        use crate::protocol::iter::AnyArrayIter;
        use crate::protocol::iter::BorrowedAnyArrayIter;
        type PairFn = fn(&[u8], &[u8]) -> bool;
        pub const ENCODING_LUT_ITER:
            BoolTable<fn(BorrowedAnyArrayIter) -> bool> =
            BoolTable::new(is_okay_encoded_iter, is_okay_no_encoding_iter);
        pub const ENCODING_LUT_ITER_PAIR: TwoBitLUT<fn(&AnyArrayIter) -> bool>
            =
            TwoBitLUT::new(pair_is_okay_encoded_iter_ff,
                pair_is_okay_encoded_iter_ft, pair_is_okay_encoded_iter_tf,
                pair_is_okay_encoded_iter_tt);
        pub const ENCODING_LUT: BoolTable<fn(&[u8]) -> bool> =
            BoolTable::new(self::is_okay_encoded, self::is_okay_no_encoding);
        pub const ENCODING_LUT_PAIR: TwoBitLUT<PairFn> =
            TwoBitLUT::new(self::is_okay_encoded_pair_ff,
                self::is_okay_encoded_pair_ft, self::is_okay_encoded_pair_tf,
                self::is_okay_encoded_pair_tt);
        /// This table maps bytes to character classes that helps us reduce the size of the
        /// transition table and generate bitmasks
        static UTF8_MAP_BYTE_TO_CHAR_CLASS: [u8; 256] =
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 2, 2, 2, 2, 2, 2, 2, 2,
                    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                    2, 2, 10, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 3, 11,
                    6, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8];
        /// This table is a transition table that maps the combination of a state of the
        /// automaton and a char class to a state
        static UTF8_TRANSITION_MAP: [u8; 108] =
            [0, 12, 24, 36, 60, 96, 84, 12, 12, 12, 48, 72, 12, 12, 12, 12,
                    12, 12, 12, 12, 12, 12, 12, 12, 12, 0, 12, 12, 12, 12, 12,
                    0, 12, 0, 12, 12, 12, 24, 12, 12, 12, 12, 12, 24, 12, 24,
                    12, 12, 12, 12, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12, 12,
                    24, 12, 12, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12, 12, 12,
                    12, 12, 12, 36, 12, 36, 12, 12, 12, 36, 12, 12, 12, 12, 12,
                    36, 12, 36, 12, 12, 12, 36, 12, 12, 12, 12, 12, 12, 12, 12,
                    12, 12];
        pub const fn pair_is_okay_encoded_iter_ff(_inp: &AnyArrayIter<'_>)
            -> bool {
            true
        }
        pub fn pair_is_okay_encoded_iter_ft(inp: &AnyArrayIter<'_>) -> bool {
            unsafe {
                let mut vptr = inp.as_ptr().add(1);
                let eptr = inp.as_ptr().add(inp.len());
                let mut state = true;
                while vptr < eptr && state {
                    state = self::is_utf8((*vptr).as_slice());
                    vptr = vptr.add(2);
                }
                state
            }
        }
        pub fn pair_is_okay_encoded_iter_tf(inp: &AnyArrayIter<'_>) -> bool {
            unsafe {
                let mut kptr = inp.as_ptr();
                let eptr = kptr.add(inp.len());
                let mut state = true;
                while kptr < eptr && state {
                    state = self::is_utf8((*kptr).as_slice());
                    kptr = kptr.add(2);
                }
                state
            }
        }
        pub fn pair_is_okay_encoded_iter_tt(inp: &AnyArrayIter<'_>) -> bool {
            unsafe {
                let mut kptr = inp.as_ptr();
                let mut vptr = inp.as_ptr().add(1);
                let eptr = kptr.add(inp.len());
                let mut state = true;
                while vptr < eptr && state {
                    state =
                        self::is_utf8((*kptr).as_slice()) &&
                            self::is_utf8((*vptr).as_slice());
                    kptr = kptr.add(2);
                    vptr = vptr.add(2);
                }
                state
            }
        }
        pub fn is_okay_encoded_iter(mut inp: BorrowedAnyArrayIter<'_>)
            -> bool {
            inp.all(self::is_okay_encoded)
        }
        pub const fn is_okay_no_encoding_iter(_inp: BorrowedAnyArrayIter<'_>)
            -> bool {
            true
        }
        pub fn is_okay_encoded(inp: &[u8]) -> bool { self::is_utf8(inp) }
        pub const fn is_okay_no_encoding(_inp: &[u8]) -> bool { true }
        pub fn is_okay_encoded_pair_tt(a: &[u8], b: &[u8]) -> bool {
            is_okay_encoded(a) && is_okay_encoded(b)
        }
        pub fn is_okay_encoded_pair_tf(a: &[u8], _b: &[u8]) -> bool {
            is_okay_encoded(a)
        }
        pub fn is_okay_encoded_pair_ft(_a: &[u8], b: &[u8]) -> bool {
            is_okay_encoded(b)
        }
        pub const fn is_okay_encoded_pair_ff(_a: &[u8], _b: &[u8]) -> bool {
            true
        }
        macro_rules! utf_transition {
            ($idx : expr) => { ucidx! (UTF8_TRANSITION_MAP, $idx) } ;
        }
        macro_rules! utfmap {
            ($idx : expr) => { ucidx! (UTF8_MAP_BYTE_TO_CHAR_CLASS, $idx) } ;
        }
        /// This method uses a dual-stream deterministic finite automaton
        /// [(DFA)](https://en.wikipedia.org/wiki/Deterministic_finite_automaton) that is used to validate
        /// UTF-8 bytes that use the encoded finite state machines defined in this module.
        ///
        /// ## Tradeoffs
        /// Read my comment in the source code (or above if you are not browsing rustdoc)
        ///
        /// ## Why
        /// This function gives us as much as a ~300% improvement over std's validation algorithm
        pub fn is_utf8(bytes: impl AsRef<[u8]>) -> bool {
            let bytes = bytes.as_ref();
            let mut half = bytes.len() / 2;
            unsafe {
                while *(bytes.as_ptr().add(half as usize)) <= 0xBF &&
                            *(bytes.as_ptr().add(half as usize)) >= 0x80 && half > 0 {
                    half -= 1;
                }
            }
            let (mut fsm_state_1, mut fsm_state_2) = (0u8, 0u8);
            let mut i = 0usize;
            let mut j = half;
            while i < half {
                unsafe {
                    fsm_state_1 =
                        *(UTF8_TRANSITION_MAP.as_ptr().add((fsm_state_1 +
                                                (*(UTF8_MAP_BYTE_TO_CHAR_CLASS.as_ptr().add((*(bytes.as_ptr().add(i
                                                                                        as usize))) as usize)))) as usize));
                    fsm_state_2 =
                        *(UTF8_TRANSITION_MAP.as_ptr().add((fsm_state_2 +
                                                (*(UTF8_MAP_BYTE_TO_CHAR_CLASS.as_ptr().add(*(bytes.as_ptr().add(j
                                                                                    as usize)) as usize)))) as usize));
                }
                i += 1;
                j += 1;
            }
            let mut j = half * 2;
            while j < bytes.len() {
                unsafe {
                    fsm_state_2 =
                        *(UTF8_TRANSITION_MAP.as_ptr().add((fsm_state_2 +
                                                (*(UTF8_MAP_BYTE_TO_CHAR_CLASS.as_ptr().add(*(bytes.as_ptr().add(j
                                                                                    as usize)) as usize)))) as usize));
                }
                j += 1;
            }
            fsm_state_1 == 0 && fsm_state_2 == 0
        }
    }
    use self::encoding::{ENCODING_LUT, ENCODING_LUT_PAIR};
    use crate::corestore::{
        booltable::BoolTable, htable::Coremap, map::bref::Ref, Data,
    };
    use crate::util::compiler;
    use parking_lot::RwLock;
    pub type KVEStandard = KVEngine<Data>;
    pub type KVEListmap = KVEngine<LockedVec>;
    pub type LockedVec = RwLock<Vec<Data>>;
    pub type SingleEncoder = fn(&[u8]) -> bool;
    pub type DoubleEncoder = fn(&[u8], &[u8]) -> bool;
    type EntryRef<'a, T> = Ref<'a, Data, T>;
    type EncodingResult<T> = Result<T, ()>;
    type OptionRef<'a, T> = Option<Ref<'a, Data, T>>;
    type EncodingResultRef<'a, T> = EncodingResult<OptionRef<'a, T>>;
    const TSYMBOL_LUT: BoolTable<u8> = BoolTable::new(b'+', b'?');
    pub trait KVEValue {
        fn verify_encoding(&self, e_v: bool)
        -> EncodingResult<()>;
    }
    impl KVEValue for Data {
        fn verify_encoding(&self, e_v: bool) -> EncodingResult<()> {
            if ENCODING_LUT[e_v](self) { Ok(()) } else { Err(()) }
        }
    }
    impl KVEValue for LockedVec {
        fn verify_encoding(&self, e_v: bool) -> EncodingResult<()> {
            let func = ENCODING_LUT[e_v];
            if self.read().iter().all(|v| func(v)) { Ok(()) } else { Err(()) }
        }
    }
    pub struct KVEngine<T> {
        data: Coremap<Data, T>,
        e_k: bool,
        e_v: bool,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl<T: ::core::fmt::Debug> ::core::fmt::Debug for KVEngine<T> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                KVEngine {
                    data: ref __self_0_0,
                    e_k: ref __self_0_1,
                    e_v: ref __self_0_2 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "KVEngine");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "data",
                            &&(*__self_0_0));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "e_k",
                            &&(*__self_0_1));
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "e_v",
                            &&(*__self_0_2));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl<T> KVEngine<T> {
        /// Create a new KVEBlob
        pub fn new(e_k: bool, e_v: bool, data: Coremap<Data, T>) -> Self {
            Self { data, e_k, e_v }
        }
        /// Create a new empty KVEBlob
        pub fn init(e_k: bool, e_v: bool) -> Self {
            Self::new(e_k, e_v, Default::default())
        }
        /// Number of KV pairs
        pub fn len(&self) -> usize { self.data.len() }
        /// Delete all the key/value pairs
        pub fn truncate_table(&self) { self.data.clear() }
        /// Returns a reference to the inner structure
        pub fn get_inner_ref(&self) -> &Coremap<Data, T> { &self.data }
        /// Check the encoding of the key
        pub fn is_key_ok(&self, key: &[u8]) -> bool {
            self._check_encoding(key, self.e_k)
        }
        /// Check the encoding of the value
        pub fn is_val_ok(&self, val: &[u8]) -> bool {
            self._check_encoding(val, self.e_v)
        }
        #[inline(always)]
        fn check_key_encoding(&self, item: &[u8]) -> Result<(), ()> {
            self.check_encoding(item, self.e_k)
        }
        #[inline(always)]
        fn check_value_encoding(&self, item: &[u8]) -> Result<(), ()> {
            self.check_encoding(item, self.e_v)
        }
        #[inline(always)]
        fn _check_encoding(&self, item: &[u8], encoded: bool) -> bool {
            ENCODING_LUT[encoded](item)
        }
        #[inline(always)]
        fn check_encoding(&self, item: &[u8], encoded: bool)
            -> Result<(), ()> {
            if compiler::likely(self._check_encoding(item, encoded)) {
                    Ok(())
                } else { Err(()) }
        }
        pub fn is_key_encoded(&self) -> bool { self.e_k }
        pub fn is_val_encoded(&self) -> bool { self.e_v }
        /// Get the key tsymbol
        pub fn get_key_tsymbol(&self) -> u8 { TSYMBOL_LUT[self.e_k] }
        /// Get the value tsymbol
        pub fn get_value_tsymbol(&self) -> u8 { TSYMBOL_LUT[self.e_v] }
        /// Returns (k_enc, v_enc)
        pub fn get_encoding_tuple(&self) -> (bool, bool) {
            (self.e_k, self.e_v)
        }
        /// Returns an encoder fnptr for the key
        pub fn get_key_encoder(&self) -> SingleEncoder {
            ENCODING_LUT[self.e_k]
        }
        /// Returns an encoder fnptr for the value
        pub fn get_val_encoder(&self) -> SingleEncoder {
            ENCODING_LUT[self.e_v]
        }
    }
    impl<T: KVEValue> KVEngine<T> {
        /// Get the value of the given key
        pub fn get<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResultRef<T> {
            self.check_key_encoding(key.as_ref()).map(|_|
                    self.get_unchecked(key))
        }
        /// Get the value of the given key without any encoding checks
        pub fn get_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> OptionRef<T> {
            self.data.get(key.as_ref())
        }
        /// Set the value of the given key
        pub fn set(&self, key: Data, val: T) -> EncodingResult<bool> {
            self.check_key_encoding(&key).and_then(|_|
                        val.verify_encoding(self.e_v)).map(|_|
                    self.set_unchecked(key, val))
        }
        /// Same as set, but doesn't check encoding. Caller must check encoding
        pub fn set_unchecked(&self, key: Data, val: T) -> bool {
            self.data.true_if_insert(key, val)
        }
        /// Check if the provided key exists
        pub fn exists<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResult<bool> {
            self.check_key_encoding(key.as_ref())?;
            Ok(self.exists_unchecked(key.as_ref()))
        }
        pub fn exists_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> bool {
            self.data.contains_key(key.as_ref())
        }
        /// Update the value of an existing key. Returns `true` if updated
        pub fn update(&self, key: Data, val: T) -> EncodingResult<bool> {
            self.check_key_encoding(&key)?;
            val.verify_encoding(self.e_v)?;
            Ok(self.update_unchecked(key, val))
        }
        /// Update the value of an existing key without encoding checks
        pub fn update_unchecked(&self, key: Data, val: T) -> bool {
            self.data.true_if_update(key, val)
        }
        /// Update or insert an entry
        pub fn upsert(&self, key: Data, val: T) -> EncodingResult<()> {
            self.check_key_encoding(&key)?;
            val.verify_encoding(self.e_v)?;
            self.upsert_unchecked(key, val);
            Ok(())
        }
        /// Update or insert an entry without encoding checks
        pub fn upsert_unchecked(&self, key: Data, val: T) {
            self.data.upsert(key, val)
        }
        /// Remove an entry
        pub fn remove<Q: AsRef<[u8]>>(&self, key: Q) -> EncodingResult<bool> {
            self.check_key_encoding(key.as_ref())?;
            Ok(self.remove_unchecked(key))
        }
        /// Remove an entry without encoding checks
        pub fn remove_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> bool {
            self.data.true_if_removed(key.as_ref())
        }
        /// Pop an entry
        pub fn pop<Q: AsRef<[u8]>>(&self, key: Q)
            -> EncodingResult<Option<T>> {
            self.check_key_encoding(key.as_ref())?;
            Ok(self.pop_unchecked(key))
        }
        /// Pop an entry without encoding checks
        pub fn pop_unchecked<Q: AsRef<[u8]>>(&self, key: Q) -> Option<T> {
            self.data.remove(key.as_ref()).map(|(_, v)| v)
        }
    }
    impl<T: Clone> KVEngine<T> {
        pub fn get_cloned<Q: AsRef<[u8]>>(&self, key: Q)
            -> EncodingResult<Option<T>> {
            self.check_key_encoding(key.as_ref())?;
            Ok(self.get_cloned_unchecked(key.as_ref()))
        }
        pub fn get_cloned_unchecked<Q: AsRef<[u8]>>(&self, key: Q)
            -> Option<T> {
            self.data.get_cloned(key.as_ref())
        }
    }
    impl KVEStandard {
        pub fn take_snapshot_unchecked<Q: AsRef<[u8]>>(&self, key: Q)
            -> Option<Data> {
            self.data.get_cloned(key.as_ref())
        }
        /// Returns an encoder that checks each key and each value in turn
        /// Usual usage:
        /// ```notest
        /// for (k, v) in samples {
        ///     assert!(kve.get_double_encoder(k, v))
        /// }
        /// ```
        pub fn get_double_encoder(&self) -> DoubleEncoder {
            ENCODING_LUT_PAIR[(self.e_k, self.e_v)]
        }
    }
    impl KVEListmap {
        pub fn list_len(&self, listname: &[u8])
            -> EncodingResult<Option<usize>> {
            self.check_key_encoding(listname)?;
            Ok(self.data.get(listname).map(|list| list.read().len()))
        }
        pub fn list_cloned(&self, listname: &[u8], count: usize)
            -> EncodingResult<Option<Vec<Data>>> {
            self.check_key_encoding(listname)?;
            Ok(self.data.get(listname).map(|list|
                        list.read().iter().take(count).cloned().collect()))
        }
        pub fn list_cloned_full(&self, listname: &[u8])
            -> EncodingResult<Option<Vec<Data>>> {
            self.check_key_encoding(listname)?;
            Ok(self.data.get(listname).map(|list|
                        list.read().iter().cloned().collect()))
        }
    }
    impl<T> Default for KVEngine<T> {
        fn default() -> Self { Self::init(false, false) }
    }
}
mod protocol {
    use {crate::corestore::heap_array::HeapArray, core::{fmt, slice}};
    pub mod interface {
        use super::ParseError;
        use crate::{
            corestore::{
                booltable::{BytesBoolTable, BytesNicheLUT},
                buffers::Integer64,
            },
            dbnet::connection::{
                QueryResult, QueryWithAdvance, RawConnection, Stream,
            },
            util::FutureResult, IoResult,
        };
        use std::io::{Error as IoError, ErrorKind};
        use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
        /// The `ProtocolSpec` trait is used to define the character set and pre-generated elements
        /// and responses for a protocol version. To make any actual use of it, you need to implement
        /// both the `ProtocolRead` and `ProtocolWrite` for the protocol
        pub trait ProtocolSpec: Send + Sync {
            /// The Skyhash protocol version
            const PROTOCOL_VERSION: f32;
            /// The Skyhash protocol version string (Skyhash-x.y)
            const PROTOCOL_VERSIONSTRING: &'static str;
            /// Type symbol for unicode strings
            const TSYMBOL_STRING: u8;
            /// Type symbol for blobs
            const TSYMBOL_BINARY: u8;
            /// Type symbol for float
            const TSYMBOL_FLOAT: u8;
            /// Type symbok for int64
            const TSYMBOL_INT64: u8;
            /// Type symbol for typed array
            const TSYMBOL_TYPED_ARRAY: u8;
            /// Type symbol for typed non-null array
            const TSYMBOL_TYPED_NON_NULL_ARRAY: u8;
            /// Type symbol for an array
            const TSYMBOL_ARRAY: u8;
            /// Type symbol for a flat array
            const TSYMBOL_FLAT_ARRAY: u8;
            /// The line-feed character or separator
            const LF: u8 = b'\n';
            /// The header for simple queries
            const SIMPLE_QUERY_HEADER: &'static [u8];
            /// The header for pipelined queries (excluding length, obviously)
            const PIPELINED_QUERY_FIRST_BYTE: u8;
            /// Null element represenation for a typed array
            const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8];
            /// Respcode 0: Okay
            const RCODE_OKAY: &'static [u8];
            /// Respcode 1: Nil
            const RCODE_NIL: &'static [u8];
            /// Respcode 2: Overwrite error
            const RCODE_OVERWRITE_ERR: &'static [u8];
            /// Respcode 3: Action error
            const RCODE_ACTION_ERR: &'static [u8];
            /// Respcode 4: Packet error
            const RCODE_PACKET_ERR: &'static [u8];
            /// Respcode 5: Server error
            const RCODE_SERVER_ERR: &'static [u8];
            /// Respcode 6: Other error
            const RCODE_OTHER_ERR_EMPTY: &'static [u8];
            /// Respcode 7: Unknown action
            const RCODE_UNKNOWN_ACTION: &'static [u8];
            /// Respcode 8: Wrongtype error
            const RCODE_WRONGTYPE_ERR: &'static [u8];
            /// Respcode 9: Unknown data type error
            const RCODE_UNKNOWN_DATA_TYPE: &'static [u8];
            /// Respcode 10: Encoding error
            const RCODE_ENCODING_ERROR: &'static [u8];
            /// Respstring when snapshot engine is busy
            const RSTRING_SNAPSHOT_BUSY: &'static [u8];
            /// Respstring when snapshots are disabled
            const RSTRING_SNAPSHOT_DISABLED: &'static [u8];
            /// Respstring when duplicate snapshot creation is attempted
            const RSTRING_SNAPSHOT_DUPLICATE: &'static [u8];
            /// Respstring when snapshot has illegal chars
            const RSTRING_SNAPSHOT_ILLEGAL_NAME: &'static [u8];
            /// Respstring when a **very bad error** happens (use after termsig)
            const RSTRING_ERR_ACCESS_AFTER_TERMSIG: &'static [u8];
            /// Respstring when the default container is unset
            const RSTRING_DEFAULT_UNSET: &'static [u8];
            /// Respstring when the container is not found
            const RSTRING_CONTAINER_NOT_FOUND: &'static [u8];
            /// Respstring when the container is still in use, but a _free_ op is attempted
            const RSTRING_STILL_IN_USE: &'static [u8];
            /// Respstring when a protected container is attempted to be accessed/modified
            const RSTRING_PROTECTED_OBJECT: &'static [u8];
            /// Respstring when an action is not suitable for the current table model
            const RSTRING_WRONG_MODEL: &'static [u8];
            /// Respstring when the container already exists
            const RSTRING_ALREADY_EXISTS: &'static [u8];
            /// Respstring when the container is not ready
            const RSTRING_NOT_READY: &'static [u8];
            /// Respstring when a DDL transaction fails
            const RSTRING_DDL_TRANSACTIONAL_FAILURE: &'static [u8];
            /// Respstring when an unknow DDL query is run (`CREATE BLAH`, for example)
            const RSTRING_UNKNOWN_DDL_QUERY: &'static [u8];
            /// Respstring when a bad DDL expression is run
            const RSTRING_BAD_EXPRESSION: &'static [u8];
            /// Respstring when an unsupported model is attempted to be used during table creation
            const RSTRING_UNKNOWN_MODEL: &'static [u8];
            /// Respstring when too many arguments are passed to a DDL query
            const RSTRING_TOO_MANY_ARGUMENTS: &'static [u8];
            /// Respstring when the container name is too long
            const RSTRING_CONTAINER_NAME_TOO_LONG: &'static [u8];
            /// Respstring when the container name
            const RSTRING_BAD_CONTAINER_NAME: &'static [u8];
            /// Respstring when an unknown inspect query is run (`INSPECT blah`, for example)
            const RSTRING_UNKNOWN_INSPECT_QUERY: &'static [u8];
            /// Respstring when an unknown table property is passed during table creation
            const RSTRING_UNKNOWN_PROPERTY: &'static [u8];
            /// Respstring when a non-empty keyspace is attempted to be dropped
            const RSTRING_KEYSPACE_NOT_EMPTY: &'static [u8];
            /// Respstring when a bad type is provided for a key in the K/V engine (like using a `list`
            /// for the key)
            const RSTRING_BAD_TYPE_FOR_KEY: &'static [u8];
            /// Respstring when a non-existent index is attempted to be accessed in a list
            const RSTRING_LISTMAP_BAD_INDEX: &'static [u8];
            /// Respstring when a list is empty and we attempt to access/modify it
            const RSTRING_LISTMAP_LIST_IS_EMPTY: &'static [u8];
            /// A string element containing the text "HEY!"
            const ELEMRESP_HEYA: &'static [u8];
            /// A **full response** for a packet error
            const FULLRESP_RCODE_PACKET_ERR: &'static [u8];
            /// A **full response** for a wrongtype error
            const FULLRESP_RCODE_WRONG_TYPE: &'static [u8];
            /// A LUT for SET operations
            const SET_NLUT: BytesNicheLUT =
                BytesNicheLUT::new(Self::RCODE_ENCODING_ERROR,
                    Self::RCODE_OKAY, Self::RCODE_OVERWRITE_ERR);
            /// A LUT for lists
            const OKAY_BADIDX_NIL_NLUT: BytesNicheLUT =
                BytesNicheLUT::new(Self::RCODE_NIL, Self::RCODE_OKAY,
                    Self::RSTRING_LISTMAP_BAD_INDEX);
            /// A LUT for SET operations
            const OKAY_OVW_BLUT: BytesBoolTable =
                BytesBoolTable::new(Self::RCODE_OKAY,
                    Self::RCODE_OVERWRITE_ERR);
            /// A LUT for UPDATE operations
            const UPDATE_NLUT: BytesNicheLUT =
                BytesNicheLUT::new(Self::RCODE_ENCODING_ERROR,
                    Self::RCODE_OKAY, Self::RCODE_NIL);
            /// respstring: already claimed (user was already claimed)
            const AUTH_ERROR_ALREADYCLAIMED: &'static [u8];
            /// respcode(10): bad credentials (either bad creds or invalid user)
            const AUTH_CODE_BAD_CREDENTIALS: &'static [u8];
            /// respstring: auth is disabled
            const AUTH_ERROR_DISABLED: &'static [u8];
            /// respcode(11): Insufficient permissions (same for anonymous user)
            const AUTH_CODE_PERMS: &'static [u8];
            /// respstring: ID is too long
            const AUTH_ERROR_ILLEGAL_USERNAME: &'static [u8];
            /// respstring: ID is protected/in use
            const AUTH_ERROR_FAILED_TO_DELETE_USER: &'static [u8];
        }
        /// # The `ProtocolRead` trait
        ///
        /// The `ProtocolRead` trait enables read operations using the protocol for a given stream `Strm` and protocol
        /// `P`. Both the stream and protocol must implement the appropriate traits for you to be able to use these
        /// traits
        ///
        /// ## DO NOT
        /// The fact that this is a trait enables great flexibility in terms of visibility, but **DO NOT EVER CALL any
        /// function other than `read_query`, `close_conn_with_error` or `write_response`**. If you mess with functions
        /// like `read_again`, you're likely to pull yourself into some good trouble.
        pub trait ProtocolRead<P, Strm>: RawConnection<P, Strm> where
            Strm: Stream, P: ProtocolSpec {
            /// Try to parse a query from the buffered data
            fn try_query(&self)
            -> Result<QueryWithAdvance, ParseError>;
            /// Read a query from the remote end
            ///
            /// This function asynchronously waits until all the data required
            /// for parsing the query is available
            fn read_query<'s, 'r: 's>(&'r mut self)
                -> FutureResult<'s, Result<QueryResult, IoError>> {
                Box::pin(async move
                        {
                        let mv_self = self;
                        loop {
                            let (buffer, stream) = mv_self.get_mut_both();
                            match stream.read_buf(buffer).await {
                                Ok(0) => {
                                    if buffer.is_empty() {
                                            return Ok(QueryResult::Disconnected);
                                        } else {
                                           return Err(IoError::from(ErrorKind::ConnectionReset));
                                       }
                                }
                                Ok(_) => {}
                                Err(e) => return Err(e),
                            }
                            match mv_self.try_query() {
                                Ok(query_with_advance) => {
                                    return Ok(QueryResult::Q(query_with_advance));
                                }
                                Err(ParseError::NotEnough) => (),
                                Err(ParseError::DatatypeParseFailure) =>
                                    return Ok(QueryResult::Wrongtype),
                                Err(ParseError::UnexpectedByte | ParseError::BadPacket) => {
                                    return Ok(QueryResult::E(P::FULLRESP_RCODE_PACKET_ERR));
                                }
                                Err(ParseError::WrongType) => {
                                    return Ok(QueryResult::E(P::FULLRESP_RCODE_WRONG_TYPE));
                                }
                            }
                        }
                    })
            }
        }
        pub trait ProtocolWrite<P, Strm>: RawConnection<P, Strm> where
            Strm: Stream, P: ProtocolSpec {
            fn _get_raw_stream(&mut self) -> &mut BufWriter<Strm> {
                self.get_mut_stream()
            }
            fn _flush_stream<'life0, 'ret_life>(&'life0 mut self)
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move { self.get_mut_stream().flush().await })
            }
            fn _write_raw<'life0, 'life1,
                'ret_life>(&'life0 mut self, data: &'life1 [u8])
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, 'life1: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        { self.get_mut_stream().write_all(data).await })
            }
            fn _write_raw_flushed<'life0, 'life1,
                'ret_life>(&'life0 mut self, data: &'life1 [u8])
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, 'life1: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        {
                        self._write_raw(data).await?;
                        self._flush_stream().await
                    })
            }
            fn close_conn_with_error<'life0, 'life1,
                'ret_life>(&'life0 mut self, resp: &'life1 [u8])
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, 'life1: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move { self._write_raw_flushed(resp).await })
            }
            fn write_simple_query_header<'life0, 'ret_life>(&'life0 mut self)
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        {
                        self.get_mut_stream().write_all(P::SIMPLE_QUERY_HEADER).await
                    })
            }
            fn write_pipelined_query_header<'life0,
                'ret_life>(&'life0 mut self, qcount: usize)
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        {
                        self.get_mut_stream().write_all(&[P::PIPELINED_QUERY_FIRST_BYTE]).await?;
                        self.get_mut_stream().write_all(&Integer64::from(qcount)).await?;
                        self.get_mut_stream().write_all(&[P::LF]).await
                    })
            }
            fn write_mono_length_prefixed_with_tsymbol<'life0, 'life1,
            'ret_life>(&'life0 mut self, data: &'life1 [u8], tsymbol: u8)
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            'life1: 'ret_life,
            Self: Send +
            'ret_life;
            /// serialize and write an `&str` to the stream
            fn write_string<'life0, 'life1,
            'ret_life>(&'life0 mut self, string: &'life1 str)
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            'life1: 'ret_life,
            Self: 'ret_life;
            /// serialize and write an `&[u8]` to the stream
            fn write_binary<'life0, 'life1,
            'ret_life>(&'life0 mut self, binary: &'life1 [u8])
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            'life1: 'ret_life,
            Self: 'ret_life;
            /// serialize and write an `usize` to the stream
            fn write_usize<'life0, 'ret_life>(&'life0 mut self, size: usize)
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            Self: 'ret_life;
            /// serialize and write an `u64` to the stream
            fn write_int64<'life0, 'ret_life>(&'life0 mut self, int: u64)
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            Self: 'ret_life;
            /// serialize and write an `f32` to the stream
            fn write_float<'life0, 'ret_life>(&'life0 mut self, float: f32)
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            Self: 'ret_life;
            fn write_typed_array_header<'life0,
                'ret_life>(&'life0 mut self, len: usize, tsymbol: u8)
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        {
                        self.get_mut_stream().write_all(&[P::TSYMBOL_TYPED_ARRAY,
                                                tsymbol]).await?;
                        self.get_mut_stream().write_all(&Integer64::from(len)).await?;
                        self.get_mut_stream().write_all(&[P::LF]).await?;
                        Ok(())
                    })
            }
            fn write_typed_array_element_null<'life0,
                'ret_life>(&'life0 mut self)
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        {
                        self.get_mut_stream().write_all(P::TYPE_TYPED_ARRAY_ELEMENT_NULL).await
                    })
            }
            fn write_typed_array_element<'life0, 'life1,
            'ret_life>(&'life0 mut self, element: &'life1 [u8])
            -> FutureResult<'ret_life, IoResult<()>>
            where
            'life0: 'ret_life,
            'life1: 'ret_life,
            Self: 'ret_life;
            fn write_typed_non_null_array_header<'life0,
                'ret_life>(&'life0 mut self, len: usize, tsymbol: u8)
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        {
                        self.get_mut_stream().write_all(&[P::TSYMBOL_TYPED_NON_NULL_ARRAY,
                                                tsymbol]).await?;
                        self.get_mut_stream().write_all(&Integer64::from(len)).await?;
                        self.get_mut_stream().write_all(&[P::LF]).await?;
                        Ok(())
                    })
            }
            fn write_typed_non_null_array_element<'life0, 'life1,
                'ret_life>(&'life0 mut self, element: &'life1 [u8])
                -> FutureResult<'ret_life, IoResult<()>> where
                'life0: 'ret_life, 'life1: 'ret_life, Self: Send + 'ret_life {
                Box::pin(async move
                        { self.write_typed_array_element(element).await })
            }
        }
    }
    pub mod iter {
        use super::UnsafeSlice;
        use bytes::Bytes;
        use core::{
            hint::unreachable_unchecked, iter::FusedIterator, ops::Deref,
            slice::Iter,
        };
        /// An iterator over an [`AnyArray`] (an [`UnsafeSlice`]). The validity of the iterator is
        /// left to the caller who has to guarantee:
        /// - Source pointers for the unsafe slice are valid
        /// - Source pointers exist as long as this iterator is used
        pub struct AnyArrayIter<'a> {
            iter: Iter<'a, UnsafeSlice>,
        }
        /// Same as [`AnyArrayIter`] with the exception that it directly dereferences to the actual
        /// slice iterator
        pub struct BorrowedAnyArrayIter<'a> {
            iter: Iter<'a, UnsafeSlice>,
        }
        impl<'a> Deref for BorrowedAnyArrayIter<'a> {
            type Target = Iter<'a, UnsafeSlice>;
            #[inline(always)]
            fn deref(&self) -> &Self::Target { &self.iter }
        }
        impl<'a> AnyArrayIter<'a> {
            /// Create a new `AnyArrayIter`.
            ///
            /// ## Safety
            /// - Valid source pointers
            /// - Source pointers exist as long as the iterator is used
            #[inline(always)]
            pub const unsafe fn new(iter: Iter<'a, UnsafeSlice>)
                -> AnyArrayIter<'a> {
                Self { iter }
            }
            /// Check if the iter is empty
            #[inline(always)]
            pub fn is_empty(&self) -> bool {
                ExactSizeIterator::len(self) == 0
            }
            /// Returns a borrowed iterator => simply put, advancing the returned iterator does not
            /// affect the base iterator owned by this object
            #[inline(always)]
            pub fn as_ref(&'a self) -> BorrowedAnyArrayIter<'a> {
                BorrowedAnyArrayIter { iter: self.iter.as_ref().iter() }
            }
            /// Returns the starting ptr of the `AnyArray`
            #[inline(always)]
            pub unsafe fn as_ptr(&self) -> *const UnsafeSlice {
                self.iter.as_ref().as_ptr()
            }
            /// Returns the next value in uppercase
            #[inline(always)]
            pub fn next_uppercase(&mut self) -> Option<Box<[u8]>> {
                self.iter.next().map(|v|
                        {
                            unsafe {
                                        v.as_slice()
                                    }.to_ascii_uppercase().into_boxed_slice()
                        })
            }
            #[inline(always)]
            pub fn next_lowercase(&mut self) -> Option<Box<[u8]>> {
                self.iter.next().map(|v|
                        {
                            unsafe {
                                        v.as_slice()
                                    }.to_ascii_lowercase().into_boxed_slice()
                        })
            }
            #[inline(always)]
            pub unsafe fn next_lowercase_unchecked(&mut self) -> Box<[u8]> {
                self.next_lowercase().unwrap_or_else(||
                        core::hint::unreachable_unchecked())
            }
            #[inline(always)]
            pub unsafe fn next_uppercase_unchecked(&mut self) -> Box<[u8]> {
                match self.next_uppercase() {
                    Some(s) => s,
                    None => { core::hint::unreachable_unchecked() }
                }
            }
            #[inline(always)]
            /// Returns the next value without any checks
            pub unsafe fn next_unchecked(&mut self) -> &'a [u8] {
                match self.next() {
                    Some(s) => s,
                    None => unreachable_unchecked(),
                }
            }
            #[inline(always)]
            /// Returns the next value without any checks as an owned copy of [`Bytes`]
            pub unsafe fn next_unchecked_bytes(&mut self) -> Bytes {
                Bytes::copy_from_slice(self.next_unchecked())
            }
            #[inline(always)]
            pub fn map_next<T>(&mut self, cls: fn(&[u8]) -> T) -> Option<T> {
                self.next().map(cls)
            }
            #[inline(always)]
            pub fn next_string_owned(&mut self) -> Option<String> {
                self.map_next(|v| String::from_utf8_lossy(v).to_string())
            }
            #[inline(always)]
            pub unsafe fn into_inner(self) -> Iter<'a, UnsafeSlice> {
                self.iter
            }
        }
        /// # Safety
        /// Caller must ensure validity of the slice returned
        pub unsafe trait DerefUnsafeSlice {
            unsafe fn deref_slice(&self)
            -> &[u8];
        }
        unsafe impl DerefUnsafeSlice for UnsafeSlice {
            #[inline(always)]
            unsafe fn deref_slice(&self) -> &[u8] { self.as_slice() }
        }
        impl<'a> Iterator for AnyArrayIter<'a> {
            type Item = &'a [u8];
            #[inline(always)]
            fn next(&mut self) -> Option<Self::Item> {
                self.iter.next().map(|v| unsafe { v.as_slice() })
            }
            #[inline(always)]
            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
        impl<'a> DoubleEndedIterator for AnyArrayIter<'a> {
            #[inline(always)]
            fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
                self.iter.next_back().map(|v| unsafe { v.as_slice() })
            }
        }
        impl<'a> ExactSizeIterator for AnyArrayIter<'a> {}
        impl<'a> FusedIterator for AnyArrayIter<'a> {}
        impl<'a> Iterator for BorrowedAnyArrayIter<'a> {
            type Item = &'a [u8];
            #[inline(always)]
            fn next(&mut self) -> Option<Self::Item> {
                self.iter.next().map(|v| unsafe { v.as_slice() })
            }
        }
        impl<'a> DoubleEndedIterator for BorrowedAnyArrayIter<'a> {
            #[inline(always)]
            fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
                self.iter.next_back().map(|v| unsafe { v.as_slice() })
            }
        }
        impl<'a> ExactSizeIterator for BorrowedAnyArrayIter<'a> {}
        impl<'a> FusedIterator for BorrowedAnyArrayIter<'a> {}
    }
    mod raw_parser {
        use {
            super::{ParseError, ParseResult, UnsafeSlice},
            core::mem::transmute,
        };
        /// The `RawParser` trait has three methods that implementors must define:
        ///
        /// - `cursor_ptr` -> Should point to the current position in the buffer for the parser
        /// - `cursor_ptr_mut` -> a mutable reference to the cursor
        /// - `data_end_ptr` -> a ptr to one byte past the allocated area of the buffer
        ///
        /// All implementors of `RawParser` get a free implementation for `RawParserMeta` and `RawParserExt`
        ///
        /// # Safety
        /// - `cursor_ptr` must point to a valid location in memory
        /// - `data_end_ptr` must point to a valid location in memory, in the **same allocated area**
        pub(super) unsafe trait RawParser {
            fn cursor_ptr(&self)
            -> *const u8;
            fn cursor_ptr_mut(&mut self)
            -> &mut *const u8;
            fn data_end_ptr(&self)
            -> *const u8;
        }
        /// The `RawParserMeta` trait builds on top of the `RawParser` trait to provide low-level interactions
        /// and information with the parser's buffer. It is implemented for any type that implements the `RawParser`
        /// trait. Manual implementation is discouraged
        pub(super) trait RawParserMeta: RawParser {
            /// Check how many bytes we have left
            fn remaining(&self) -> usize {
                self.data_end_ptr() as usize - self.cursor_ptr() as usize
            }
            /// Check if we have `size` bytes remaining
            fn has_remaining(&self, size: usize) -> bool {
                self.remaining() >= size
            }
            /// Check if we have exhausted the buffer
            fn exhausted(&self) -> bool {
                self.cursor_ptr() >= self.data_end_ptr()
            }
            /// Check if the buffer is not exhausted
            fn not_exhausted(&self) -> bool {
                self.cursor_ptr() < self.data_end_ptr()
            }
            /// Attempts to return the byte pointed at by the cursor.
            /// WARNING: The same segfault warning
            unsafe fn get_byte_at_cursor(&self) -> u8 { *self.cursor_ptr() }
            /// Increment the cursor by `by` positions
            unsafe fn incr_cursor_by(&mut self, by: usize) {
                let current = *self.cursor_ptr_mut();
                *self.cursor_ptr_mut() = current.add(by);
            }
            /// Increment the position of the cursor by one position
            unsafe fn incr_cursor(&mut self) { self.incr_cursor_by(1); }
        }
        impl<T> RawParserMeta for T where T: RawParser {}
        /// `RawParserExt` builds on the `RawParser` and `RawParserMeta` traits to provide high level abstractions
        /// like reading lines, or a slice of a given length. It is implemented for any type that
        /// implements the `RawParser` trait. Manual implementation is discouraged
        pub(super) trait RawParserExt: RawParser + RawParserMeta {
            /// Attempt to read `len` bytes
            fn read_until(&mut self, len: usize) -> ParseResult<UnsafeSlice> {
                if self.has_remaining(len) {
                        unsafe {
                            let slice = UnsafeSlice::new(self.cursor_ptr(), len);
                            self.incr_cursor_by(len);
                            Ok(slice)
                        }
                    } else { Err(ParseError::NotEnough) }
            }
            /// Attempt to read a line, **rejecting an empty payload**
            fn read_line_pedantic(&mut self) -> ParseResult<UnsafeSlice> {
                let start_ptr = self.cursor_ptr();
                unsafe {
                    while self.not_exhausted() &&
                            self.get_byte_at_cursor() != b'\n' {
                        self.incr_cursor();
                    }
                    let len = self.cursor_ptr() as usize - start_ptr as usize;
                    let has_lf =
                        self.not_exhausted() && self.get_byte_at_cursor() == b'\n';
                    if has_lf && len != 0 {
                            self.incr_cursor();
                            Ok(UnsafeSlice::new(start_ptr, len))
                        } else { Err(transmute(has_lf)) }
                }
            }
            /// Attempt to read an `usize` from the buffer
            fn read_usize(&mut self) -> ParseResult<usize> {
                let line = self.read_line_pedantic()?;
                let bytes = unsafe { line.as_slice() };
                let mut ret = 0usize;
                for byte in bytes {
                    if byte.is_ascii_digit() {
                            ret =
                                match ret.checked_mul(10) {
                                    Some(r) => r,
                                    None => return Err(ParseError::DatatypeParseFailure),
                                };
                            ret =
                                match ret.checked_add((byte & 0x0F) as _) {
                                    Some(r) => r,
                                    None => return Err(ParseError::DatatypeParseFailure),
                                };
                        } else { return Err(ParseError::DatatypeParseFailure); }
                }
                Ok(ret)
            }
        }
        impl<T> RawParserExt for T where T: RawParser + RawParserMeta {}
    }
    mod v1 {
        use {
            super::{
                raw_parser::{RawParser, RawParserExt, RawParserMeta},
                ParseError, ParseResult, PipelinedQuery, Query, SimpleQuery,
                UnsafeSlice,
            },
            crate::{
                corestore::heap_array::{HeapArray, HeapArrayWriter},
                dbnet::connection::QueryWithAdvance,
            },
        };
        mod interface_impls {
            use {
                crate::{
                    corestore::buffers::Integer64,
                    dbnet::connection::{
                        QueryWithAdvance, RawConnection, Stream,
                    },
                    protocol::{
                        interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
                        ParseError, Skyhash1,
                    },
                    util::FutureResult, IoResult,
                },
                ::sky_macros::compiled_eresp_bytes_v1 as eresp,
                tokio::io::AsyncWriteExt,
            };
            impl ProtocolSpec for Skyhash1 {
                const PROTOCOL_VERSION: f32 = 1.0;
                const PROTOCOL_VERSIONSTRING: &'static str = "Skyhash-1.0";
                const TSYMBOL_STRING: u8 = b'+';
                const TSYMBOL_BINARY: u8 = b'?';
                const TSYMBOL_FLOAT: u8 = b'%';
                const TSYMBOL_INT64: u8 = b':';
                const TSYMBOL_TYPED_ARRAY: u8 = b'@';
                const TSYMBOL_TYPED_NON_NULL_ARRAY: u8 = b'^';
                const TSYMBOL_ARRAY: u8 = b'&';
                const TSYMBOL_FLAT_ARRAY: u8 = b'_';
                const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8] = b"\0";
                const SIMPLE_QUERY_HEADER: &'static [u8] = b"*";
                const PIPELINED_QUERY_FIRST_BYTE: u8 = b'$';
                const RCODE_OKAY: &'static [u8] =
                    &[b'!', 49u8, b'\n', 48u8, b'\n'];
                const RCODE_NIL: &'static [u8] =
                    &[b'!', 49u8, b'\n', 49u8, b'\n'];
                const RCODE_OVERWRITE_ERR: &'static [u8] =
                    &[b'!', 49u8, b'\n', 50u8, b'\n'];
                const RCODE_ACTION_ERR: &'static [u8] =
                    &[b'!', 49u8, b'\n', 51u8, b'\n'];
                const RCODE_PACKET_ERR: &'static [u8] =
                    &[b'!', 49u8, b'\n', 52u8, b'\n'];
                const RCODE_SERVER_ERR: &'static [u8] =
                    &[b'!', 49u8, b'\n', 53u8, b'\n'];
                const RCODE_OTHER_ERR_EMPTY: &'static [u8] =
                    &[b'!', 49u8, b'\n', 54u8, b'\n'];
                const RCODE_UNKNOWN_ACTION: &'static [u8] =
                    &[b'!', 49u8, 52u8, b'\n', 85u8, 110u8, 107u8, 110u8, 111u8,
                                119u8, 110u8, 32u8, 97u8, 99u8, 116u8, 105u8, 111u8, 110u8,
                                b'\n'];
                const RCODE_WRONGTYPE_ERR: &'static [u8] =
                    &[b'!', 49u8, b'\n', 55u8, b'\n'];
                const RCODE_UNKNOWN_DATA_TYPE: &'static [u8] =
                    &[b'!', 49u8, b'\n', 56u8, b'\n'];
                const RCODE_ENCODING_ERROR: &'static [u8] =
                    &[b'!', 49u8, b'\n', 57u8, b'\n'];
                const RSTRING_SNAPSHOT_BUSY: &'static [u8] =
                    &[b'!', 49u8, 55u8, b'\n', 101u8, 114u8, 114u8, 45u8, 115u8,
                                110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 98u8,
                                117u8, 115u8, 121u8, b'\n'];
                const RSTRING_SNAPSHOT_DISABLED: &'static [u8] =
                    &[b'!', 50u8, 49u8, b'\n', 101u8, 114u8, 114u8, 45u8, 115u8,
                                110u8, 97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 100u8,
                                105u8, 115u8, 97u8, 98u8, 108u8, 101u8, 100u8, b'\n'];
                const RSTRING_SNAPSHOT_DUPLICATE: &'static [u8] =
                    &[b'!', 49u8, 56u8, b'\n', 100u8, 117u8, 112u8, 108u8,
                                105u8, 99u8, 97u8, 116u8, 101u8, 45u8, 115u8, 110u8, 97u8,
                                112u8, 115u8, 104u8, 111u8, 116u8, b'\n'];
                const RSTRING_SNAPSHOT_ILLEGAL_NAME: &'static [u8] =
                    &[b'!', 50u8, 53u8, b'\n', 101u8, 114u8, 114u8, 45u8, 105u8,
                                110u8, 118u8, 97u8, 108u8, 105u8, 100u8, 45u8, 115u8, 110u8,
                                97u8, 112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 110u8, 97u8,
                                109u8, 101u8, b'\n'];
                const RSTRING_ERR_ACCESS_AFTER_TERMSIG: &'static [u8] =
                    &[b'!', 50u8, 52u8, b'\n', 101u8, 114u8, 114u8, 45u8, 97u8,
                                99u8, 99u8, 101u8, 115u8, 115u8, 45u8, 97u8, 102u8, 116u8,
                                101u8, 114u8, 45u8, 116u8, 101u8, 114u8, 109u8, 115u8,
                                105u8, 103u8, b'\n'];
                const RSTRING_DEFAULT_UNSET: &'static [u8] =
                    &[b'!', 50u8, 51u8, b'\n', 100u8, 101u8, 102u8, 97u8, 117u8,
                                108u8, 116u8, 45u8, 99u8, 111u8, 110u8, 116u8, 97u8, 105u8,
                                110u8, 101u8, 114u8, 45u8, 117u8, 110u8, 115u8, 101u8,
                                116u8, b'\n'];
                const RSTRING_CONTAINER_NOT_FOUND: &'static [u8] =
                    &[b'!', 49u8, 57u8, b'\n', 99u8, 111u8, 110u8, 116u8, 97u8,
                                105u8, 110u8, 101u8, 114u8, 45u8, 110u8, 111u8, 116u8, 45u8,
                                102u8, 111u8, 117u8, 110u8, 100u8, b'\n'];
                const RSTRING_STILL_IN_USE: &'static [u8] =
                    &[b'!', 49u8, 50u8, b'\n', 115u8, 116u8, 105u8, 108u8,
                                108u8, 45u8, 105u8, 110u8, 45u8, 117u8, 115u8, 101u8,
                                b'\n'];
                const RSTRING_PROTECTED_OBJECT: &'static [u8] =
                    &[b'!', 50u8, 48u8, b'\n', 101u8, 114u8, 114u8, 45u8, 112u8,
                                114u8, 111u8, 116u8, 101u8, 99u8, 116u8, 101u8, 100u8, 45u8,
                                111u8, 98u8, 106u8, 101u8, 99u8, 116u8, b'\n'];
                const RSTRING_WRONG_MODEL: &'static [u8] =
                    &[b'!', 49u8, 49u8, b'\n', 119u8, 114u8, 111u8, 110u8,
                                103u8, 45u8, 109u8, 111u8, 100u8, 101u8, 108u8, b'\n'];
                const RSTRING_ALREADY_EXISTS: &'static [u8] =
                    &[b'!', 49u8, 56u8, b'\n', 101u8, 114u8, 114u8, 45u8, 97u8,
                                108u8, 114u8, 101u8, 97u8, 100u8, 121u8, 45u8, 101u8, 120u8,
                                105u8, 115u8, 116u8, 115u8, b'\n'];
                const RSTRING_NOT_READY: &'static [u8] =
                    &[b'!', 57u8, b'\n', 110u8, 111u8, 116u8, 45u8, 114u8,
                                101u8, 97u8, 100u8, 121u8, b'\n'];
                const RSTRING_DDL_TRANSACTIONAL_FAILURE: &'static [u8] =
                    &[b'!', 50u8, 49u8, b'\n', 116u8, 114u8, 97u8, 110u8, 115u8,
                                97u8, 99u8, 116u8, 105u8, 111u8, 110u8, 97u8, 108u8, 45u8,
                                102u8, 97u8, 105u8, 108u8, 117u8, 114u8, 101u8, b'\n'];
                const RSTRING_UNKNOWN_DDL_QUERY: &'static [u8] =
                    &[b'!', 49u8, 55u8, b'\n', 117u8, 110u8, 107u8, 110u8,
                                111u8, 119u8, 110u8, 45u8, 100u8, 100u8, 108u8, 45u8, 113u8,
                                117u8, 101u8, 114u8, 121u8, b'\n'];
                const RSTRING_BAD_EXPRESSION: &'static [u8] =
                    &[b'!', 50u8, 48u8, b'\n', 109u8, 97u8, 108u8, 102u8, 111u8,
                                114u8, 109u8, 101u8, 100u8, 45u8, 101u8, 120u8, 112u8,
                                114u8, 101u8, 115u8, 115u8, 105u8, 111u8, 110u8, b'\n'];
                const RSTRING_UNKNOWN_MODEL: &'static [u8] =
                    &[b'!', 49u8, 51u8, b'\n', 117u8, 110u8, 107u8, 110u8,
                                111u8, 119u8, 110u8, 45u8, 109u8, 111u8, 100u8, 101u8,
                                108u8, b'\n'];
                const RSTRING_TOO_MANY_ARGUMENTS: &'static [u8] =
                    &[b'!', 49u8, 51u8, b'\n', 116u8, 111u8, 111u8, 45u8, 109u8,
                                97u8, 110u8, 121u8, 45u8, 97u8, 114u8, 103u8, 115u8, b'\n'];
                const RSTRING_CONTAINER_NAME_TOO_LONG: &'static [u8] =
                    &[b'!', 50u8, 51u8, b'\n', 99u8, 111u8, 110u8, 116u8, 97u8,
                                105u8, 110u8, 101u8, 114u8, 45u8, 110u8, 97u8, 109u8, 101u8,
                                45u8, 116u8, 111u8, 111u8, 45u8, 108u8, 111u8, 110u8, 103u8,
                                b'\n'];
                const RSTRING_BAD_CONTAINER_NAME: &'static [u8] =
                    &[b'!', 49u8, 56u8, b'\n', 98u8, 97u8, 100u8, 45u8, 99u8,
                                111u8, 110u8, 116u8, 97u8, 105u8, 110u8, 101u8, 114u8, 45u8,
                                110u8, 97u8, 109u8, 101u8, b'\n'];
                const RSTRING_UNKNOWN_INSPECT_QUERY: &'static [u8] =
                    &[b'!', 50u8, 49u8, b'\n', 117u8, 110u8, 107u8, 110u8,
                                111u8, 119u8, 110u8, 45u8, 105u8, 110u8, 115u8, 112u8,
                                101u8, 99u8, 116u8, 45u8, 113u8, 117u8, 101u8, 114u8, 121u8,
                                b'\n'];
                const RSTRING_UNKNOWN_PROPERTY: &'static [u8] =
                    &[b'!', 49u8, 54u8, b'\n', 117u8, 110u8, 107u8, 110u8,
                                111u8, 119u8, 110u8, 45u8, 112u8, 114u8, 111u8, 112u8,
                                101u8, 114u8, 116u8, 121u8, b'\n'];
                const RSTRING_KEYSPACE_NOT_EMPTY: &'static [u8] =
                    &[b'!', 49u8, 56u8, b'\n', 107u8, 101u8, 121u8, 115u8,
                                112u8, 97u8, 99u8, 101u8, 45u8, 110u8, 111u8, 116u8, 45u8,
                                101u8, 109u8, 112u8, 116u8, 121u8, b'\n'];
                const RSTRING_BAD_TYPE_FOR_KEY: &'static [u8] =
                    &[b'!', 49u8, 54u8, b'\n', 98u8, 97u8, 100u8, 45u8, 116u8,
                                121u8, 112u8, 101u8, 45u8, 102u8, 111u8, 114u8, 45u8, 107u8,
                                101u8, 121u8, b'\n'];
                const RSTRING_LISTMAP_BAD_INDEX: &'static [u8] =
                    &[b'!', 49u8, 52u8, b'\n', 98u8, 97u8, 100u8, 45u8, 108u8,
                                105u8, 115u8, 116u8, 45u8, 105u8, 110u8, 100u8, 101u8,
                                120u8, b'\n'];
                const RSTRING_LISTMAP_LIST_IS_EMPTY: &'static [u8] =
                    &[b'!', 49u8, 51u8, b'\n', 108u8, 105u8, 115u8, 116u8, 45u8,
                                105u8, 115u8, 45u8, 101u8, 109u8, 112u8, 116u8, 121u8,
                                b'\n'];
                const ELEMRESP_HEYA: &'static [u8] = b"+4\nHEY!\n";
                const FULLRESP_RCODE_PACKET_ERR: &'static [u8] =
                    b"*1\n!1\n4\n";
                const FULLRESP_RCODE_WRONG_TYPE: &'static [u8] =
                    b"*1\n!1\n7\n";
                const AUTH_ERROR_ALREADYCLAIMED: &'static [u8] =
                    &[b'!', 50u8, 52u8, b'\n', 101u8, 114u8, 114u8, 45u8, 97u8,
                                117u8, 116u8, 104u8, 45u8, 97u8, 108u8, 114u8, 101u8, 97u8,
                                100u8, 121u8, 45u8, 99u8, 108u8, 97u8, 105u8, 109u8, 101u8,
                                100u8, b'\n'];
                const AUTH_CODE_BAD_CREDENTIALS: &'static [u8] =
                    &[b'!', 50u8, b'\n', 49u8, 48u8, b'\n'];
                const AUTH_ERROR_DISABLED: &'static [u8] =
                    &[b'!', 49u8, 55u8, b'\n', 101u8, 114u8, 114u8, 45u8, 97u8,
                                117u8, 116u8, 104u8, 45u8, 100u8, 105u8, 115u8, 97u8, 98u8,
                                108u8, 101u8, 100u8, b'\n'];
                const AUTH_CODE_PERMS: &'static [u8] =
                    &[b'!', 50u8, b'\n', 49u8, 49u8, b'\n'];
                const AUTH_ERROR_ILLEGAL_USERNAME: &'static [u8] =
                    &[b'!', 50u8, 53u8, b'\n', 101u8, 114u8, 114u8, 45u8, 97u8,
                                117u8, 116u8, 104u8, 45u8, 105u8, 108u8, 108u8, 101u8,
                                103u8, 97u8, 108u8, 45u8, 117u8, 115u8, 101u8, 114u8, 110u8,
                                97u8, 109u8, 101u8, b'\n'];
                const AUTH_ERROR_FAILED_TO_DELETE_USER: &'static [u8] =
                    &[b'!', 50u8, 49u8, b'\n', 101u8, 114u8, 114u8, 45u8, 97u8,
                                117u8, 116u8, 104u8, 45u8, 100u8, 101u8, 108u8, 117u8,
                                115u8, 101u8, 114u8, 45u8, 102u8, 97u8, 105u8, 108u8,
                                b'\n'];
            }
            impl<Strm, T> ProtocolRead<Skyhash1, Strm> for T where
                T: RawConnection<Skyhash1, Strm> + Send + Sync, Strm: Stream {
                fn try_query(&self) -> Result<QueryWithAdvance, ParseError> {
                    Skyhash1::parse(self.get_buffer())
                }
            }
            impl<Strm, T> ProtocolWrite<Skyhash1, Strm> for T where
                T: RawConnection<Skyhash1, Strm> + Send + Sync, Strm: Stream {
                fn write_mono_length_prefixed_with_tsymbol<'life0, 'life1,
                    'ret_life>(&'life0 mut self, data: &'life1 [u8],
                    tsymbol: u8) -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: Send +
                    'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[tsymbol]).await?;
                            stream.write_all(&Integer64::from(data.len())).await?;
                            stream.write_all(&[Skyhash1::LF]).await?;
                            stream.write_all(data).await?;
                            stream.write_all(&[Skyhash1::LF]).await
                        })
                }
                fn write_string<'life0, 'life1,
                    'ret_life>(&'life0 mut self, string: &'life1 str)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash1::TSYMBOL_STRING]).await?;
                            let len_bytes = Integer64::from(string.len());
                            stream.write_all(&len_bytes).await?;
                            stream.write_all(&[Skyhash1::LF]).await?;
                            stream.write_all(string.as_bytes()).await?;
                            stream.write_all(&[Skyhash1::LF]).await
                        })
                }
                fn write_binary<'life0, 'life1,
                    'ret_life>(&'life0 mut self, binary: &'life1 [u8])
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash1::TSYMBOL_BINARY]).await?;
                            let len_bytes = Integer64::from(binary.len());
                            stream.write_all(&len_bytes).await?;
                            stream.write_all(&[Skyhash1::LF]).await?;
                            stream.write_all(binary).await?;
                            stream.write_all(&[Skyhash1::LF]).await
                        })
                }
                fn write_usize<'life0,
                    'ret_life>(&'life0 mut self, size: usize)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, Self: 'ret_life {
                    Box::pin(async move { self.write_int64(size as _).await })
                }
                fn write_int64<'life0, 'ret_life>(&'life0 mut self, int: u64)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash1::TSYMBOL_INT64]).await?;
                            let body = Integer64::from(int);
                            let body_len = Integer64::from(body.len());
                            stream.write_all(&body_len).await?;
                            stream.write_all(&[Skyhash1::LF]).await?;
                            stream.write_all(&body).await?;
                            stream.write_all(&[Skyhash1::LF]).await
                        })
                }
                fn write_float<'life0,
                    'ret_life>(&'life0 mut self, float: f32)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash1::TSYMBOL_FLOAT]).await?;
                            let body = float.to_string();
                            let body = body.as_bytes();
                            let sizeline = Integer64::from(body.len());
                            stream.write_all(&sizeline).await?;
                            stream.write_all(&[Skyhash1::LF]).await?;
                            stream.write_all(body).await?;
                            stream.write_all(&[Skyhash1::LF]).await
                        })
                }
                fn write_typed_array_element<'life0, 'life1,
                    'ret_life>(&'life0 mut self, element: &'life1 [u8])
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&Integer64::from(element.len())).await?;
                            stream.write_all(&[Skyhash1::LF]).await?;
                            stream.write_all(element).await?;
                            stream.write_all(&[Skyhash1::LF]).await
                        })
                }
            }
        }
        /// A parser for Skyhash 1.0
        ///
        /// Packet structure example (simple query):
        /// ```text
        /// *1\n
        /// ~3\n
        /// 3\n
        /// SET\n
        /// 1\n
        /// x\n
        /// 3\n
        /// 100\n
        /// ```
        pub struct Parser {
            end: *const u8,
            cursor: *const u8,
        }
        unsafe impl RawParser for Parser {
            fn cursor_ptr(&self) -> *const u8 { self.cursor }
            fn cursor_ptr_mut(&mut self) -> &mut *const u8 {
                &mut self.cursor
            }
            fn data_end_ptr(&self) -> *const u8 { self.end }
        }
        unsafe impl Send for Parser {}
        unsafe impl Sync for Parser {}
        impl Parser {
            /// Initialize a new parser
            fn new(slice: &[u8]) -> Self {
                unsafe {
                    Self {
                        end: slice.as_ptr().add(slice.len()),
                        cursor: slice.as_ptr(),
                    }
                }
            }
        }
        impl Parser {
            /// Returns true if the cursor will give a char, but if `this_if_nothing_ahead` is set
            /// to true, then if no byte is ahead, it will still return true
            fn will_cursor_give_char(&self, ch: u8,
                true_if_nothing_ahead: bool) -> ParseResult<bool> {
                if self.exhausted() {
                        if true_if_nothing_ahead {
                                Ok(true)
                            } else { Err(ParseError::NotEnough) }
                    } else if unsafe { self.get_byte_at_cursor().eq(&ch) } {
                       Ok(true)
                   } else { Ok(false) }
            }
            /// Check if the current cursor will give an LF
            fn will_cursor_give_linefeed(&self) -> ParseResult<bool> {
                self.will_cursor_give_char(b'\n', false)
            }
            /// Gets the _next element. **The cursor should be at the tsymbol (passed)**
            fn _next(&mut self) -> ParseResult<UnsafeSlice> {
                let element_size = self.read_usize()?;
                self.read_until(element_size)
            }
        }
        impl Parser {
            /// Parse the next blob. **The cursor should be at the tsymbol (passed)**
            fn parse_next_blob(&mut self) -> ParseResult<UnsafeSlice> {
                {
                    let chunk = self._next()?;
                    if self.will_cursor_give_linefeed()? {
                            unsafe { self.incr_cursor(); }
                            Ok(chunk)
                        } else { Err(ParseError::UnexpectedByte) }
                }
            }
        }
        impl Parser {
            /// The buffer should resemble the below structure:
            /// ```
            /// ~<count>\n
            /// <e0l0>\n
            /// <e0>\n
            /// <e1l1>\n
            /// <e1>\n
            /// ...
            /// ```
            fn _parse_simple_query(&mut self)
                -> ParseResult<HeapArray<UnsafeSlice>> {
                if self.not_exhausted() {
                        if unsafe { self.get_byte_at_cursor() } != b'~' {
                                return Err(ParseError::WrongType);
                            }
                        unsafe { self.incr_cursor(); }
                        let query_count = self.read_usize()?;
                        let mut writer =
                            HeapArrayWriter::with_capacity(query_count);
                        for i in 0..query_count {
                            unsafe {
                                writer.write_to_index(i, self.parse_next_blob()?);
                            }
                        }
                        Ok(unsafe { writer.finish() })
                    } else { Err(ParseError::NotEnough) }
            }
            fn parse_simple_query(&mut self) -> ParseResult<SimpleQuery> {
                Ok(SimpleQuery::new(self._parse_simple_query()?))
            }
            /// The buffer should resemble the following structure:
            /// ```text
            /// # query 1
            /// ~<count>\n
            /// <e0l0>\n
            /// <e0>\n
            /// <e1l1>\n
            /// <e1>\n
            /// # query 2
            /// ~<count>\n
            /// <e0l0>\n
            /// <e0>\n
            /// <e1l1>\n
            /// <e1>\n
            /// ...
            /// ```
            fn parse_pipelined_query(&mut self, length: usize)
                -> ParseResult<PipelinedQuery> {
                let mut writer = HeapArrayWriter::with_capacity(length);
                for i in 0..length {
                    unsafe {
                        writer.write_to_index(i, self._parse_simple_query()?);
                    }
                }
                unsafe { Ok(PipelinedQuery::new(writer.finish())) }
            }
            fn _parse(&mut self) -> ParseResult<Query> {
                if self.not_exhausted() {
                        let first_byte = unsafe { self.get_byte_at_cursor() };
                        if first_byte != b'*' { return Err(ParseError::BadPacket); }
                        unsafe { self.incr_cursor() };
                        let query_count = self.read_usize()?;
                        if query_count == 1 {
                                Ok(Query::Simple(self.parse_simple_query()?))
                            } else {
                               Ok(Query::Pipelined(self.parse_pipelined_query(query_count)?))
                           }
                    } else { Err(ParseError::NotEnough) }
            }
            pub fn parse(buf: &[u8]) -> ParseResult<QueryWithAdvance> {
                let mut slf = Self::new(buf);
                let body = slf._parse()?;
                let consumed =
                    slf.cursor_ptr() as usize - buf.as_ptr() as usize;
                Ok((body, consumed))
            }
        }
    }
    mod v2 {
        mod interface_impls {
            use crate::{
                corestore::buffers::Integer64,
                dbnet::connection::{QueryWithAdvance, RawConnection, Stream},
                protocol::{
                    interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
                    ParseError, Skyhash2,
                },
                util::FutureResult, IoResult,
            };
            use ::sky_macros::compiled_eresp_bytes as eresp;
            use tokio::io::AsyncWriteExt;
            impl ProtocolSpec for Skyhash2 {
                const PROTOCOL_VERSION: f32 = 2.0;
                const PROTOCOL_VERSIONSTRING: &'static str = "Skyhash-2.0";
                const TSYMBOL_STRING: u8 = b'+';
                const TSYMBOL_BINARY: u8 = b'?';
                const TSYMBOL_FLOAT: u8 = b'%';
                const TSYMBOL_INT64: u8 = b':';
                const TSYMBOL_TYPED_ARRAY: u8 = b'@';
                const TSYMBOL_TYPED_NON_NULL_ARRAY: u8 = b'^';
                const TSYMBOL_ARRAY: u8 = b'&';
                const TSYMBOL_FLAT_ARRAY: u8 = b'_';
                const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8] = b"\0";
                const SIMPLE_QUERY_HEADER: &'static [u8] = b"*";
                const PIPELINED_QUERY_FIRST_BYTE: u8 = b'$';
                const RCODE_OKAY: &'static [u8] = &[b'!', 48u8, b'\n'];
                const RCODE_NIL: &'static [u8] = &[b'!', 49u8, b'\n'];
                const RCODE_OVERWRITE_ERR: &'static [u8] =
                    &[b'!', 50u8, b'\n'];
                const RCODE_ACTION_ERR: &'static [u8] = &[b'!', 51u8, b'\n'];
                const RCODE_PACKET_ERR: &'static [u8] = &[b'!', 52u8, b'\n'];
                const RCODE_SERVER_ERR: &'static [u8] = &[b'!', 53u8, b'\n'];
                const RCODE_OTHER_ERR_EMPTY: &'static [u8] =
                    &[b'!', 54u8, b'\n'];
                const RCODE_UNKNOWN_ACTION: &'static [u8] =
                    &[b'!', 85u8, 110u8, 107u8, 110u8, 111u8, 119u8, 110u8,
                                32u8, 97u8, 99u8, 116u8, 105u8, 111u8, 110u8, b'\n'];
                const RCODE_WRONGTYPE_ERR: &'static [u8] =
                    &[b'!', 55u8, b'\n'];
                const RCODE_UNKNOWN_DATA_TYPE: &'static [u8] =
                    &[b'!', 56u8, b'\n'];
                const RCODE_ENCODING_ERROR: &'static [u8] =
                    &[b'!', 57u8, b'\n'];
                const RSTRING_SNAPSHOT_BUSY: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 115u8, 110u8, 97u8,
                                112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 98u8, 117u8, 115u8,
                                121u8, b'\n'];
                const RSTRING_SNAPSHOT_DISABLED: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 115u8, 110u8, 97u8,
                                112u8, 115u8, 104u8, 111u8, 116u8, 45u8, 100u8, 105u8,
                                115u8, 97u8, 98u8, 108u8, 101u8, 100u8, b'\n'];
                const RSTRING_SNAPSHOT_DUPLICATE: &'static [u8] =
                    &[b'!', 100u8, 117u8, 112u8, 108u8, 105u8, 99u8, 97u8,
                                116u8, 101u8, 45u8, 115u8, 110u8, 97u8, 112u8, 115u8, 104u8,
                                111u8, 116u8, b'\n'];
                const RSTRING_SNAPSHOT_ILLEGAL_NAME: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 105u8, 110u8, 118u8,
                                97u8, 108u8, 105u8, 100u8, 45u8, 115u8, 110u8, 97u8, 112u8,
                                115u8, 104u8, 111u8, 116u8, 45u8, 110u8, 97u8, 109u8, 101u8,
                                b'\n'];
                const RSTRING_ERR_ACCESS_AFTER_TERMSIG: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 97u8, 99u8, 99u8, 101u8,
                                115u8, 115u8, 45u8, 97u8, 102u8, 116u8, 101u8, 114u8, 45u8,
                                116u8, 101u8, 114u8, 109u8, 115u8, 105u8, 103u8, b'\n'];
                const RSTRING_DEFAULT_UNSET: &'static [u8] =
                    &[b'!', 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8,
                                45u8, 99u8, 111u8, 110u8, 116u8, 97u8, 105u8, 110u8, 101u8,
                                114u8, 45u8, 117u8, 110u8, 115u8, 101u8, 116u8, b'\n'];
                const RSTRING_CONTAINER_NOT_FOUND: &'static [u8] =
                    &[b'!', 99u8, 111u8, 110u8, 116u8, 97u8, 105u8, 110u8,
                                101u8, 114u8, 45u8, 110u8, 111u8, 116u8, 45u8, 102u8, 111u8,
                                117u8, 110u8, 100u8, b'\n'];
                const RSTRING_STILL_IN_USE: &'static [u8] =
                    &[b'!', 115u8, 116u8, 105u8, 108u8, 108u8, 45u8, 105u8,
                                110u8, 45u8, 117u8, 115u8, 101u8, b'\n'];
                const RSTRING_PROTECTED_OBJECT: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 112u8, 114u8, 111u8,
                                116u8, 101u8, 99u8, 116u8, 101u8, 100u8, 45u8, 111u8, 98u8,
                                106u8, 101u8, 99u8, 116u8, b'\n'];
                const RSTRING_WRONG_MODEL: &'static [u8] =
                    &[b'!', 119u8, 114u8, 111u8, 110u8, 103u8, 45u8, 109u8,
                                111u8, 100u8, 101u8, 108u8, b'\n'];
                const RSTRING_ALREADY_EXISTS: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 97u8, 108u8, 114u8,
                                101u8, 97u8, 100u8, 121u8, 45u8, 101u8, 120u8, 105u8, 115u8,
                                116u8, 115u8, b'\n'];
                const RSTRING_NOT_READY: &'static [u8] =
                    &[b'!', 110u8, 111u8, 116u8, 45u8, 114u8, 101u8, 97u8,
                                100u8, 121u8, b'\n'];
                const RSTRING_DDL_TRANSACTIONAL_FAILURE: &'static [u8] =
                    &[b'!', 116u8, 114u8, 97u8, 110u8, 115u8, 97u8, 99u8, 116u8,
                                105u8, 111u8, 110u8, 97u8, 108u8, 45u8, 102u8, 97u8, 105u8,
                                108u8, 117u8, 114u8, 101u8, b'\n'];
                const RSTRING_UNKNOWN_DDL_QUERY: &'static [u8] =
                    &[b'!', 117u8, 110u8, 107u8, 110u8, 111u8, 119u8, 110u8,
                                45u8, 100u8, 100u8, 108u8, 45u8, 113u8, 117u8, 101u8, 114u8,
                                121u8, b'\n'];
                const RSTRING_BAD_EXPRESSION: &'static [u8] =
                    &[b'!', 109u8, 97u8, 108u8, 102u8, 111u8, 114u8, 109u8,
                                101u8, 100u8, 45u8, 101u8, 120u8, 112u8, 114u8, 101u8,
                                115u8, 115u8, 105u8, 111u8, 110u8, b'\n'];
                const RSTRING_UNKNOWN_MODEL: &'static [u8] =
                    &[b'!', 117u8, 110u8, 107u8, 110u8, 111u8, 119u8, 110u8,
                                45u8, 109u8, 111u8, 100u8, 101u8, 108u8, b'\n'];
                const RSTRING_TOO_MANY_ARGUMENTS: &'static [u8] =
                    &[b'!', 116u8, 111u8, 111u8, 45u8, 109u8, 97u8, 110u8,
                                121u8, 45u8, 97u8, 114u8, 103u8, 115u8, b'\n'];
                const RSTRING_CONTAINER_NAME_TOO_LONG: &'static [u8] =
                    &[b'!', 99u8, 111u8, 110u8, 116u8, 97u8, 105u8, 110u8,
                                101u8, 114u8, 45u8, 110u8, 97u8, 109u8, 101u8, 45u8, 116u8,
                                111u8, 111u8, 45u8, 108u8, 111u8, 110u8, 103u8, b'\n'];
                const RSTRING_BAD_CONTAINER_NAME: &'static [u8] =
                    &[b'!', 98u8, 97u8, 100u8, 45u8, 99u8, 111u8, 110u8, 116u8,
                                97u8, 105u8, 110u8, 101u8, 114u8, 45u8, 110u8, 97u8, 109u8,
                                101u8, b'\n'];
                const RSTRING_UNKNOWN_INSPECT_QUERY: &'static [u8] =
                    &[b'!', 117u8, 110u8, 107u8, 110u8, 111u8, 119u8, 110u8,
                                45u8, 105u8, 110u8, 115u8, 112u8, 101u8, 99u8, 116u8, 45u8,
                                113u8, 117u8, 101u8, 114u8, 121u8, b'\n'];
                const RSTRING_UNKNOWN_PROPERTY: &'static [u8] =
                    &[b'!', 117u8, 110u8, 107u8, 110u8, 111u8, 119u8, 110u8,
                                45u8, 112u8, 114u8, 111u8, 112u8, 101u8, 114u8, 116u8,
                                121u8, b'\n'];
                const RSTRING_KEYSPACE_NOT_EMPTY: &'static [u8] =
                    &[b'!', 107u8, 101u8, 121u8, 115u8, 112u8, 97u8, 99u8,
                                101u8, 45u8, 110u8, 111u8, 116u8, 45u8, 101u8, 109u8, 112u8,
                                116u8, 121u8, b'\n'];
                const RSTRING_BAD_TYPE_FOR_KEY: &'static [u8] =
                    &[b'!', 98u8, 97u8, 100u8, 45u8, 116u8, 121u8, 112u8, 101u8,
                                45u8, 102u8, 111u8, 114u8, 45u8, 107u8, 101u8, 121u8,
                                b'\n'];
                const RSTRING_LISTMAP_BAD_INDEX: &'static [u8] =
                    &[b'!', 98u8, 97u8, 100u8, 45u8, 108u8, 105u8, 115u8, 116u8,
                                45u8, 105u8, 110u8, 100u8, 101u8, 120u8, b'\n'];
                const RSTRING_LISTMAP_LIST_IS_EMPTY: &'static [u8] =
                    &[b'!', 108u8, 105u8, 115u8, 116u8, 45u8, 105u8, 115u8,
                                45u8, 101u8, 109u8, 112u8, 116u8, 121u8, b'\n'];
                const ELEMRESP_HEYA: &'static [u8] = b"+4\nHEY!";
                const FULLRESP_RCODE_PACKET_ERR: &'static [u8] = b"*!4\n";
                const FULLRESP_RCODE_WRONG_TYPE: &'static [u8] = b"*!7\n";
                const AUTH_ERROR_ALREADYCLAIMED: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 97u8, 117u8, 116u8,
                                104u8, 45u8, 97u8, 108u8, 114u8, 101u8, 97u8, 100u8, 121u8,
                                45u8, 99u8, 108u8, 97u8, 105u8, 109u8, 101u8, 100u8, b'\n'];
                const AUTH_CODE_BAD_CREDENTIALS: &'static [u8] =
                    &[b'!', 49u8, 48u8, b'\n'];
                const AUTH_ERROR_DISABLED: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 97u8, 117u8, 116u8,
                                104u8, 45u8, 100u8, 105u8, 115u8, 97u8, 98u8, 108u8, 101u8,
                                100u8, b'\n'];
                const AUTH_CODE_PERMS: &'static [u8] =
                    &[b'!', 49u8, 49u8, b'\n'];
                const AUTH_ERROR_ILLEGAL_USERNAME: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 97u8, 117u8, 116u8,
                                104u8, 45u8, 105u8, 108u8, 108u8, 101u8, 103u8, 97u8, 108u8,
                                45u8, 117u8, 115u8, 101u8, 114u8, 110u8, 97u8, 109u8, 101u8,
                                b'\n'];
                const AUTH_ERROR_FAILED_TO_DELETE_USER: &'static [u8] =
                    &[b'!', 101u8, 114u8, 114u8, 45u8, 97u8, 117u8, 116u8,
                                104u8, 45u8, 100u8, 101u8, 108u8, 117u8, 115u8, 101u8,
                                114u8, 45u8, 102u8, 97u8, 105u8, 108u8, b'\n'];
            }
            impl<Strm, T> ProtocolRead<Skyhash2, Strm> for T where
                T: RawConnection<Skyhash2, Strm> + Send + Sync, Strm: Stream {
                fn try_query(&self) -> Result<QueryWithAdvance, ParseError> {
                    Skyhash2::parse(self.get_buffer())
                }
            }
            impl<Strm, T> ProtocolWrite<Skyhash2, Strm> for T where
                T: RawConnection<Skyhash2, Strm> + Send + Sync, Strm: Stream {
                fn write_mono_length_prefixed_with_tsymbol<'life0, 'life1,
                    'ret_life>(&'life0 mut self, data: &'life1 [u8],
                    tsymbol: u8) -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: Send +
                    'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[tsymbol]).await?;
                            stream.write_all(&Integer64::from(data.len())).await?;
                            stream.write_all(&[Skyhash2::LF]).await?;
                            stream.write_all(data).await
                        })
                }
                fn write_string<'life0, 'life1,
                    'ret_life>(&'life0 mut self, string: &'life1 str)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash2::TSYMBOL_STRING]).await?;
                            let len_bytes = Integer64::from(string.len());
                            stream.write_all(&len_bytes).await?;
                            stream.write_all(&[Skyhash2::LF]).await?;
                            stream.write_all(string.as_bytes()).await
                        })
                }
                fn write_binary<'life0, 'life1,
                    'ret_life>(&'life0 mut self, binary: &'life1 [u8])
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash2::TSYMBOL_BINARY]).await?;
                            let len_bytes = Integer64::from(binary.len());
                            stream.write_all(&len_bytes).await?;
                            stream.write_all(&[Skyhash2::LF]).await?;
                            stream.write_all(binary).await
                        })
                }
                fn write_usize<'life0,
                    'ret_life>(&'life0 mut self, size: usize)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, Self: 'ret_life {
                    Box::pin(async move { self.write_int64(size as _).await })
                }
                fn write_int64<'life0, 'ret_life>(&'life0 mut self, int: u64)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash2::TSYMBOL_INT64]).await?;
                            stream.write_all(&Integer64::from(int)).await?;
                            stream.write_all(&[Skyhash2::LF]).await
                        })
                }
                fn write_float<'life0,
                    'ret_life>(&'life0 mut self, float: f32)
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&[Skyhash2::TSYMBOL_FLOAT]).await?;
                            stream.write_all(float.to_string().as_bytes()).await?;
                            stream.write_all(&[Skyhash2::LF]).await
                        })
                }
                fn write_typed_array_element<'life0, 'life1,
                    'ret_life>(&'life0 mut self, element: &'life1 [u8])
                    -> FutureResult<'ret_life, IoResult<()>> where
                    'life0: 'ret_life, 'life1: 'ret_life, Self: 'ret_life {
                    Box::pin(async move
                            {
                            let stream = self.get_mut_stream();
                            stream.write_all(&Integer64::from(element.len())).await?;
                            stream.write_all(&[Skyhash2::LF]).await?;
                            stream.write_all(element).await
                        })
                }
            }
        }
        use {
            super::{
                raw_parser::{RawParser, RawParserExt, RawParserMeta},
                ParseError, ParseResult, PipelinedQuery, Query, SimpleQuery,
                UnsafeSlice,
            },
            crate::{
                corestore::heap_array::HeapArray,
                dbnet::connection::QueryWithAdvance,
            },
        };
        /// A parser for Skyhash 2.0
        pub struct Parser {
            end: *const u8,
            cursor: *const u8,
        }
        unsafe impl RawParser for Parser {
            fn cursor_ptr(&self) -> *const u8 { self.cursor }
            fn cursor_ptr_mut(&mut self) -> &mut *const u8 {
                &mut self.cursor
            }
            fn data_end_ptr(&self) -> *const u8 { self.end }
        }
        unsafe impl Sync for Parser {}
        unsafe impl Send for Parser {}
        impl Parser {
            /// Initialize a new parser
            fn new(slice: &[u8]) -> Self {
                unsafe {
                    Self {
                        end: slice.as_ptr().add(slice.len()),
                        cursor: slice.as_ptr(),
                    }
                }
            }
        }
        impl Parser {
            /// Parse the next simple query. This should have passed the `*` tsymbol
            ///
            /// Simple query structure (tokenized line-by-line):
            /// ```text
            /// *      -> Simple Query Header
            /// <n>\n  -> Count of elements in the simple query
            /// <l0>\n -> Length of element 1
            /// <e0>   -> element 1 itself
            /// <l1>\n -> Length of element 2
            /// <e1>   -> element 2 itself
            /// ...
            /// ```
            fn _next_simple_query(&mut self)
                -> ParseResult<HeapArray<UnsafeSlice>> {
                let element_count = self.read_usize()?;
                unsafe {
                    let mut data = HeapArray::new_writer(element_count);
                    for i in 0..element_count {
                        let element_size = self.read_usize()?;
                        let element = self.read_until(element_size)?;
                        data.write_to_index(i, element);
                    }
                    Ok(data.finish())
                }
            }
            /// Parse a simple query
            fn next_simple_query(&mut self) -> ParseResult<SimpleQuery> {
                Ok(SimpleQuery::new(self._next_simple_query()?))
            }
            /// Parse a pipelined query. This should have passed the `$` tsymbol
            ///
            /// Pipelined query structure (tokenized line-by-line):
            /// ```text
            /// $          -> Pipeline
            /// <n>\n      -> Pipeline has n queries
            /// <lq0>\n    -> Query 1 has 3 elements
            /// <lq0e0>\n  -> Q1E1 has 3 bytes
            /// <q0e0>     -> Q1E1 itself
            /// <lq0e1>\n  -> Q1E2 has 1 byte
            /// <q0e1>     -> Q1E2 itself
            /// <lq0e2>\n  -> Q1E3 has 3 bytes
            /// <q0e2>     -> Q1E3 itself
            /// <lq1>\n    -> Query 2 has 2 elements
            /// <lq1e0>\n  -> Q2E1 has 3 bytes
            /// <q1e0>     -> Q2E1 itself
            /// <lq1e1>\n  -> Q2E2 has 1 byte
            /// <q1e1>     -> Q2E2 itself
            /// ...
            /// ```
            ///
            /// Example:
            /// ```text
            /// $    -> Pipeline
            /// 2\n  -> Pipeline has 2 queries
            /// 3\n  -> Query 1 has 3 elements
            /// 3\n  -> Q1E1 has 3 bytes
            /// SET  -> Q1E1 itself
            /// 1\n  -> Q1E2 has 1 byte
            /// x    -> Q1E2 itself
            /// 3\n  -> Q1E3 has 3 bytes
            /// 100  -> Q1E3 itself
            /// 2\n  -> Query 2 has 2 elements
            /// 3\n  -> Q2E1 has 3 bytes
            /// GET  -> Q2E1 itself
            /// 1\n  -> Q2E2 has 1 byte
            /// x    -> Q2E2 itself
            /// ```
            fn next_pipeline(&mut self) -> ParseResult<PipelinedQuery> {
                let query_count = self.read_usize()?;
                unsafe {
                    let mut queries = HeapArray::new_writer(query_count);
                    for i in 0..query_count {
                        let sq = self._next_simple_query()?;
                        queries.write_to_index(i, sq);
                    }
                    Ok(PipelinedQuery { data: queries.finish() })
                }
            }
            fn _parse(&mut self) -> ParseResult<Query> {
                if self.not_exhausted() {
                        unsafe {
                            let first_byte = self.get_byte_at_cursor();
                            self.incr_cursor();
                            let data =
                                match first_byte {
                                    b'*' => { Query::Simple(self.next_simple_query()?) }
                                    b'$' => { Query::Pipelined(self.next_pipeline()?) }
                                    _ => return Err(ParseError::UnexpectedByte),
                                };
                            Ok(data)
                        }
                    } else { Err(ParseError::NotEnough) }
            }
            pub fn parse(buf: &[u8]) -> ParseResult<QueryWithAdvance> {
                let mut slf = Self::new(buf);
                let body = slf._parse()?;
                let consumed =
                    slf.cursor_ptr() as usize - buf.as_ptr() as usize;
                Ok((body, consumed))
            }
        }
    }
    pub type Skyhash2 = v2::Parser;
    pub type Skyhash1 = v1::Parser;
    /// As its name says, an [`UnsafeSlice`] is a terribly unsafe slice. It's guarantess are
    /// very C-like, your ptr goes dangling -- and everything is unsafe.
    ///
    /// ## Safety contracts
    /// - The `start_ptr` is valid
    /// - The `len` is correct
    /// - `start_ptr` remains valid as long as the object is used
    ///
    pub struct UnsafeSlice {
        start_ptr: *const u8,
        len: usize,
    }
    impl ::core::marker::StructuralPartialEq for UnsafeSlice {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for UnsafeSlice {
        #[inline]
        fn eq(&self, other: &UnsafeSlice) -> bool {
            match *other {
                UnsafeSlice { start_ptr: ref __self_1_0, len: ref __self_1_1 }
                    =>
                    match *self {
                        UnsafeSlice { start_ptr: ref __self_0_0, len: ref __self_0_1
                            } =>
                            (*__self_0_0) == (*__self_1_0) &&
                                (*__self_0_1) == (*__self_1_1),
                    },
            }
        }
        #[inline]
        fn ne(&self, other: &UnsafeSlice) -> bool {
            match *other {
                UnsafeSlice { start_ptr: ref __self_1_0, len: ref __self_1_1 }
                    =>
                    match *self {
                        UnsafeSlice { start_ptr: ref __self_0_0, len: ref __self_0_1
                            } =>
                            (*__self_0_0) != (*__self_1_0) ||
                                (*__self_0_1) != (*__self_1_1),
                    },
            }
        }
    }
    unsafe impl Send for UnsafeSlice {}
    unsafe impl Sync for UnsafeSlice {}
    impl fmt::Debug for UnsafeSlice {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            unsafe {
                f.write_str(core::str::from_utf8_unchecked(self.as_slice()))
            }
        }
    }
    impl UnsafeSlice {
        /// Create a new `UnsafeSlice`
        #[inline(always)]
        pub const fn new(start_ptr: *const u8, len: usize) -> Self {
            Self { start_ptr, len }
        }
        /// Return self as a slice
        /// ## Safety
        /// The caller must ensure that the pointer and length used when constructing the slice
        /// are valid when this is called
        #[inline(always)]
        pub unsafe fn as_slice(&self) -> &[u8] {
            slice::from_raw_parts(self.start_ptr, self.len)
        }
    }
    #[repr(u8)]
    /// # Parser Errors
    ///
    /// Several errors can arise during parsing and this enum accounts for them
    pub enum ParseError {

        /// Didn't get the number of expected bytes
        NotEnough = 0u8,

        /// The packet simply contains invalid data
        BadPacket = 1u8,

        /// The query contains an unexpected byte
        UnexpectedByte = 2u8,

        /// A data type was given but the parser failed to serialize it into this type
        ///
        /// This can happen not just for elements but can also happen for their sizes ([`Self::parse_into_u64`])
        DatatypeParseFailure = 3u8,

        /// The client supplied the wrong query data type for the given query
        WrongType = 4u8,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for ParseError {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&ParseError::NotEnough,) => {
                    ::core::fmt::Formatter::write_str(f, "NotEnough")
                }
                (&ParseError::BadPacket,) => {
                    ::core::fmt::Formatter::write_str(f, "BadPacket")
                }
                (&ParseError::UnexpectedByte,) => {
                    ::core::fmt::Formatter::write_str(f, "UnexpectedByte")
                }
                (&ParseError::DatatypeParseFailure,) => {
                    ::core::fmt::Formatter::write_str(f, "DatatypeParseFailure")
                }
                (&ParseError::WrongType,) => {
                    ::core::fmt::Formatter::write_str(f, "WrongType")
                }
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for ParseError {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for ParseError {
        #[inline]
        fn eq(&self, other: &ParseError) -> bool {
            {
                let __self_vi =
                    ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi =
                    ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                        match (&*self, &*other) { _ => true, }
                    } else { false }
            }
        }
    }
    /// A generic result to indicate parsing errors thorugh the [`ParseError`] enum
    pub type ParseResult<T> = Result<T, ParseError>;
    pub enum Query { Simple(SimpleQuery), Pipelined(PipelinedQuery), }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for Query {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&Query::Simple(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Simple");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&Query::Pipelined(ref __self_0),) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Pipelined");
                    let _ =
                        ::core::fmt::DebugTuple::field(debug_trait_builder,
                            &&(*__self_0));
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
            }
        }
    }
    pub struct SimpleQuery {
        data: HeapArray<UnsafeSlice>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for SimpleQuery {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                SimpleQuery { data: ref __self_0_0 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f, "SimpleQuery");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "data",
                            &&(*__self_0_0));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl SimpleQuery {
        pub const fn new(data: HeapArray<UnsafeSlice>) -> Self {
            Self { data }
        }
        #[inline(always)]
        pub fn as_slice(&self) -> &[UnsafeSlice] { &self.data }
    }
    pub struct PipelinedQuery {
        data: HeapArray<HeapArray<UnsafeSlice>>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for PipelinedQuery {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match *self {
                PipelinedQuery { data: ref __self_0_0 } => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_struct(f,
                                "PipelinedQuery");
                    let _ =
                        ::core::fmt::DebugStruct::field(debug_trait_builder, "data",
                            &&(*__self_0_0));
                    ::core::fmt::DebugStruct::finish(debug_trait_builder)
                }
            }
        }
    }
    impl PipelinedQuery {
        pub const fn new(data: HeapArray<HeapArray<UnsafeSlice>>) -> Self {
            Self { data }
        }
        pub fn len(&self) -> usize { self.data.len() }
        pub fn into_inner(self) -> HeapArray<HeapArray<UnsafeSlice>> {
            self.data
        }
    }
}
mod queryengine {
    //! # The Query Engine
    use crate::actions::{ActionError, ActionResult};
    use crate::auth;
    use crate::corestore::Corestore;
    use crate::dbnet::connection::prelude::*;
    use crate::protocol::{
        iter::AnyArrayIter, PipelinedQuery, SimpleQuery, UnsafeSlice,
    };
    use crate::queryengine::parser::Entity;
    use crate::{actions, admin};
    mod ddl {
        use super::parser;
        use super::parser::VALID_CONTAINER_NAME;
        use crate::corestore::memstore::ObjectID;
        use crate::dbnet::connection::prelude::*;
        use crate::kvengine::encoding;
        use crate::registry;
        use core::str;
        pub const TABLE: &[u8] = "TABLE".as_bytes();
        pub const KEYSPACE: &[u8] = "KEYSPACE".as_bytes();
        const VOLATILE: &[u8] = "volatile".as_bytes();
        const FORCE_REMOVE: &[u8] = "force".as_bytes();
        #[doc =
        r" Handle `create table <tableid> <model>(args)` and `create keyspace <ksid>`"]
        #[doc = r" like queries"]
        pub async fn create<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size > 1)?;
            let mut create_what =
                unsafe { act.next().unsafe_unwrap() }.to_vec();
            create_what.make_ascii_uppercase();
            match create_what.as_ref() {
                TABLE => create_table(handle, con, act).await?,
                KEYSPACE => create_keyspace(handle, con, act).await?,
                _ => { con._write_raw(P::RSTRING_UNKNOWN_DDL_QUERY).await?; }
            }
            Ok(())
        }
        #[doc = r" Handle `drop table <tableid>` and `drop keyspace <ksid>`"]
        #[doc = r" like queries"]
        pub async fn ddl_drop<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size > 1)?;
            let mut create_what =
                unsafe { act.next().unsafe_unwrap() }.to_vec();
            create_what.make_ascii_uppercase();
            match create_what.as_ref() {
                TABLE => drop_table(handle, con, act).await?,
                KEYSPACE => drop_keyspace(handle, con, act).await?,
                _ => { con._write_raw(P::RSTRING_UNKNOWN_DDL_QUERY).await?; }
            }
            Ok(())
        }
        #[doc = r" We should have `<tableid> <model>(args) properties`"]
        pub async fn create_table<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size > 1 && size < 4)?;
            let table_name = unsafe { act.next().unsafe_unwrap() };
            let model_name = unsafe { act.next().unsafe_unwrap() };
            let (table_entity, model_code) =
                parser::parse_table_args::<P>(table_name, model_name)?;
            let is_volatile =
                match act.next() {
                    Some(maybe_volatile) => {
                        ensure_cond_or_err(maybe_volatile.eq(VOLATILE),
                                P::RSTRING_UNKNOWN_PROPERTY)?;
                        true
                    }
                    None => false,
                };
            if registry::state_okay() {
                    translate_ddl_error::<P,
                                ()>(handle.create_table(table_entity, model_code,
                                is_volatile))?;
                    con._write_raw(P::RCODE_OKAY).await?;
                } else { return util::err(P::RCODE_SERVER_ERR); }
            Ok(())
        }
        #[doc = r" We should have `<ksid>`"]
        pub async fn create_keyspace<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len == 1)?;
            match act.next() {
                Some(ksid) => {
                    ensure_cond_or_err(encoding::is_utf8(&ksid),
                            P::RCODE_ENCODING_ERROR)?;
                    let ksid_str = unsafe { str::from_utf8_unchecked(ksid) };
                    ensure_cond_or_err(VALID_CONTAINER_NAME.is_match(ksid_str),
                            P::RSTRING_BAD_EXPRESSION)?;
                    ensure_cond_or_err(ksid.len() < 64,
                            P::RSTRING_CONTAINER_NAME_TOO_LONG)?;
                    let ksid = unsafe { ObjectID::from_slice(ksid_str) };
                    if registry::state_okay() {
                            translate_ddl_error::<P, ()>(handle.create_keyspace(ksid))?;
                            con._write_raw(P::RCODE_OKAY).await?
                        } else { return util::err(P::RCODE_SERVER_ERR); }
                }
                None => return util::err(P::RCODE_ACTION_ERR),
            }
            Ok(())
        }
        #[doc = r" Drop a table (`<tblid>` only)"]
        pub async fn drop_table<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size == 1)?;
            match act.next() {
                Some(eg) => {
                    let entity_group = parser::Entity::from_slice::<P>(eg)?;
                    if registry::state_okay() {
                            translate_ddl_error::<P,
                                        ()>(handle.drop_table(entity_group))?;
                            con._write_raw(P::RCODE_OKAY).await?;
                        } else { return util::err(P::RCODE_SERVER_ERR); }
                }
                None => return util::err(P::RCODE_ACTION_ERR),
            }
            Ok(())
        }
        #[doc = r" Drop a keyspace (`<ksid>` only)"]
        pub async fn drop_keyspace<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |size| size == 1)?;
            match act.next() {
                Some(ksid) => {
                    ensure_cond_or_err(ksid.len() < 64,
                            P::RSTRING_CONTAINER_NAME_TOO_LONG)?;
                    let force_remove =
                        match act.next() {
                            Some(bts) if bts.eq(FORCE_REMOVE) => true,
                            None => false,
                            _ => { return util::err(P::RCODE_UNKNOWN_ACTION); }
                        };
                    if registry::state_okay() {
                            let objid = unsafe { ObjectID::from_slice(ksid) };
                            let result =
                                if force_remove {
                                        handle.force_drop_keyspace(objid)
                                    } else { handle.drop_keyspace(objid) };
                            translate_ddl_error::<P, ()>(result)?;
                            con._write_raw(P::RCODE_OKAY).await?;
                        } else { return util::err(P::RCODE_SERVER_ERR); }
                }
                None => return util::err(P::RCODE_ACTION_ERR),
            }
            Ok(())
        }
    }
    mod inspect {
        use super::ddl::{KEYSPACE, TABLE};
        use crate::corestore::{
            memstore::{Keyspace, ObjectID},
            table::Table,
        };
        use crate::dbnet::connection::prelude::*;
        const KEYSPACES: &[u8] = "KEYSPACES".as_bytes();
        #[doc = r" Runs an inspect query:"]
        #[doc = r" - `INSPECT KEYSPACES` is run by this function itself"]
        #[doc =
        r" - `INSPECT TABLE <tblid>` is delegated to self::inspect_table"]
        #[doc =
        r" - `INSPECT KEYSPACE <ksid>` is delegated to self::inspect_keyspace"]
        pub async fn inspect<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            match act.next() {
                Some(inspect_what) => {
                    let mut inspect_what = inspect_what.to_vec();
                    inspect_what.make_ascii_uppercase();
                    match inspect_what.as_ref() {
                        KEYSPACE => inspect_keyspace(handle, con, act).await?,
                        TABLE => inspect_table(handle, con, act).await?,
                        KEYSPACES => {
                            ensure_length::<P>(act.len(), |len| len == 0)?;
                            let ks_list: Vec<ObjectID> =
                                handle.get_store().keyspaces.iter().map(|kv|
                                            kv.key().clone()).collect();
                            con.write_typed_non_null_array_header(ks_list.len(),
                                        b'+').await?;
                            for ks in ks_list {
                                con.write_typed_non_null_array_element(&ks).await?;
                            }
                        }
                        _ => return util::err(P::RSTRING_UNKNOWN_INSPECT_QUERY),
                    }
                }
                None => return util::err(P::RCODE_ACTION_ERR),
            }
            Ok(())
        }
        #[doc = r" INSPECT a keyspace. This should only have the keyspace ID"]
        pub async fn inspect_keyspace<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len < 2)?;
            let tbl_list: Vec<ObjectID> =
                match act.next() {
                    Some(keyspace_name) => {
                        let ksid =
                            if keyspace_name.len() > 64 {
                                    return util::err(P::RSTRING_BAD_CONTAINER_NAME);
                                } else { keyspace_name };
                        let ks =
                            match handle.get_keyspace(ksid) {
                                Some(kspace) => kspace,
                                None => return util::err(P::RSTRING_CONTAINER_NOT_FOUND),
                            };
                        ks.tables.iter().map(|kv| kv.key().clone()).collect()
                    }
                    None => {
                        let cks =
                            translate_ddl_error::<P, &Keyspace>(handle.get_cks())?;
                        cks.tables.iter().map(|kv| kv.key().clone()).collect()
                    }
                };
            con.write_typed_non_null_array_header(tbl_list.len(),
                        b'+').await?;
            for tbl in tbl_list {
                con.write_typed_non_null_array_element(&tbl).await?;
            }
            Ok(())
        }
        #[doc = r" INSPECT a table. This should only have the table ID"]
        pub async fn inspect_table<'a, T: 'a +
            crate::dbnet::connection::ClientConnection<P, Strm>,
            Strm: crate::dbnet::connection::Stream,
            P: crate::protocol::interface::ProtocolSpec>(handle: &Corestore,
            con: &'a mut T, mut act: ActionIter<'a>)
            -> crate::actions::ActionResult<()> {
            ensure_length::<P>(act.len(), |len| len < 2)?;
            match act.next() {
                Some(entity) => {
                    let entity =
                        {
                            match crate::queryengine::parser::Entity::from_slice::<P>(&entity)
                                {
                                Ok(e) => e,
                                Err(e) => return Err(e.into()),
                            }
                        };
                    con.write_string({
                                        crate::actions::translate_ddl_error::<P,
                                                    ::std::sync::Arc<crate::corestore::table::Table>>(handle.get_table(entity))?
                                    }.describe_self()).await?;
                }
                None => {
                    let tbl =
                        translate_ddl_error::<P,
                                    &Table>(handle.get_table_result())?;
                    con.write_string(tbl.describe_self()).await?;
                }
            }
            Ok(())
        }
    }
    pub mod parser {
        use crate::corestore::{lazy::Lazy, memstore::ObjectID};
        use crate::kvengine::encoding;
        use crate::queryengine::ProtocolSpec;
        use crate::util::{self, compiler::{self, cold_err}};
        use core::{fmt, str};
        use regex::Regex;
        type LazyRegexFn = Lazy<Regex, fn() -> Regex>;
        const KEYMAP: &[u8] = "keymap".as_bytes();
        const BINSTR: &[u8] = "binstr".as_bytes();
        const STR: &[u8] = "str".as_bytes();
        const LIST_STR: &[u8] = "list<str>".as_bytes();
        const LIST_BINSTR: &[u8] = "list<binstr>".as_bytes();
        pub(super) static VALID_CONTAINER_NAME: LazyRegexFn =
            LazyRegexFn::new(||
                    Regex::new("^[a-zA-Z_][a-zA-Z_0-9]*$").unwrap());
        pub(super) static VALID_TYPENAME: LazyRegexFn =
            LazyRegexFn::new(||
                    Regex::new("^<[a-zA-Z][a-zA-Z0-9]+[^>\\s]?>{1}$").unwrap());
        pub(super) fn parse_table_args<'a,
            P: ProtocolSpec>(table_name: &'a [u8], model_name: &'a [u8])
            -> Result<(Entity<'a>, u8), &'static [u8]> {
            if compiler::unlikely(!encoding::is_utf8(&table_name) ||
                            !encoding::is_utf8(&model_name)) {
                    return Err(P::RCODE_ENCODING_ERROR);
                }
            let model_name_str =
                unsafe { str::from_utf8_unchecked(model_name) };
            let entity_group = Entity::from_slice::<P>(table_name)?;
            let splits: Vec<&str> = model_name_str.split('(').collect();
            if compiler::unlikely(splits.len() != 2) {
                    return Err(P::RSTRING_BAD_EXPRESSION);
                }
            let model_name_split =
                unsafe { *(splits.as_ptr().add(0 as usize)) };
            let model_args_split =
                unsafe { *(splits.as_ptr().add(1 as usize)) };
            if compiler::unlikely(model_name_split.is_empty() ||
                            model_args_split.is_empty()) {
                    return Err(P::RSTRING_BAD_EXPRESSION);
                }
            if model_name_split.as_bytes() != KEYMAP {
                    return Err(P::RSTRING_UNKNOWN_MODEL);
                }
            let non_bracketed_end =
                unsafe {
                    *((*model_args_split.as_bytes()).as_ptr().add((model_args_split.len()
                                            - 1) as usize)) != b')'
                };
            if compiler::unlikely(non_bracketed_end) {
                    return Err(P::RSTRING_BAD_EXPRESSION);
                }
            let model_args: Vec<&str> =
                model_args_split[..model_args_split.len() -
                                        1].split(',').map(|v| v.trim()).collect();
            if compiler::unlikely(model_args.len() != 2) {
                    return cold_err({
                                let all_nonzero =
                                    model_args.into_iter().all(|v| !v.is_empty());
                                if all_nonzero {
                                        Err(P::RSTRING_TOO_MANY_ARGUMENTS)
                                    } else { Err(P::RSTRING_BAD_EXPRESSION) }
                            });
                }
            let key_ty = unsafe { *(model_args.as_ptr().add(0 as usize)) };
            let val_ty = unsafe { *(model_args.as_ptr().add(1 as usize)) };
            let valid_key_ty =
                if let Some(idx) = key_ty.chars().position(|v| v.eq(&'<')) {
                        VALID_CONTAINER_NAME.is_match(&key_ty[..idx]) &&
                            VALID_TYPENAME.is_match(&key_ty[idx..])
                    } else { VALID_CONTAINER_NAME.is_match(key_ty) };
            let valid_val_ty =
                if let Some(idx) = val_ty.chars().position(|v| v.eq(&'<')) {
                        VALID_CONTAINER_NAME.is_match(&val_ty[..idx]) &&
                            VALID_TYPENAME.is_match(&val_ty[idx..])
                    } else { VALID_CONTAINER_NAME.is_match(val_ty) };
            if compiler::unlikely(!(valid_key_ty || valid_val_ty)) {
                    return Err(P::RSTRING_BAD_EXPRESSION);
                }
            let key_ty = key_ty.as_bytes();
            let val_ty = val_ty.as_bytes();
            let model_code: u8 =
                match (key_ty, val_ty) {
                    (BINSTR, BINSTR) => 0,
                    (BINSTR, STR) => 1,
                    (STR, STR) => 2,
                    (STR, BINSTR) => 3,
                    (BINSTR, LIST_BINSTR) => 4,
                    (BINSTR, LIST_STR) => 5,
                    (STR, LIST_BINSTR) => 6,
                    (STR, LIST_STR) => 7,
                    (LIST_STR, _) | (LIST_BINSTR, _) =>
                        return Err(P::RSTRING_BAD_TYPE_FOR_KEY),
                    _ => return Err(P::RCODE_UNKNOWN_DATA_TYPE),
                };
            Ok((entity_group, model_code))
        }
        type ByteSlice<'a> = &'a [u8];
        pub enum Entity<'a> {

            /// Fully qualified syntax (ks:table)
            Full(ByteSlice<'a>, ByteSlice<'a>),

            /// Half entity syntax (only ks/table)
            Single(ByteSlice<'a>),

            /// Partial entity syntax (`:table`)
            Partial(ByteSlice<'a>),
        }
        impl<'a> ::core::marker::StructuralPartialEq for Entity<'a> {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<'a> ::core::cmp::PartialEq for Entity<'a> {
            #[inline]
            fn eq(&self, other: &Entity<'a>) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&Entity::Full(ref __self_0, ref __self_1),
                                    &Entity::Full(ref __arg_1_0, ref __arg_1_1)) =>
                                    (*__self_0) == (*__arg_1_0) && (*__self_1) == (*__arg_1_1),
                                (&Entity::Single(ref __self_0),
                                    &Entity::Single(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                (&Entity::Partial(ref __self_0),
                                    &Entity::Partial(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { false }
                }
            }
            #[inline]
            fn ne(&self, other: &Entity<'a>) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&Entity::Full(ref __self_0, ref __self_1),
                                    &Entity::Full(ref __arg_1_0, ref __arg_1_1)) =>
                                    (*__self_0) != (*__arg_1_0) || (*__self_1) != (*__arg_1_1),
                                (&Entity::Single(ref __self_0),
                                    &Entity::Single(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                (&Entity::Partial(ref __self_0),
                                    &Entity::Partial(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { true }
                }
            }
        }
        pub enum OwnedEntity {
            Full(ObjectID, ObjectID),
            Single(ObjectID),
            Partial(ObjectID),
        }
        impl ::core::marker::StructuralPartialEq for OwnedEntity {}
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl ::core::cmp::PartialEq for OwnedEntity {
            #[inline]
            fn eq(&self, other: &OwnedEntity) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&OwnedEntity::Full(ref __self_0, ref __self_1),
                                    &OwnedEntity::Full(ref __arg_1_0, ref __arg_1_1)) =>
                                    (*__self_0) == (*__arg_1_0) && (*__self_1) == (*__arg_1_1),
                                (&OwnedEntity::Single(ref __self_0),
                                    &OwnedEntity::Single(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                (&OwnedEntity::Partial(ref __self_0),
                                    &OwnedEntity::Partial(ref __arg_1_0)) =>
                                    (*__self_0) == (*__arg_1_0),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { false }
                }
            }
            #[inline]
            fn ne(&self, other: &OwnedEntity) -> bool {
                {
                    let __self_vi =
                        ::core::intrinsics::discriminant_value(&*self);
                    let __arg_1_vi =
                        ::core::intrinsics::discriminant_value(&*other);
                    if true && __self_vi == __arg_1_vi {
                            match (&*self, &*other) {
                                (&OwnedEntity::Full(ref __self_0, ref __self_1),
                                    &OwnedEntity::Full(ref __arg_1_0, ref __arg_1_1)) =>
                                    (*__self_0) != (*__arg_1_0) || (*__self_1) != (*__arg_1_1),
                                (&OwnedEntity::Single(ref __self_0),
                                    &OwnedEntity::Single(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                (&OwnedEntity::Partial(ref __self_0),
                                    &OwnedEntity::Partial(ref __arg_1_0)) =>
                                    (*__self_0) != (*__arg_1_0),
                                _ => unsafe { ::core::intrinsics::unreachable() }
                            }
                        } else { true }
                }
            }
        }
        impl fmt::Debug for OwnedEntity {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    OwnedEntity::Full(a, b) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Full(\'", ":",
                                            "\')"],
                                &[::core::fmt::ArgumentV1::new_display(&String::from_utf8_lossy(a)),
                                            ::core::fmt::ArgumentV1::new_display(&String::from_utf8_lossy(b))])),
                    OwnedEntity::Single(a) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Single(\'",
                                            "\')"],
                                &[::core::fmt::ArgumentV1::new_display(&String::from_utf8_lossy(a))])),
                    OwnedEntity::Partial(a) =>
                        f.write_fmt(::core::fmt::Arguments::new_v1(&["Partial(\':",
                                            "\')"],
                                &[::core::fmt::ArgumentV1::new_display(&String::from_utf8_lossy(a))])),
                }
            }
        }
        impl<'a> fmt::Debug for Entity<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(&[""],
                        &[::core::fmt::ArgumentV1::new_debug(&self.as_owned())]))
            }
        }
        impl<'a> Entity<'a> {
            pub fn from_slice<P: ProtocolSpec>(input: ByteSlice<'a>)
                -> Result<Entity<'a>, &'static [u8]> {
                let parts: Vec<&[u8]> = input.split(|b| *b == b':').collect();
                if compiler::unlikely(parts.is_empty() || parts.len() > 2) {
                        return util::err(P::RSTRING_BAD_EXPRESSION);
                    }
                let first_entity =
                    unsafe { *(parts.as_ptr().add(0 as usize)) };
                if parts.len() == 1 {
                        Ok(Entity::Single(Self::verify_entity_name::<P>(first_entity)?))
                    } else {
                       let second_entity =
                           Self::verify_entity_name::<P>(unsafe {
                                       *(parts.as_ptr().add(1 as usize))
                                   })?;
                       if first_entity.is_empty() {
                               Ok(Entity::Partial(second_entity))
                           } else {
                              let keyspace = Self::verify_entity_name::<P>(first_entity)?;
                              let table = Self::verify_entity_name::<P>(second_entity)?;
                              Ok(Entity::Full(keyspace, table))
                          }
                   }
            }
            #[inline(always)]
            fn verify_entity_name<P: ProtocolSpec>(input: &[u8])
                -> Result<&[u8], &'static [u8]> {
                let mut valid_name =
                    input.len() < 65 && encoding::is_utf8(input) &&
                        unsafe {
                            VALID_CONTAINER_NAME.is_match(str::from_utf8_unchecked(input))
                        };

                #[cfg(not(windows))]
                {
                    valid_name &=
                        (input != b"PRELOAD") && (input != b"PARTMAP");
                }
                if compiler::likely(valid_name && !input.is_empty()) {
                        Ok(input)
                    } else if compiler::unlikely(input.is_empty()) {
                       util::err(P::RSTRING_BAD_EXPRESSION)
                   } else if compiler::unlikely(input.eq(b"system")) {
                       util::err(P::RSTRING_PROTECTED_OBJECT)
                   } else { util::err(P::RSTRING_BAD_CONTAINER_NAME) }
            }
            pub fn as_owned(&self) -> OwnedEntity {
                unsafe {
                    match self {
                        Self::Full(a, b) => {
                            OwnedEntity::Full(ObjectID::from_slice(a),
                                ObjectID::from_slice(b))
                        }
                        Self::Single(a) =>
                            OwnedEntity::Single(ObjectID::from_slice(a)),
                        Self::Partial(a) =>
                            OwnedEntity::Partial(ObjectID::from_slice(a)),
                    }
                }
            }
            pub fn into_owned(self) -> OwnedEntity { self.as_owned() }
        }
    }
    pub type ActionIter<'a> = AnyArrayIter<'a>;
    const ACTION_AUTH: &[u8] = b"auth";
    macro_rules! gen_constants_and_matches {
        ($con : expr, $buf : ident, $db : ident,
        $($action : ident => $fns : path), *,
        { $($action2 : ident => $fns2 : expr), * }) =>
        {
            mod tags
            {
                //! This module is a collection of tags/strings used for evaluating queries
                //! and responses
                $(pub const $action : & [u8] = stringify! ($action).as_bytes()
                ;) *
                $(pub const $action2 : & [u8] = stringify!
                ($action2).as_bytes() ;) *
            } let first =
            $buf.next_uppercase().unwrap_or_custom_aerr(P :: RCODE_PACKET_ERR)
            ? ; match first.as_ref()
            {
                $(tags :: $action => $fns($db, $con, $buf).await ?,) *
                $(tags :: $action2 => $fns2.await ?,) * _ =>
                { $con._write_raw(P :: RCODE_UNKNOWN_ACTION).await ? ; }
            }
        } ;
    }
    #[doc = r" Execute queries for an anonymous user"]
    pub async fn execute_simple_noauth<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(_db: &mut Corestore,
        con: &mut T, auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        buf: SimpleQuery) -> crate::actions::ActionResult<()> {
        let bufref = buf.as_slice();
        let mut iter = unsafe { AnyArrayIter::new(bufref.iter()) };
        match iter.next_lowercase().unwrap_or_custom_aerr(P::RCODE_PACKET_ERR)?.as_ref()
            {
            ACTION_AUTH => auth::auth_login_only(con, auth, iter).await,
            _ => util::err(P::AUTH_CODE_BAD_CREDENTIALS),
        }
    }
    pub async fn execute_simple<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(db: &mut Corestore,
        con: &mut T, auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        buf: SimpleQuery) -> crate::actions::ActionResult<()> {
        self::execute_stage(db, con, auth, buf.as_slice()).await
    }
    async fn execute_stage<'a, P: ProtocolSpec, T: 'a +
        ClientConnection<P, Strm>,
        Strm: Stream>(db: &mut Corestore, con: &'a mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>, buf: &[UnsafeSlice])
        -> ActionResult<()> {
        let mut iter = unsafe { AnyArrayIter::new(buf.iter()) };
        {
            mod tags {
                //! This module is a collection of tags/strings used for evaluating queries
                //! and responses
                pub const GET: &[u8] = "GET".as_bytes();
                pub const SET: &[u8] = "SET".as_bytes();
                pub const UPDATE: &[u8] = "UPDATE".as_bytes();
                pub const DEL: &[u8] = "DEL".as_bytes();
                pub const HEYA: &[u8] = "HEYA".as_bytes();
                pub const EXISTS: &[u8] = "EXISTS".as_bytes();
                pub const MSET: &[u8] = "MSET".as_bytes();
                pub const MGET: &[u8] = "MGET".as_bytes();
                pub const MUPDATE: &[u8] = "MUPDATE".as_bytes();
                pub const SSET: &[u8] = "SSET".as_bytes();
                pub const SDEL: &[u8] = "SDEL".as_bytes();
                pub const SUPDATE: &[u8] = "SUPDATE".as_bytes();
                pub const DBSIZE: &[u8] = "DBSIZE".as_bytes();
                pub const FLUSHDB: &[u8] = "FLUSHDB".as_bytes();
                pub const USET: &[u8] = "USET".as_bytes();
                pub const KEYLEN: &[u8] = "KEYLEN".as_bytes();
                pub const MKSNAP: &[u8] = "MKSNAP".as_bytes();
                pub const LSKEYS: &[u8] = "LSKEYS".as_bytes();
                pub const POP: &[u8] = "POP".as_bytes();
                pub const CREATE: &[u8] = "CREATE".as_bytes();
                pub const DROP: &[u8] = "DROP".as_bytes();
                pub const USE: &[u8] = "USE".as_bytes();
                pub const INSPECT: &[u8] = "INSPECT".as_bytes();
                pub const MPOP: &[u8] = "MPOP".as_bytes();
                pub const LSET: &[u8] = "LSET".as_bytes();
                pub const LGET: &[u8] = "LGET".as_bytes();
                pub const LMOD: &[u8] = "LMOD".as_bytes();
                pub const WHEREAMI: &[u8] = "WHEREAMI".as_bytes();
                pub const SYS: &[u8] = "SYS".as_bytes();
                pub const AUTH: &[u8] = "AUTH".as_bytes();
            }
            let first =
                iter.next_uppercase().unwrap_or_custom_aerr(P::RCODE_PACKET_ERR)?;
            match first.as_ref() {
                tags::GET => actions::get::get(db, con, iter).await?,
                tags::SET => actions::set::set(db, con, iter).await?,
                tags::UPDATE => actions::update::update(db, con, iter).await?,
                tags::DEL => actions::del::del(db, con, iter).await?,
                tags::HEYA => actions::heya::heya(db, con, iter).await?,
                tags::EXISTS => actions::exists::exists(db, con, iter).await?,
                tags::MSET => actions::mset::mset(db, con, iter).await?,
                tags::MGET => actions::mget::mget(db, con, iter).await?,
                tags::MUPDATE =>
                    actions::mupdate::mupdate(db, con, iter).await?,
                tags::SSET => actions::strong::sset(db, con, iter).await?,
                tags::SDEL => actions::strong::sdel(db, con, iter).await?,
                tags::SUPDATE =>
                    actions::strong::supdate(db, con, iter).await?,
                tags::DBSIZE => actions::dbsize::dbsize(db, con, iter).await?,
                tags::FLUSHDB =>
                    actions::flushdb::flushdb(db, con, iter).await?,
                tags::USET => actions::uset::uset(db, con, iter).await?,
                tags::KEYLEN => actions::keylen::keylen(db, con, iter).await?,
                tags::MKSNAP => admin::mksnap::mksnap(db, con, iter).await?,
                tags::LSKEYS => actions::lskeys::lskeys(db, con, iter).await?,
                tags::POP => actions::pop::pop(db, con, iter).await?,
                tags::CREATE => ddl::create(db, con, iter).await?,
                tags::DROP => ddl::ddl_drop(db, con, iter).await?,
                tags::USE => self::entity_swap(db, con, iter).await?,
                tags::INSPECT => inspect::inspect(db, con, iter).await?,
                tags::MPOP => actions::mpop::mpop(db, con, iter).await?,
                tags::LSET => actions::lists::lset(db, con, iter).await?,
                tags::LGET =>
                    actions::lists::lget::lget(db, con, iter).await?,
                tags::LMOD =>
                    actions::lists::lmod::lmod(db, con, iter).await?,
                tags::WHEREAMI =>
                    actions::whereami::whereami(db, con, iter).await?,
                tags::SYS => admin::sys::sys(db, con, iter).await?,
                tags::AUTH => auth::auth(con, auth, iter).await?,
                _ => { con._write_raw(P::RCODE_UNKNOWN_ACTION).await?; }
            };
        }
        Ok(())
    }
    #[doc = r" Handle `use <entity>` like queries"]
    pub async fn entity_swap<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(handle: &mut Corestore,
        con: &mut T, mut act: ActionIter<'a>)
        -> crate::actions::ActionResult<()> {
        ensure_length::<P>(act.len(), |len| len == 1)?;
        let entity = unsafe { act.next_unchecked() };
        translate_ddl_error::<P,
                    ()>(handle.swap_entity(Entity::from_slice::<P>(entity)?))?;
        con._write_raw(P::RCODE_OKAY).await?;
        Ok(())
    }
    /// Execute a stage **completely**. This means that action errors are never propagated
    /// over the try operator
    async fn execute_stage_pedantic<'a, P: ProtocolSpec,
        T: ClientConnection<P, Strm> + 'a, Strm: Stream +
        'a>(handle: &mut Corestore, con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>, stage: &[UnsafeSlice])
        -> crate::IoResult<()> {
        let ret =
            async {
                self::execute_stage(handle, con, auth, stage).await?;
                Ok(())
            };
        match ret.await {
            Ok(()) => Ok(()),
            Err(ActionError::ActionError(e)) => con._write_raw(e).await,
            Err(ActionError::IoError(ioe)) => Err(ioe),
        }
    }
    #[doc = r" Execute a basic pipelined query"]
    pub async fn execute_pipeline<'a, T: 'a +
        crate::dbnet::connection::ClientConnection<P, Strm>,
        Strm: crate::dbnet::connection::Stream,
        P: crate::protocol::interface::ProtocolSpec>(handle: &mut Corestore,
        con: &mut T, auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        pipeline: PipelinedQuery) -> crate::actions::ActionResult<()> {
        for stage in pipeline.into_inner().iter() {
            self::execute_stage_pedantic(handle, con, auth, stage).await?;
        }
        Ok(())
    }
}
pub mod registry {
    //! # System-wide registry
    //!
    //! The registry module provides interfaces for system-wide, global state management
    //!
    use crate::corestore::lock::{QLGuard, QuickLock};
    use core::sync::atomic::AtomicBool;
    use core::sync::atomic::Ordering;
    const ORD_ACQ: Ordering = Ordering::Acquire;
    const ORD_REL: Ordering = Ordering::Release;
    const ORD_SEQ: Ordering = Ordering::SeqCst;
    /// A digital _trip switch_ that can be tripped and untripped in a thread
    /// friendly, consistent manner. It is slightly expensive on processors
    /// with weaker memory ordering (like ARM) when compared to the native
    /// strong ordering provided by some platforms (like x86).
    pub struct Trip {
        /// the switch
        inner: AtomicBool,
    }
    impl Trip {
        /// Get an untripped switch
        pub const fn new_untripped() -> Self {
            Self { inner: AtomicBool::new(false) }
        }
        /// trip the switch
        pub fn trip(&self) { self.inner.store(true, ORD_SEQ) }
        /// reset the switch
        pub fn untrip(&self) { self.inner.store(false, ORD_SEQ) }
        /// check if the switch has tripped
        pub fn is_tripped(&self) -> bool { self.inner.load(ORD_SEQ) }
        /// Returns the previous state and untrips the switch. **Single op**
        pub fn check_and_untrip(&self) -> bool {
            self.inner.swap(false, ORD_SEQ)
        }
    }
    /// The global system health
    static GLOBAL_STATE: AtomicBool = AtomicBool::new(true);
    /// The global flush state
    static FLUSH_STATE: QuickLock<()> = QuickLock::new(());
    /// The preload trip switch
    static PRELOAD_TRIPSWITCH: Trip = Trip::new_untripped();
    static CLEANUP_TRIPSWITCH: Trip = Trip::new_untripped();
    /// Check the global system state
    pub fn state_okay() -> bool { GLOBAL_STATE.load(ORD_ACQ) }
    /// Lock the global flush state. **Remember to drop the lock guard**; else you'll
    /// end up pausing all sorts of global flushing/transactional systems
    pub fn lock_flush_state() -> QLGuard<'static, ()> { FLUSH_STATE.lock() }
    /// Poison the global system state
    pub fn poison() { GLOBAL_STATE.store(false, ORD_REL) }
    /// Unpoison the global system state
    pub fn unpoison() { GLOBAL_STATE.store(true, ORD_REL) }
    /// Get a static reference to the global preload trip switch
    pub fn get_preload_tripswitch() -> &'static Trip { &PRELOAD_TRIPSWITCH }
    /// Get a static reference to the global cleanup trip switch
    pub fn get_cleanup_tripswitch() -> &'static Trip { &CLEANUP_TRIPSWITCH }
}
mod services {
    pub mod bgsave {
        use crate::{
            config::BGSave, corestore::Corestore, dbnet::Terminator, registry,
            storage::{self, v1::flush::Autoflush},
            IoResult,
        };
        use tokio::time::{self, Duration};
        /// The bgsave_scheduler calls the bgsave task in `Corestore` after `every` seconds
        ///
        /// The time after which the scheduler will wake up the BGSAVE task is determined by
        /// `bgsave_cfg` which is to be passed as an argument. If BGSAVE is disabled, this function
        /// immediately returns
        pub async fn bgsave_scheduler(handle: Corestore, bgsave_cfg: BGSave,
            mut terminator: Terminator) {
            match bgsave_cfg {
                BGSave::Enabled(duration) => {
                    let duration = Duration::from_secs(duration);
                    loop {
                        {
                            #[doc(hidden)]
                            mod __tokio_select_util {
                                pub(super) enum Out<_0, _1> { _0(_0), _1(_1), Disabled, }
                                pub(super) type Mask = u8;
                            }
                            use ::tokio::macros::support::Future;
                            use ::tokio::macros::support::Pin;
                            use ::tokio::macros::support::Poll::{Ready, Pending};
                            const BRANCHES: u32 = 2;
                            let mut disabled: __tokio_select_util::Mask =
                                Default::default();
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 0;
                                    disabled |= mask;
                                }
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 1;
                                    disabled |= mask;
                                }
                            let mut output =
                                {
                                    let mut futures =
                                        (time::sleep_until(time::Instant::now() + duration),
                                            terminator.receive_signal());
                                    ::tokio::macros::support::poll_fn(|cx|
                                                {
                                                    let mut is_pending = false;
                                                    let start =
                                                        { ::tokio::macros::support::thread_rng_n(BRANCHES) };
                                                    for i in 0..BRANCHES {
                                                        let branch;

                                                        #[allow(clippy :: modulo_one)]
                                                        { branch = (start + i) % BRANCHES; }
                                                        match branch
                                                            {
                                                                #[allow(unreachable_code)]
                                                                0 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { _ => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_0(out));
                                                            }
                                                                #[allow(unreachable_code)]
                                                                1 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (_, fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { _ => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_1(out));
                                                            }
                                                            _ =>
                                                                ::core::panicking::unreachable_display(&"reaching this means there probably is an off by one bug"),
                                                        }
                                                    }
                                                    if is_pending {
                                                            Pending
                                                        } else { Ready(__tokio_select_util::Out::Disabled) }
                                                }).await
                                };
                            match output {
                                __tokio_select_util::Out::_0(_) => {
                                    let cloned_handle = handle.clone();
                                    tokio::task::spawn_blocking(move ||
                                                    {
                                                        let owned_handle = cloned_handle;
                                                        let _ = bgsave_blocking_section(owned_handle);
                                                    }).await.expect("Something caused the background service to panic");
                                }
                                __tokio_select_util::Out::_1(_) => { break; }
                                __tokio_select_util::Out::Disabled => {
                                    ::std::rt::begin_panic("all branches are disabled and there is no else branch")
                                }
                                _ =>
                                    ::core::panicking::unreachable_display(&"failed to match bind"),
                            }
                        }
                    }
                }
                BGSave::Disabled => {}
            }
            {
                let lvl = ::log::Level::Info;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level()
                        {
                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["BGSAVE service has exited"],
                                &[]), lvl,
                            &("skyd::services::bgsave", "skyd::services::bgsave",
                                    "server/src/services/bgsave.rs", 72u32),
                            ::log::__private_api::Option::None);
                    }
            };
        }
        /// Run bgsave
        ///
        /// This function just hides away the BGSAVE blocking section from the _public API_
        pub fn run_bgsave(handle: &Corestore) -> IoResult<()> {
            storage::v1::flush::flush_full(Autoflush, handle.get_store())
        }
        /// This just wraps around [`_bgsave_blocking_section`] and prints nice log messages depending on the outcome
        fn bgsave_blocking_section(handle: Corestore) -> bool {
            registry::lock_flush_state();
            match run_bgsave(&handle) {
                Ok(_) => {
                    {
                        let lvl = ::log::Level::Info;
                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                    lvl <= ::log::max_level() {
                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["BGSAVE completed successfully"],
                                        &[]), lvl,
                                    &("skyd::services::bgsave", "skyd::services::bgsave",
                                            "server/src/services/bgsave.rs", 87u32),
                                    ::log::__private_api::Option::None);
                            }
                    };
                    registry::unpoison();
                    true
                }
                Err(e) => {
                    {
                        let lvl = ::log::Level::Error;
                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                    lvl <= ::log::max_level() {
                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["BGSAVE failed with error: "],
                                        &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                    &("skyd::services::bgsave", "skyd::services::bgsave",
                                            "server/src/services/bgsave.rs", 92u32),
                                    ::log::__private_api::Option::None);
                            }
                    };
                    registry::poison();
                    false
                }
            }
        }
    }
    pub mod snapshot {
        use crate::config::SnapshotConfig;
        use crate::corestore::Corestore;
        use crate::dbnet::Terminator;
        use crate::registry;
        use crate::storage::v1::sengine::{
            SnapshotActionResult, SnapshotEngine,
        };
        use std::sync::Arc;
        use tokio::time::{self, Duration};
        /// The snapshot service
        ///
        /// This service calls `SnapEngine::mksnap()` periodically to create snapshots. Whenever
        /// the interval for snapshotting expires or elapses, we create a snapshot. The snapshot service
        /// keeps creating snapshots, as long as the database keeps running. Once [`dbnet::run`] broadcasts
        /// a termination signal, we're ready to quit. This function will, by default, poison the database
        /// if snapshotting fails, unless customized by the user.
        pub async fn snapshot_service(engine: Arc<SnapshotEngine>,
            handle: Corestore, ss_config: SnapshotConfig,
            mut termination_signal: Terminator) {
            match ss_config {
                SnapshotConfig::Disabled => { return; }
                SnapshotConfig::Enabled(configuration) => {
                    let (duration, _, failsafe) = configuration.decompose();
                    let duration = Duration::from_secs(duration);
                    loop {
                        {
                            #[doc(hidden)]
                            mod __tokio_select_util {
                                pub(super) enum Out<_0, _1> { _0(_0), _1(_1), Disabled, }
                                pub(super) type Mask = u8;
                            }
                            use ::tokio::macros::support::Future;
                            use ::tokio::macros::support::Pin;
                            use ::tokio::macros::support::Poll::{Ready, Pending};
                            const BRANCHES: u32 = 2;
                            let mut disabled: __tokio_select_util::Mask =
                                Default::default();
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 0;
                                    disabled |= mask;
                                }
                            if !true {
                                    let mask: __tokio_select_util::Mask = 1 << 1;
                                    disabled |= mask;
                                }
                            let mut output =
                                {
                                    let mut futures =
                                        (time::sleep_until(time::Instant::now() + duration),
                                            termination_signal.receive_signal());
                                    ::tokio::macros::support::poll_fn(|cx|
                                                {
                                                    let mut is_pending = false;
                                                    let start =
                                                        { ::tokio::macros::support::thread_rng_n(BRANCHES) };
                                                    for i in 0..BRANCHES {
                                                        let branch;

                                                        #[allow(clippy :: modulo_one)]
                                                        { branch = (start + i) % BRANCHES; }
                                                        match branch
                                                            {
                                                                #[allow(unreachable_code)]
                                                                0 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { _ => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_0(out));
                                                            }
                                                                #[allow(unreachable_code)]
                                                                1 => {
                                                                let mask = 1 << branch;
                                                                if disabled & mask == mask { continue; }
                                                                let (_, fut, ..) = &mut futures;
                                                                let mut fut = unsafe { Pin::new_unchecked(fut) };
                                                                let out =
                                                                    match Future::poll(fut, cx) {
                                                                        Ready(out) => out,
                                                                        Pending => { is_pending = true; continue; }
                                                                    };
                                                                disabled |= mask;

                                                                #[allow(unused_variables)]
                                                                #[allow(unused_mut)]
                                                                match &out { _ => {} _ => continue, }
                                                                return Ready(__tokio_select_util::Out::_1(out));
                                                            }
                                                            _ =>
                                                                ::core::panicking::unreachable_display(&"reaching this means there probably is an off by one bug"),
                                                        }
                                                    }
                                                    if is_pending {
                                                            Pending
                                                        } else { Ready(__tokio_select_util::Out::Disabled) }
                                                }).await
                                };
                            match output {
                                __tokio_select_util::Out::_0(_) => {
                                    let succeeded =
                                        engine.mksnap(handle.clone_store()).await ==
                                            SnapshotActionResult::Ok;
                                    if succeeded {
                                            registry::unpoison();
                                        } else if failsafe { registry::poison(); }
                                }
                                __tokio_select_util::Out::_1(_) => { break; }
                                __tokio_select_util::Out::Disabled => {
                                    ::std::rt::begin_panic("all branches are disabled and there is no else branch")
                                }
                                _ =>
                                    ::core::panicking::unreachable_display(&"failed to match bind"),
                            }
                        }
                    }
                }
            }
            {
                let lvl = ::log::Level::Info;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level()
                        {
                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Snapshot service has exited"],
                                &[]), lvl,
                            &("skyd::services::snapshot", "skyd::services::snapshot",
                                    "server/src/services/snapshot.rs", 86u32),
                            ::log::__private_api::Option::None);
                    }
            };
        }
    }
    use crate::corestore::memstore::Memstore;
    use crate::diskstore::flock::FileLock;
    use crate::storage;
    use crate::util::os;
    use crate::IoResult;
    pub fn restore_data(src: Option<String>) -> IoResult<()> {
        if let Some(src) = src {
                os::recursive_copy(src, "data")?;
                {
                    let lvl = ::log::Level::Info;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Successfully restored data from snapshot"],
                                    &[]), lvl,
                                &("skyd::services", "skyd::services",
                                        "server/src/services/mod.rs", 39u32),
                                ::log::__private_api::Option::None);
                        }
                };
            }
        Ok(())
    }
    pub fn pre_shutdown_cleanup(mut pid_file: FileLock, mr: Option<&Memstore>)
        -> bool {
        if let Err(e) = pid_file.unlock() {
                {
                    let lvl = ::log::Level::Error;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Shutdown failure: Failed to unlock pid file: "],
                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                &("skyd::services", "skyd::services",
                                        "server/src/services/mod.rs", 46u32),
                                ::log::__private_api::Option::None);
                        }
                };
                return false;
            }
        if let Some(mr) = mr {
                {
                    let lvl = ::log::Level::Info;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Compacting tree"],
                                    &[]), lvl,
                                &("skyd::services", "skyd::services",
                                        "server/src/services/mod.rs", 50u32),
                                ::log::__private_api::Option::None);
                        }
                };
                if let Err(e) = storage::v1::interface::cleanup_tree(mr) {
                        {
                            let lvl = ::log::Level::Error;
                            if lvl <= ::log::STATIC_MAX_LEVEL &&
                                        lvl <= ::log::max_level() {
                                    ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Failed to compact tree: "],
                                            &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                        &("skyd::services", "skyd::services",
                                                "server/src/services/mod.rs", 52u32),
                                        ::log::__private_api::Option::None);
                                }
                        };
                        return false;
                    }
            }
        true
    }
}
mod storage {
    /*!
# Storage Engine

The main code in here lies inside `v1`. The reason we've chose to do so is for backwards compatibility.
Unlike other projects that can _just break_, well, we can't. A database has no right to break data no
matter what the reason. You can't just mess up someone's data because you found a more efficient
way to store things. That's why we'll version modules that correspond to version of Cyanstore. It is
totally legal for one version to call data that correspond to other versions.

## How to break

Whenever we're making changes, here's what we need to keep in mind:
1. If the format has only changed, but not the corestore structures, then simply gate a v2 and change
the functions here
2. If the format has changed and so have the corestore structures, then:
    1. Move out all the _old_ corestore structures into that version gate
    2. Then create the new structures in corestore, as appropriate
    3. The methods here should "identify" a version (usually by bytemarks on the `PRELOAD` which
    is here to stay)
    4. Now, the appropriate (if any) version's decoder is called, then the old structures are restored.
    Now, create the new structures using the old ones and then finally return them

Here's some rust-flavored pseudocode:
```
let version = find_version(preload_file_contents)?;
match version {
    V1 => {
        migration::migrate(v1::read_full()?)
    }
    V2 => {
        v2::read_full()
    }
    _ => error!("Unknown version"),
}
```

The migration module, which doesn't exist, yet will always have a way to transform older structures into
the current one. This can be achieved with some trait/generic hackery (although it might be pretty simple
in practice).
*/
    pub mod v1 {
        /*! # Storage engine (v1/Cyanstore 1A)

This module contains code to rapidly encode/decode data. All sizes are encoded into unsigned
64-bit integers for compatibility across 16/32/64 bit platforms. This means that a
data file generated on a 32-bit machine will work seamlessly on a 64-bit machine
and vice versa. Of course, provided that On a 32-bit system, 32 high bits are just zeroed.

## Endianness

All sizes are stored in native endian. If a dataset is imported from a system from a different endian, it is
simply translated into the host's native endian. How everything else is stored is not worth
discussing here. Byte swaps just need one instruction on most architectures

## Safety

> Trust me, all methods are bombingly unsafe. They do such crazy things that you might not
think of using them anywhere outside. This is a specialized parser built for the database.
-- Sayan (July 2021)

*/
        use crate::corestore::array::Array;
        use crate::corestore::htable::Coremap;
        use crate::corestore::Data;
        use core::hash::Hash;
        use core::mem;
        use core::slice;
        use std::collections::HashSet;
        use std::io::Write;
        #[macro_use]
        mod macros {
            macro_rules! little_endian {
                ($block : block) =>
                { #[cfg(target_endian = "little")] { $block } } ;
            }
            macro_rules! big_endian {
                ($block : block) =>
                { #[cfg(target_endian = "big")] { $block } } ;
            }
            macro_rules! not_64_bit {
                ($block : block) =>
                { #[cfg(not(target_pointer_width = "64"))] { $block } } ;
            }
            macro_rules! is_64_bit {
                ($block : block) =>
                { #[cfg(target_pointer_width = "64")] { $block } } ;
            }
            macro_rules! to_64bit_native_endian {
                ($e : expr) => { ($e as u64) } ;
            }
            macro_rules! try_dir_ignore_existing {
                ($dir : expr) =>
                {
                    {
                        match std :: fs :: create_dir_all($dir)
                        {
                            Ok(_) => Ok(()), Err(e) => match e.kind()
                            {
                                std :: io :: ErrorKind :: AlreadyExists => Ok(()), _ =>
                                Err(e),
                            },
                        }
                    }
                } ; ($($dir : expr), *) =>
                { $(try_dir_ignore_existing! ($dir) ? ;) * }
            }
            #[macro_export]
            macro_rules! concat_path {
                ($($s : expr), +) =>
                {
                    {
                        {
                            let mut path = std :: path :: PathBuf ::
                            with_capacity($(($s).len() +) * 0) ; $(path.push($s) ;) *
                            path
                        }
                    }
                } ;
            }
            #[macro_export]
            macro_rules! concat_str {
                ($($s : expr), +) =>
                {
                    {
                        {
                            let mut st = std :: string :: String ::
                            with_capacity($(($s).len() +) * 0) ; $(st.push_str($s) ;) *
                            st
                        }
                    }
                } ;
            }
            macro_rules! read_dir_to_col {
                ($root : expr) =>
                {
                    std :: fs :: read_dir($root)
                    ?.map(| v |
                    {
                        v.expect("Unexpected directory parse failure").file_name().to_string_lossy().to_string()
                    }).collect()
                } ;
            }
        }
        pub mod bytemarks {
            #![allow(unused)]
            //! # Bytemarks
            //!
            //! Bytemarks are single bytes that are written to parts of files to provide metadata. This module
            //! contains a collection of these.
            //!
            //! ## Userspace and system bytemarks
            //!
            //! Although ks/system and ks/default might _reside_ next to each other, their bytemarks are entirely
            //! different!
            /// KVEBlob model bytemark with key:bin, val:bin
            pub const BYTEMARK_MODEL_KV_BIN_BIN: u8 = 0;
            /// KVEBlob model bytemark with key:bin, val:str
            pub const BYTEMARK_MODEL_KV_BIN_STR: u8 = 1;
            /// KVEBlob model bytemark with key:str, val:str
            pub const BYTEMARK_MODEL_KV_STR_STR: u8 = 2;
            /// KVEBlob model bytemark with key:str, val:bin
            pub const BYTEMARK_MODEL_KV_STR_BIN: u8 = 3;
            /// KVEBlob model bytemark with key:binstr, val: list<binstr>
            pub const BYTEMARK_MODEL_KV_BINSTR_LIST_BINSTR: u8 = 4;
            /// KVEBlob model bytemark with key:binstr, val: list<str>
            pub const BYTEMARK_MODEL_KV_BINSTR_LIST_STR: u8 = 5;
            /// KVEBlob model bytemark with key:str, val: list<binstr>
            pub const BYTEMARK_MODEL_KV_STR_LIST_BINSTR: u8 = 6;
            /// KVEBlob model bytemark with key:str, val: list<str>
            pub const BYTEMARK_MODEL_KV_STR_LIST_STR: u8 = 7;
            /// Persistent storage bytemark
            pub const BYTEMARK_STORAGE_PERSISTENT: u8 = 0;
            /// Volatile storage bytemark
            pub const BYTEMARK_STORAGE_VOLATILE: u8 = 1;
            pub const SYSTEM_TABLE_AUTH: u8 = 0;
        }
        pub mod error {
            use crate::corestore::memstore::ObjectID;
            use core::fmt;
            use std::io::Error as IoError;
            pub type StorageEngineResult<T> = Result<T, StorageEngineError>;
            pub trait ErrorContext<T> {
                /// Provide some context to an error
                fn map_err_context(self, extra: impl ToString)
                -> StorageEngineResult<T>;
            }
            impl<T> ErrorContext<T> for Result<T, IoError> {
                fn map_err_context(self, extra: impl ToString)
                    -> StorageEngineResult<T> {
                    self.map_err(|e|
                            StorageEngineError::ioerror_extra(e, extra.to_string()))
                }
            }
            pub enum StorageEngineError {

                /// An I/O Error
                IoError(IoError),

                /// An I/O Error with extra context
                IoErrorExtra(IoError, String),

                /// A corrupted file
                CorruptedFile(String),

                /// The file contains bad metadata
                BadMetadata(String),
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for StorageEngineError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match (&*self,) {
                        (&StorageEngineError::IoError(ref __self_0),) => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_tuple(f, "IoError");
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_0));
                            ::core::fmt::DebugTuple::finish(debug_trait_builder)
                        }
                        (&StorageEngineError::IoErrorExtra(ref __self_0,
                            ref __self_1),) => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_tuple(f, "IoErrorExtra");
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_0));
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_1));
                            ::core::fmt::DebugTuple::finish(debug_trait_builder)
                        }
                        (&StorageEngineError::CorruptedFile(ref __self_0),) => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_tuple(f,
                                        "CorruptedFile");
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_0));
                            ::core::fmt::DebugTuple::finish(debug_trait_builder)
                        }
                        (&StorageEngineError::BadMetadata(ref __self_0),) => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_tuple(f, "BadMetadata");
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_0));
                            ::core::fmt::DebugTuple::finish(debug_trait_builder)
                        }
                    }
                }
            }
            impl StorageEngineError {
                pub fn corrupted_partmap(ksid: &ObjectID) -> Self {
                    Self::CorruptedFile({
                            let res =
                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["",
                                                    "/PARTMAP"],
                                        &[::core::fmt::ArgumentV1::new_display(&unsafe {
                                                                ksid.as_str()
                                                            })]));
                            res
                        })
                }
                pub fn bad_metadata_in_table(ksid: &ObjectID,
                    table: &ObjectID) -> Self {
                    unsafe {
                        Self::CorruptedFile({
                                let res =
                                    ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["",
                                                        "/"],
                                            &[::core::fmt::ArgumentV1::new_display(&ksid.as_str()),
                                                        ::core::fmt::ArgumentV1::new_display(&table.as_str())]));
                                res
                            })
                    }
                }
                pub fn corrupted_preload() -> Self {
                    Self::CorruptedFile("PRELOAD".into())
                }
                pub fn ioerror_extra(ioe: IoError, extra: impl ToString)
                    -> Self {
                    Self::IoErrorExtra(ioe, extra.to_string())
                }
            }
            impl From<IoError> for StorageEngineError {
                fn from(ioe: IoError) -> Self { Self::IoError(ioe) }
            }
            impl fmt::Display for StorageEngineError {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    match self {
                        Self::IoError(ioe) =>
                            f.write_fmt(::core::fmt::Arguments::new_v1(&["I/O error: "],
                                    &[::core::fmt::ArgumentV1::new_display(&ioe)])),
                        Self::IoErrorExtra(ioe, extra) =>
                            f.write_fmt(::core::fmt::Arguments::new_v1(&["I/O error while ",
                                                ": "],
                                    &[::core::fmt::ArgumentV1::new_display(&extra),
                                                ::core::fmt::ArgumentV1::new_display(&ioe)])),
                        Self::CorruptedFile(cfile) =>
                            f.write_fmt(::core::fmt::Arguments::new_v1(&["file `",
                                                "` is corrupted"],
                                    &[::core::fmt::ArgumentV1::new_display(&cfile)])),
                        Self::BadMetadata(file) =>
                            f.write_fmt(::core::fmt::Arguments::new_v1(&["bad metadata in file `",
                                                "`"], &[::core::fmt::ArgumentV1::new_display(&file)])),
                    }
                }
            }
        }
        pub mod flush {
            //! # Flush routines
            //!
            //! This module contains multiple flush routines: at the memstore level, the keyspace level and
            //! the table level
            use super::{bytemarks, interface};
            use crate::corestore::memstore::SYSTEM;
            use crate::corestore::{
                map::iter::BorrowedIter,
                memstore::{Keyspace, Memstore, ObjectID, SystemKeyspace},
                table::{DataModel, SystemDataModel, SystemTable, Table},
            };
            use crate::registry;
            use crate::util::Wrapper;
            use crate::IoResult;
            use core::ops::Deref;
            use std::io::Write;
            use std::sync::Arc;
            pub trait StorageTarget {
                /// This storage target needs a reinit of the tree despite no preload trip.
                /// Exempli gratia: rsnap, snap
                const NEEDS_TREE_INIT: bool;
                /// This storage target should untrip the trip switch
                ///
                /// Example cases where this doesn't apply: snapshots
                const SHOULD_UNTRIP_PRELOAD_TRIPSWITCH: bool;
                /// The root for this storage target. **Must not be separator terminated!**
                fn root(&self)
                -> String;
                /// Returns the path to the `PRELOAD_` **temporary file** ($ROOT/PRELOAD)
                fn preload_target(&self) -> String {
                    let mut p = self.root();
                    p.push('/');
                    p.push_str("PRELOAD_");
                    p
                }
                /// Returns the path to the keyspace folder. ($ROOT/{keyspace})
                fn keyspace_target(&self, keyspace: &str) -> String {
                    let mut p = self.root();
                    p.push('/');
                    p.push_str(keyspace);
                    p
                }
                /// Returns the path to a `PARTMAP_` for the given keyspace. **temporary file**
                /// ($ROOT/{keyspace}/PARTMAP)
                fn partmap_target(&self, keyspace: &str) -> String {
                    let mut p = self.keyspace_target(keyspace);
                    p.push('/');
                    p.push_str("PARTMAP_");
                    p
                }
                /// Returns the path to the table file. **temporary file** ($ROOT/{keyspace}/{table}_)
                fn table_target(&self, keyspace: &str, table: &str)
                    -> String {
                    let mut p = self.keyspace_target(keyspace);
                    p.push('/');
                    p.push_str(table);
                    p.push('_');
                    p
                }
            }
            /// The autoflush target (BGSAVE target)
            pub struct Autoflush;
            impl StorageTarget for Autoflush {
                const NEEDS_TREE_INIT: bool = false;
                const SHOULD_UNTRIP_PRELOAD_TRIPSWITCH: bool = true;
                fn root(&self) -> String {
                    String::from(interface::DIR_KSROOT)
                }
            }
            /// A remote snapshot storage target
            pub struct RemoteSnapshot<'a> {
                name: &'a str,
            }
            impl<'a> RemoteSnapshot<'a> {
                pub fn new(name: &'a str) -> Self { Self { name } }
            }
            impl<'a> StorageTarget for RemoteSnapshot<'a> {
                const NEEDS_TREE_INIT: bool = true;
                const SHOULD_UNTRIP_PRELOAD_TRIPSWITCH: bool = false;
                fn root(&self) -> String {
                    let mut p = String::from(interface::DIR_RSNAPROOT);
                    p.push('/');
                    p.push_str(self.name);
                    p
                }
            }
            /// A snapshot storage target
            pub struct LocalSnapshot {
                name: String,
            }
            impl LocalSnapshot {
                pub fn new(name: String) -> Self { Self { name } }
            }
            impl StorageTarget for LocalSnapshot {
                const NEEDS_TREE_INIT: bool = true;
                const SHOULD_UNTRIP_PRELOAD_TRIPSWITCH: bool = false;
                fn root(&self) -> String {
                    let mut p = String::from(interface::DIR_SNAPROOT);
                    p.push('/');
                    p.push_str(&self.name);
                    p
                }
            }
            /// A keyspace that can be flushed
            pub trait FlushableKeyspace<T: FlushableTable,
                U: Deref<Target = T>> {
                /// The number of tables in this keyspace
                fn table_count(&self)
                -> usize;
                /// An iterator to the tables in this keyspace.
                /// All of them implement [`FlushableTable`]
                fn get_iter(&self)
                -> BorrowedIter<'_, ObjectID, U>;
            }
            impl FlushableKeyspace<Table, Arc<Table>> for Keyspace {
                fn table_count(&self) -> usize { self.tables.len() }
                fn get_iter(&self) -> BorrowedIter<'_, ObjectID, Arc<Table>> {
                    self.tables.iter()
                }
            }
            impl FlushableKeyspace<SystemTable, Wrapper<SystemTable>> for
                SystemKeyspace {
                fn table_count(&self) -> usize { self.tables.len() }
                fn get_iter(&self)
                    -> BorrowedIter<'_, ObjectID, Wrapper<SystemTable>> {
                    self.tables.iter()
                }
            }
            pub trait FlushableTable {
                /// Table is volatile
                fn is_volatile(&self)
                -> bool;
                /// Returns the storage code bytemark
                fn storage_code(&self)
                -> u8;
                /// Serializes the table and writes it to the provided buffer
                fn write_table_to<W: Write>(&self, writer: &mut W)
                -> IoResult<()>;
                /// Returns the model code bytemark
                fn model_code(&self)
                -> u8;
            }
            impl FlushableTable for Table {
                fn is_volatile(&self) -> bool { self.is_volatile() }
                fn write_table_to<W: Write>(&self, writer: &mut W)
                    -> IoResult<()> {
                    match self.get_model_ref() {
                        DataModel::KV(ref kve) =>
                            super::se::raw_serialize_map(kve.get_inner_ref(), writer),
                        DataModel::KVExtListmap(ref kvl) => {
                            super::se::raw_serialize_list_map(kvl.get_inner_ref(),
                                writer)
                        }
                    }
                }
                fn storage_code(&self) -> u8 { self.storage_type() }
                fn model_code(&self) -> u8 { self.get_model_code() }
            }
            impl FlushableTable for SystemTable {
                fn is_volatile(&self) -> bool { false }
                fn write_table_to<W: Write>(&self, writer: &mut W)
                    -> IoResult<()> {
                    match self.get_model_ref() {
                        SystemDataModel::Auth(amap) =>
                            super::se::raw_serialize_map(amap.as_ref(), writer),
                    }
                }
                fn storage_code(&self) -> u8 { 0 }
                fn model_code(&self) -> u8 {
                    match self.get_model_ref() {
                        SystemDataModel::Auth(_) => bytemarks::SYSTEM_TABLE_AUTH,
                    }
                }
            }
            /// Flush the entire **preload + keyspaces + their partmaps**
            pub fn flush_full<T: StorageTarget>(target: T, store: &Memstore)
                -> IoResult<()> {
                let mut should_create_tree = T::NEEDS_TREE_INIT;
                if T::SHOULD_UNTRIP_PRELOAD_TRIPSWITCH {
                        should_create_tree |=
                            registry::get_preload_tripswitch().check_and_untrip();
                    }
                if should_create_tree {
                        super::interface::create_tree(&target, store)?;
                        self::oneshot::flush_preload(&target, store)?;
                    }
                for keyspace in store.keyspaces.iter() {
                    self::flush_keyspace_full(&target, keyspace.key(),
                            keyspace.value().as_ref())?;
                }
                self::flush_keyspace_full(&target, &SYSTEM, &store.system)?;
                Ok(())
            }
            /// Flushes the entire **keyspace + partmap**
            pub fn flush_keyspace_full<T, U, Tbl,
                K>(target: &T, ksid: &ObjectID, keyspace: &K) -> IoResult<()>
                where T: StorageTarget, U: Deref<Target = Tbl>,
                Tbl: FlushableTable, K: FlushableKeyspace<Tbl, U> {
                self::oneshot::flush_partmap(target, ksid, keyspace)?;
                self::oneshot::flush_keyspace(target, ksid, keyspace)
            }
            pub mod oneshot {
                //! # Irresponsible flushing
                //!
                //! Every function does **exactly what it says** and nothing more. No partition
                //! files et al are handled
                //!
                use super::*;
                use std::fs::{self, File};
                /// No `partmap` handling. Just flushes the table to the expected location
                pub fn flush_table<T: StorageTarget,
                    U: FlushableTable>(target: &T, tableid: &ObjectID,
                    ksid: &ObjectID, table: &U) -> IoResult<()> {
                    if table.is_volatile() {
                            Ok(())
                        } else {
                           let path =
                               unsafe {
                                   target.table_target(ksid.as_str(), tableid.as_str())
                               };
                           let mut file = File::create(&path)?;
                           super::interface::serialize_into_slow_buffer(&mut file,
                                   table)?;
                           file.sync_all()?;
                           fs::rename(&path, &path[..path.len() - 1])
                       }
                }
                /// Flushes an entire keyspace to the expected location. No `partmap` or `preload` handling
                pub fn flush_keyspace<T, U, Tbl,
                    K>(target: &T, ksid: &ObjectID, keyspace: &K)
                    -> IoResult<()> where T: StorageTarget,
                    U: Deref<Target = Tbl>, Tbl: FlushableTable,
                    K: FlushableKeyspace<Tbl, U> {
                    for table in keyspace.get_iter() {
                        self::flush_table(target, table.key(), ksid,
                                table.value().deref())?;
                    }
                    Ok(())
                }
                /// Flushes a single partmap
                pub fn flush_partmap<T, U, Tbl,
                    K>(target: &T, ksid: &ObjectID, keyspace: &K)
                    -> IoResult<()> where T: StorageTarget,
                    U: Deref<Target = Tbl>, Tbl: FlushableTable,
                    K: FlushableKeyspace<Tbl, U> {
                    let path = unsafe { target.partmap_target(ksid.as_str()) };
                    let mut file = File::create(&path)?;
                    super::interface::serialize_partmap_into_slow_buffer(&mut file,
                            keyspace)?;
                    file.sync_all()?;
                    fs::rename(&path, &path[..path.len() - 1])?;
                    Ok(())
                }
                pub fn flush_preload<T: StorageTarget>(target: &T,
                    store: &Memstore) -> IoResult<()> {
                    let preloadtmp = target.preload_target();
                    let mut file = File::create(&preloadtmp)?;
                    super::interface::serialize_preload_into_slow_buffer(&mut file,
                            store)?;
                    file.sync_all()?;
                    fs::rename(&preloadtmp,
                            &preloadtmp[..preloadtmp.len() - 1])?;
                    Ok(())
                }
            }
        }
        pub mod interface {
            //! Interfaces with the file system
            use crate::corestore::memstore::Memstore;
            use crate::registry;
            use crate::storage::v1::flush::FlushableKeyspace;
            use crate::storage::v1::flush::FlushableTable;
            use crate::storage::v1::flush::StorageTarget;
            use crate::IoResult;
            use core::ops::Deref;
            use std::collections::HashSet;
            use std::fs;
            use std::io::{BufWriter, Write};
            pub const DIR_KSROOT: &str = "data/ks";
            pub const DIR_SNAPROOT: &str = "data/snaps";
            pub const DIR_RSNAPROOT: &str = "data/rsnap";
            pub const DIR_BACKUPS: &str = "data/backups";
            pub const DIR_ROOT: &str = "data";
            /// Creates the directories for the keyspaces
            pub fn create_tree<T: StorageTarget>(target: &T,
                memroot: &Memstore) -> IoResult<()> {
                for ks in memroot.keyspaces.iter() {
                    unsafe {
                        {
                                match std::fs::create_dir_all(target.keyspace_target(ks.key().as_str()))
                                    {
                                    Ok(_) => Ok(()),
                                    Err(e) =>
                                        match e.kind() {
                                            std::io::ErrorKind::AlreadyExists => Ok(()),
                                            _ => Err(e),
                                        },
                                }
                            }?;
                    }
                }
                Ok(())
            }
            /// This creates the root directory structure:
            /// ```
            /// data/
            ///     ks/
            ///         ks1/
            ///         ks2/
            ///         ks3/
            ///     snaps/
            ///     backups/
            /// ```
            ///
            /// If any directories exist, they are simply ignored
            pub fn create_tree_fresh<T: StorageTarget>(target: &T,
                memroot: &Memstore) -> IoResult<()> {
                {
                        match std::fs::create_dir_all(DIR_ROOT) {
                            Ok(_) => Ok(()),
                            Err(e) =>
                                match e.kind() {
                                    std::io::ErrorKind::AlreadyExists => Ok(()),
                                    _ => Err(e),
                                },
                        }
                    }?;
                {
                        match std::fs::create_dir_all(DIR_KSROOT) {
                            Ok(_) => Ok(()),
                            Err(e) =>
                                match e.kind() {
                                    std::io::ErrorKind::AlreadyExists => Ok(()),
                                    _ => Err(e),
                                },
                        }
                    }?;
                {
                        match std::fs::create_dir_all(DIR_BACKUPS) {
                            Ok(_) => Ok(()),
                            Err(e) =>
                                match e.kind() {
                                    std::io::ErrorKind::AlreadyExists => Ok(()),
                                    _ => Err(e),
                                },
                        }
                    }?;
                {
                        match std::fs::create_dir_all(DIR_SNAPROOT) {
                            Ok(_) => Ok(()),
                            Err(e) =>
                                match e.kind() {
                                    std::io::ErrorKind::AlreadyExists => Ok(()),
                                    _ => Err(e),
                                },
                        }
                    }?;
                {
                        match std::fs::create_dir_all(DIR_RSNAPROOT) {
                            Ok(_) => Ok(()),
                            Err(e) =>
                                match e.kind() {
                                    std::io::ErrorKind::AlreadyExists => Ok(()),
                                    _ => Err(e),
                                },
                        }
                    }?;
                ;
                self::create_tree(target, memroot)
            }
            /// Clean up the tree
            ///
            /// **Warning**: Calling this is quite inefficient so consider calling it once or twice
            /// throughout the lifecycle of the server
            pub fn cleanup_tree(memroot: &Memstore) -> IoResult<()> {
                if registry::get_cleanup_tripswitch().is_tripped() {
                        let mut dir_keyspaces: HashSet<String> =
                            std::fs::read_dir(DIR_KSROOT)?.map(|v|
                                        {
                                            v.expect("Unexpected directory parse failure").file_name().to_string_lossy().to_string()
                                        }).collect();
                        dir_keyspaces.remove("PRELOAD");
                        let our_keyspaces: HashSet<String> =
                            memroot.keyspaces.iter().map(|kv|
                                        unsafe { kv.key().as_str() }.to_owned()).collect();
                        for folder in dir_keyspaces.difference(&our_keyspaces) {
                            let ks_path =
                                {
                                    {
                                        let mut st =
                                            std::string::String::with_capacity((DIR_KSROOT).len() +
                                                            ("/").len() + (folder).len() + 0);
                                        st.push_str(DIR_KSROOT);
                                        st.push_str("/");
                                        st.push_str(folder);
                                        st
                                    }
                                };
                            fs::remove_dir_all(ks_path)?;
                        }
                        for keyspace in memroot.keyspaces.iter() {
                            let ks_path =
                                unsafe {
                                    {
                                        {
                                            let mut st =
                                                std::string::String::with_capacity((DIR_KSROOT).len() +
                                                                ("/").len() + (keyspace.key().as_str()).len() + 0);
                                            st.push_str(DIR_KSROOT);
                                            st.push_str("/");
                                            st.push_str(keyspace.key().as_str());
                                            st
                                        }
                                    }
                                };
                            let mut dir_tbls: HashSet<String> =
                                std::fs::read_dir(&ks_path)?.map(|v|
                                            {
                                                v.expect("Unexpected directory parse failure").file_name().to_string_lossy().to_string()
                                            }).collect();
                            dir_tbls.remove("PARTMAP");
                            let our_tbls: HashSet<String> =
                                keyspace.value().tables.iter().map(|v|
                                            unsafe { v.key().as_str() }.to_owned()).collect();
                            for old_file in dir_tbls.difference(&our_tbls) {
                                let fpath =
                                    {
                                        {
                                            let mut path =
                                                std::path::PathBuf::with_capacity((&ks_path).len() +
                                                            (old_file).len() + 0);
                                            path.push(&ks_path);
                                            path.push(old_file);
                                            path
                                        }
                                    };
                                fs::remove_file(&fpath)?;
                            }
                        }
                        for keyspace in memroot.keyspaces.iter() {
                            let ks_path =
                                unsafe {
                                    {
                                        {
                                            let mut st =
                                                std::string::String::with_capacity((DIR_KSROOT).len() +
                                                                ("/").len() + (keyspace.key().as_str()).len() + 0);
                                            st.push_str(DIR_KSROOT);
                                            st.push_str("/");
                                            st.push_str(keyspace.key().as_str());
                                            st
                                        }
                                    }
                                };
                            let dir_tbls: HashSet<String> =
                                std::fs::read_dir(&ks_path)?.map(|v|
                                            {
                                                v.expect("Unexpected directory parse failure").file_name().to_string_lossy().to_string()
                                            }).collect();
                            let our_tbls: HashSet<String> =
                                keyspace.value().tables.iter().map(|v|
                                            unsafe { v.key().as_str() }.to_owned()).collect();
                            for old_file in dir_tbls.difference(&our_tbls) {
                                if old_file != "PARTMAP" {
                                        fs::remove_file({
                                                    {
                                                        let mut path =
                                                            std::path::PathBuf::with_capacity((&ks_path).len() +
                                                                        (old_file).len() + 0);
                                                        path.push(&ks_path);
                                                        path.push(old_file);
                                                        path
                                                    }
                                                })?;
                                    }
                            }
                        }
                    }
                Ok(())
            }
            /// Uses a buffered writer under the hood to improve write performance as the provided
            /// writable interface might be very slow. The buffer does flush once done, however, it
            /// is important that you fsync yourself!
            pub fn serialize_into_slow_buffer<T: Write,
                U: FlushableTable>(buffer: &mut T, writable_item: &U)
                -> IoResult<()> {
                let mut buffer = BufWriter::new(buffer);
                writable_item.write_table_to(&mut buffer)?;
                buffer.flush()?;
                Ok(())
            }
            pub fn serialize_partmap_into_slow_buffer<T, U, Tbl,
                K>(buffer: &mut T, ks: &K) -> IoResult<()> where T: Write,
                U: Deref<Target = Tbl>, Tbl: FlushableTable,
                K: FlushableKeyspace<Tbl, U> {
                let mut buffer = BufWriter::new(buffer);
                super::se::raw_serialize_partmap(&mut buffer, ks)?;
                buffer.flush()?;
                Ok(())
            }
            pub fn serialize_preload_into_slow_buffer<T: Write>(buffer:
                    &mut T, store: &Memstore) -> IoResult<()> {
                let mut buffer = BufWriter::new(buffer);
                super::preload::raw_generate_preload(&mut buffer, store)?;
                buffer.flush()?;
                Ok(())
            }
        }
        pub mod iter {
            use crate::storage::v1::Data;
            use core::mem;
            use core::ptr;
            use core::slice;
            const SIZE_64BIT: usize = mem::size_of::<u64>();
            const SIZE_128BIT: usize = SIZE_64BIT * 2;
            /// This contains the fn ptr to decode bytes wrt to the host's endian. For example, if you're on an LE machine and
            /// you're reading data from a BE machine, then simply set the endian to big. This only affects the first read and not
            /// subsequent ones (unless you switch between machines of different endian, obviously)
            static mut NATIVE_ENDIAN_READER: unsafe fn(*const u8) -> usize =
                super::de::transmute_len;
            /// Use this to set the current endian to LE.
            ///
            /// ## Safety
            /// Make sure this is run from a single thread only! If not, good luck
            pub(super) unsafe fn endian_set_little() {
                NATIVE_ENDIAN_READER = super::de::transmute_len_le;
            }
            /// Use this to set the current endian to BE.
            ///
            /// ## Safety
            /// Make sure this is run from a single thread only! If not, good luck
            pub(super) unsafe fn endian_set_big() {
                NATIVE_ENDIAN_READER = super::de::transmute_len_be;
            }
            /// A raw slice iterator by using raw pointers
            pub struct RawSliceIter<'a> {
                _base: &'a [u8],
                cursor: *const u8,
                terminal: *const u8,
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl<'a> ::core::fmt::Debug for RawSliceIter<'a> {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match *self {
                        RawSliceIter {
                            _base: ref __self_0_0,
                            cursor: ref __self_0_1,
                            terminal: ref __self_0_2 } => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_struct(f,
                                        "RawSliceIter");
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "_base", &&(*__self_0_0));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "cursor", &&(*__self_0_1));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "terminal", &&(*__self_0_2));
                            ::core::fmt::DebugStruct::finish(debug_trait_builder)
                        }
                    }
                }
            }
            impl<'a> RawSliceIter<'a> {
                /// Create a new slice iterator
                pub fn new(slice: &'a [u8]) -> Self {
                    Self {
                        cursor: slice.as_ptr(),
                        terminal: unsafe { slice.as_ptr().add(slice.len()) },
                        _base: slice,
                    }
                }
                /// Check the number of remaining bytes in the buffer
                fn remaining(&self) -> usize {
                    unsafe { self.terminal.offset_from(self.cursor) as usize }
                }
                /// Increment the cursor by one
                unsafe fn incr_cursor(&mut self) { self.incr_cursor_by(1) }
                /// Check if the buffer was exhausted
                fn exhausted(&self) -> bool { self.cursor > self.terminal }
                /// Increment the cursor by the provided length
                unsafe fn incr_cursor_by(&mut self, ahead: usize) {
                    { self.cursor = self.cursor.add(ahead) }
                }
                /// Get the next 64-bit integer, casting it to an `usize`, respecting endianness
                pub fn next_64bit_integer_to_usize(&mut self)
                    -> Option<usize> {
                    if self.remaining() < 8 {
                            None
                        } else {
                           unsafe {
                               let l = NATIVE_ENDIAN_READER(self.cursor);
                               self.incr_cursor_by(SIZE_64BIT);
                               Some(l)
                           }
                       }
                }
                /// Get a borrowed slice for the given length. The lifetime is important!
                pub fn next_borrowed_slice(&mut self, len: usize)
                    -> Option<&'a [u8]> {
                    if self.remaining() < len {
                            None
                        } else {
                           unsafe {
                               let d = slice::from_raw_parts(self.cursor, len);
                               self.incr_cursor_by(len);
                               Some(d)
                           }
                       }
                }
                /// Get the next 64-bit usize
                pub fn next_64bit_integer_pair_to_usize(&mut self)
                    -> Option<(usize, usize)> {
                    if self.remaining() < SIZE_128BIT {
                            None
                        } else {
                           unsafe {
                               let v1 = NATIVE_ENDIAN_READER(self.cursor);
                               self.incr_cursor_by(SIZE_64BIT);
                               let v2 = NATIVE_ENDIAN_READER(self.cursor);
                               self.incr_cursor_by(SIZE_64BIT);
                               Some((v1, v2))
                           }
                       }
                }
                /// Get the next owned [`Data`] with the provided length
                pub fn next_owned_data(&mut self, len: usize)
                    -> Option<Data> {
                    if self.remaining() < len {
                            None
                        } else {
                           unsafe {
                               let d = slice::from_raw_parts(self.cursor, len);
                               let d = Some(Data::copy_from_slice(d));
                               self.incr_cursor_by(len);
                               d
                           }
                       }
                }
                /// Get the next 8-bit unsigned integer
                pub fn next_8bit_integer(&mut self) -> Option<u8> {
                    if self.exhausted() {
                            None
                        } else {
                           unsafe {
                               let x = ptr::read(self.cursor);
                               self.incr_cursor();
                               Some(x)
                           }
                       }
                }
                /// Check if the cursor has reached end-of-allocation
                pub fn end_of_allocation(&self) -> bool {
                    self.cursor == self.terminal
                }
                /// Get a borrowed iterator. This is super safe, funny enough, because of the lifetime
                /// bound that we add to the iterator object
                pub fn get_borrowed_iter(&mut self)
                    -> RawSliceIterBorrowed<'_> {
                    RawSliceIterBorrowed::new(self.cursor, self.terminal,
                        &mut self.cursor)
                }
            }
            pub struct RawSliceIterBorrowed<'a> {
                cursor: *const u8,
                end_ptr: *const u8,
                mut_ptr: &'a mut *const u8,
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl<'a> ::core::fmt::Debug for RawSliceIterBorrowed<'a> {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match *self {
                        RawSliceIterBorrowed {
                            cursor: ref __self_0_0,
                            end_ptr: ref __self_0_1,
                            mut_ptr: ref __self_0_2 } => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_struct(f,
                                        "RawSliceIterBorrowed");
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "cursor", &&(*__self_0_0));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "end_ptr", &&(*__self_0_1));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "mut_ptr", &&(*__self_0_2));
                            ::core::fmt::DebugStruct::finish(debug_trait_builder)
                        }
                    }
                }
            }
            impl<'a> RawSliceIterBorrowed<'a> {
                fn new(cursor: *const u8, end_ptr: *const u8,
                    mut_ptr: &'a mut *const u8) -> RawSliceIterBorrowed<'a> {
                    Self { cursor, end_ptr, mut_ptr }
                }
                /// Check the number of remaining bytes in the buffer
                fn remaining(&self) -> usize {
                    unsafe { self.end_ptr.offset_from(self.cursor) as usize }
                }
                /// Increment the cursor by the provided length
                unsafe fn incr_cursor_by(&mut self, ahead: usize) {
                    { self.cursor = self.cursor.add(ahead) }
                }
                pub fn next_64bit_integer_to_usize(&mut self)
                    -> Option<usize> {
                    if self.remaining() < 8 {
                            None
                        } else {
                           unsafe {
                               let size = NATIVE_ENDIAN_READER(self.cursor);
                               self.incr_cursor_by(SIZE_64BIT);
                               Some(size)
                           }
                       }
                }
                pub fn next_owned_data(&mut self, len: usize)
                    -> Option<Data> {
                    if self.remaining() < len {
                            None
                        } else {
                           unsafe {
                               let d = slice::from_raw_parts(self.cursor, len);
                               let d = Some(Data::copy_from_slice(d));
                               self.incr_cursor_by(len);
                               d
                           }
                       }
                }
            }
            impl<'a> Drop for RawSliceIterBorrowed<'a> {
                fn drop(&mut self) { *self.mut_ptr = self.cursor; }
            }
        }
        pub mod preload {
            //! # Preload binary files
            //!
            //! Preloads are very critical binary files which contain metadata for this instance of
            //! the database. Preloads are of two kinds:
            //! 1. the `PRELOAD` that is placed at the root directory
            //! 2. the `PARTMAP` preload that is placed in the ks directory
            //!
            use crate::corestore::memstore::Memstore;
            use crate::corestore::memstore::ObjectID;
            use crate::storage::v1::error::{
                StorageEngineError, StorageEngineResult,
            };
            use crate::IoResult;
            use core::ptr;
            use std::collections::HashMap;
            use std::collections::HashSet;
            use std::io::Write;
            pub type LoadedPartfile = HashMap<ObjectID, (u8, u8)>;
            const META_SEGMENT_LE: u8 = 0b1000_0000;
            const META_SEGMENT_BE: u8 = 0b1000_0001;
            #[cfg(target_endian = "little")]
            const META_SEGMENT: u8 = META_SEGMENT_LE;
            /// Generate the `PRELOAD` disk file for this instance
            /// ```text
            /// [1B: Endian Mark/Version Mark (padded)] => Meta segment
            /// [8B: Extent header] => Predata Segment
            /// ([8B: Partion ID len][8B: Parition ID (not padded)])* => Data segment
            /// ```
            ///
            pub(super) fn raw_generate_preload<W: Write>(w: &mut W,
                store: &Memstore) -> IoResult<()> {
                w.write_all(&[META_SEGMENT])?;
                super::se::raw_serialize_set(&store.keyspaces, w)?;
                Ok(())
            }
            /// Reads the preload file and returns a set
            pub(super) fn read_preload_raw(preload: Vec<u8>)
                -> StorageEngineResult<HashSet<ObjectID>> {
                if preload.len() < 16 {
                        return Err(StorageEngineError::corrupted_preload());
                    }
                unsafe {
                    let meta_segment: u8 = ptr::read(preload.as_ptr());
                    match meta_segment {
                        META_SEGMENT_BE => { super::iter::endian_set_big(); }
                        META_SEGMENT_LE => { super::iter::endian_set_little(); }
                        _ =>
                            return Err(StorageEngineError::BadMetadata("preload".into())),
                    }
                }
                super::de::deserialize_set_ctype(&preload[1..]).ok_or_else(StorageEngineError::corrupted_preload)
            }
        }
        pub mod sengine {
            use self::queue::Queue;
            use super::interface::{DIR_RSNAPROOT, DIR_SNAPROOT};
            use crate::corestore::iarray::IArray;
            use crate::corestore::lazy::Lazy;
            use crate::corestore::lock::QuickLock;
            use crate::corestore::memstore::Memstore;
            use crate::storage::v1::flush::{LocalSnapshot, RemoteSnapshot};
            use bytes::Bytes;
            use chrono::prelude::Utc;
            use core::fmt;
            use core::str;
            use regex::Regex;
            use std::collections::HashSet;
            use std::fs;
            use std::io::Error as IoError;
            use std::path::Path;
            use std::sync::Arc;
            type QStore = IArray<[String; 64]>;
            type SnapshotResult<T> = Result<T, SnapshotEngineError>;
            /// Matches any string which is in the following format:
            /// ```text
            /// YYYYMMDD-HHMMSS
            /// ```
            pub static SNAP_MATCH: Lazy<Regex, fn() -> Regex> =
                Lazy::new(||
                        {
                            Regex::new("^\\d{4}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])(-)(?:(?:([01]?\\d|2[0-3]))?([0-5]?\\d))?([0-5]?\\d)$").unwrap()
                        });
            pub enum SnapshotEngineError {
                Io(IoError),
                Engine(&'static str),
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for SnapshotEngineError {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match (&*self,) {
                        (&SnapshotEngineError::Io(ref __self_0),) => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_tuple(f, "Io");
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_0));
                            ::core::fmt::DebugTuple::finish(debug_trait_builder)
                        }
                        (&SnapshotEngineError::Engine(ref __self_0),) => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_tuple(f, "Engine");
                            let _ =
                                ::core::fmt::DebugTuple::field(debug_trait_builder,
                                    &&(*__self_0));
                            ::core::fmt::DebugTuple::finish(debug_trait_builder)
                        }
                    }
                }
            }
            impl From<IoError> for SnapshotEngineError {
                fn from(e: IoError) -> SnapshotEngineError {
                    SnapshotEngineError::Io(e)
                }
            }
            impl From<&'static str> for SnapshotEngineError {
                fn from(e: &'static str) -> SnapshotEngineError {
                    SnapshotEngineError::Engine(e)
                }
            }
            impl fmt::Display for SnapshotEngineError {
                fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>)
                    -> std::result::Result<(), fmt::Error> {
                    match self {
                        Self::Engine(estr) => {
                            formatter.write_str("Snapshot engine error")?;
                            formatter.write_str(estr)?;
                        }
                        Self::Io(e) => {
                            formatter.write_str("Snapshot engine IOError:")?;
                            formatter.write_str(&e.to_string())?;
                        }
                    }
                    Ok(())
                }
            }
            /// The snapshot engine
            pub struct SnapshotEngine {
                local_enabled: bool,
                /// the local snapshot queue
                local_queue: QuickLock<Queue>,
                /// the remote snapshot lock
                remote_queue: QuickLock<HashSet<Bytes>>,
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for SnapshotEngine {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match *self {
                        SnapshotEngine {
                            local_enabled: ref __self_0_0,
                            local_queue: ref __self_0_1,
                            remote_queue: ref __self_0_2 } => {
                            let debug_trait_builder =
                                &mut ::core::fmt::Formatter::debug_struct(f,
                                        "SnapshotEngine");
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "local_enabled", &&(*__self_0_0));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "local_queue", &&(*__self_0_1));
                            let _ =
                                ::core::fmt::DebugStruct::field(debug_trait_builder,
                                    "remote_queue", &&(*__self_0_2));
                            ::core::fmt::DebugStruct::finish(debug_trait_builder)
                        }
                    }
                }
            }
            pub enum SnapshotActionResult {
                Ok,
                Busy,
                Disabled,
                Failure,
                AlreadyExists,
            }
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::fmt::Debug for SnapshotActionResult {
                fn fmt(&self, f: &mut ::core::fmt::Formatter)
                    -> ::core::fmt::Result {
                    match (&*self,) {
                        (&SnapshotActionResult::Ok,) => {
                            ::core::fmt::Formatter::write_str(f, "Ok")
                        }
                        (&SnapshotActionResult::Busy,) => {
                            ::core::fmt::Formatter::write_str(f, "Busy")
                        }
                        (&SnapshotActionResult::Disabled,) => {
                            ::core::fmt::Formatter::write_str(f, "Disabled")
                        }
                        (&SnapshotActionResult::Failure,) => {
                            ::core::fmt::Formatter::write_str(f, "Failure")
                        }
                        (&SnapshotActionResult::AlreadyExists,) => {
                            ::core::fmt::Formatter::write_str(f, "AlreadyExists")
                        }
                    }
                }
            }
            impl ::core::marker::StructuralPartialEq for SnapshotActionResult
                {}
            #[automatically_derived]
            #[allow(unused_qualifications)]
            impl ::core::cmp::PartialEq for SnapshotActionResult {
                #[inline]
                fn eq(&self, other: &SnapshotActionResult) -> bool {
                    {
                        let __self_vi =
                            ::core::intrinsics::discriminant_value(&*self);
                        let __arg_1_vi =
                            ::core::intrinsics::discriminant_value(&*other);
                        if true && __self_vi == __arg_1_vi {
                                match (&*self, &*other) { _ => true, }
                            } else { false }
                    }
                }
            }
            impl SnapshotEngine {
                /// Returns a fresh, uninitialized snapshot engine instance
                pub fn new(maxlen: usize) -> Self {
                    Self {
                        local_enabled: true,
                        local_queue: QuickLock::new(Queue::new(maxlen,
                                maxlen == 0)),
                        remote_queue: QuickLock::new(HashSet::new()),
                    }
                }
                pub fn new_disabled() -> Self {
                    Self {
                        local_enabled: false,
                        local_queue: QuickLock::new(Queue::new(0, true)),
                        remote_queue: QuickLock::new(HashSet::new()),
                    }
                }
                fn _parse_dir(dir: &str, is_okay: impl Fn(&str) -> bool,
                    mut append: impl FnMut(String)) -> SnapshotResult<()> {
                    let dir = fs::read_dir(dir)?;
                    for entry in dir {
                        let entry = entry?;
                        if entry.file_type()?.is_dir() {
                                let fname = entry.file_name();
                                let name = fname.to_string_lossy();
                                if !is_okay(&name) {
                                        return Err("unknown folder in snapshot directory".into());
                                    }
                                append(name.to_string());
                            } else {
                               return Err("unrecognized file in snapshot directory".into());
                           }
                    }
                    Ok(())
                }
                pub fn parse_dir(&self) -> SnapshotResult<()> {
                    let mut local_queue = self.local_queue.lock();
                    Self::_parse_dir(DIR_SNAPROOT,
                            |name| SNAP_MATCH.is_match(name),
                            |snapshot| local_queue.push(snapshot))?;
                    let mut remote_queue = self.remote_queue.lock();
                    Self::_parse_dir(DIR_RSNAPROOT, |_| true,
                            |rsnap| { remote_queue.insert(Bytes::from(rsnap)); })?;
                    Ok(())
                }
                /// Generate the snapshot name
                fn get_snapname(&self) -> String {
                    Utc::now().format("%Y%m%d-%H%M%S").to_string()
                }
                fn _mksnap_blocking_section(store: &Memstore, name: String)
                    -> SnapshotResult<()> {
                    if Path::new(&{
                                            let res =
                                                ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["",
                                                                    "/"],
                                                        &[::core::fmt::ArgumentV1::new_display(&DIR_SNAPROOT),
                                                                    ::core::fmt::ArgumentV1::new_display(&name)]));
                                            res
                                        }).exists() {
                            Err(SnapshotEngineError::Engine("Server time is incorrect"))
                        } else {
                           let snapshot = LocalSnapshot::new(name);
                           super::flush::flush_full(snapshot, store)?;
                           Ok(())
                       }
                }
                fn _rmksnap_blocking_section(store: &Memstore, name: &str)
                    -> SnapshotResult<()> {
                    let snapshot = RemoteSnapshot::new(name);
                    super::flush::flush_full(snapshot, store)?;
                    Ok(())
                }
                /// Spawns a blocking task on a threadpool for blocking tasks. Returns either of:
                /// - `0` => Okay (returned **even if old snap deletion failed**)
                /// - `1` => Error
                /// - `2` => Disabled
                /// - `3` => Busy
                pub async fn mksnap(&self, store: Arc<Memstore>)
                    -> SnapshotActionResult {
                    if self.local_enabled {
                            let mut queue =
                                match self.local_queue.try_lock() {
                                    Some(lck) => lck,
                                    None => return SnapshotActionResult::Busy,
                                };
                            let name = self.get_snapname();
                            let nameclone = name.clone();
                            let todel = queue.add_new(name);
                            let snap_create_result =
                                tokio::task::spawn_blocking(move ||
                                                {
                                                    Self::_mksnap_blocking_section(&store, nameclone)
                                                }).await.expect("mksnap thread panicked");
                            match snap_create_result {
                                Ok(_) => {
                                    {
                                        let lvl = ::log::Level::Info;
                                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                    lvl <= ::log::max_level() {
                                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Successfully created snapshot"],
                                                        &[]), lvl,
                                                    &("skyd::storage::v1::sengine",
                                                            "skyd::storage::v1::sengine",
                                                            "server/src/storage/v1/sengine.rs", 205u32),
                                                    ::log::__private_api::Option::None);
                                            }
                                    };
                                }
                                Err(e) => {
                                    {
                                        let lvl = ::log::Level::Error;
                                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                    lvl <= ::log::max_level() {
                                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Failed to create snapshot with error: "],
                                                        &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                    &("skyd::storage::v1::sengine",
                                                            "skyd::storage::v1::sengine",
                                                            "server/src/storage/v1/sengine.rs", 208u32),
                                                    ::log::__private_api::Option::None);
                                            }
                                    };
                                    let _ = queue.pop_last().unwrap();
                                    return SnapshotActionResult::Failure;
                                }
                            }
                            if let Some(snap) = todel {
                                    tokio::task::spawn_blocking(move ||
                                                    {
                                                        if let Err(e) =
                                                                    fs::remove_dir_all({
                                                                            {
                                                                                let mut path =
                                                                                    std::path::PathBuf::with_capacity((DIR_SNAPROOT).len() +
                                                                                                (snap).len() + 0);
                                                                                path.push(DIR_SNAPROOT);
                                                                                path.push(snap);
                                                                                path
                                                                            }
                                                                        }) {
                                                                {
                                                                    let lvl = ::log::Level::Warn;
                                                                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                                                lvl <= ::log::max_level() {
                                                                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Failed to remove older snapshot (ignored): "],
                                                                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                                                &("skyd::storage::v1::sengine",
                                                                                        "skyd::storage::v1::sengine",
                                                                                        "server/src/storage/v1/sengine.rs", 219u32),
                                                                                ::log::__private_api::Option::None);
                                                                        }
                                                                };
                                                            } else {
                                                               {
                                                                   let lvl = ::log::Level::Info;
                                                                   if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                                               lvl <= ::log::max_level() {
                                                                           ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Successfully removed older snapshot"],
                                                                                   &[]), lvl,
                                                                               &("skyd::storage::v1::sengine",
                                                                                       "skyd::storage::v1::sengine",
                                                                                       "server/src/storage/v1/sengine.rs", 221u32),
                                                                               ::log::__private_api::Option::None);
                                                                       }
                                                               };
                                                           }
                                                    }).await.expect("mksnap thread panicked");
                                }
                            drop(queue);
                            SnapshotActionResult::Ok
                        } else { SnapshotActionResult::Disabled }
                }
                /// Spawns a blocking task to create a remote snapshot. Returns either of:
                /// - `0` => Okay
                /// - `1` => Error
                /// - `3` => Busy
                /// (consistent with mksnap)
                pub async fn mkrsnap(&self, name: Bytes, store: Arc<Memstore>)
                    -> SnapshotActionResult {
                    let mut remq =
                        match self.remote_queue.try_lock() {
                            Some(q) => q,
                            None => return SnapshotActionResult::Busy,
                        };
                    if remq.contains(&name) {
                            SnapshotActionResult::AlreadyExists
                        } else {
                           let nameclone = name.clone();
                           let ret =
                               tokio::task::spawn_blocking(move ||
                                               {
                                                   let name_str =
                                                       unsafe { str::from_utf8_unchecked(&nameclone) };
                                                   if let Err(e) =
                                                               Self::_rmksnap_blocking_section(&store, name_str) {
                                                           {
                                                               let lvl = ::log::Level::Error;
                                                               if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                                           lvl <= ::log::max_level() {
                                                                       ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Remote snapshot failed with: "],
                                                                               &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                                                           &("skyd::storage::v1::sengine",
                                                                                   "skyd::storage::v1::sengine",
                                                                                   "server/src/storage/v1/sengine.rs", 253u32),
                                                                           ::log::__private_api::Option::None);
                                                                   }
                                                           };
                                                           SnapshotActionResult::Failure
                                                       } else {
                                                          {
                                                              let lvl = ::log::Level::Info;
                                                              if lvl <= ::log::STATIC_MAX_LEVEL &&
                                                                          lvl <= ::log::max_level() {
                                                                      ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Remote snapshot succeeded"],
                                                                              &[]), lvl,
                                                                          &("skyd::storage::v1::sengine",
                                                                                  "skyd::storage::v1::sengine",
                                                                                  "server/src/storage/v1/sengine.rs", 256u32),
                                                                          ::log::__private_api::Option::None);
                                                                  }
                                                          };
                                                          SnapshotActionResult::Ok
                                                      }
                                               }).await.expect("rmksnap thread panicked");
                           if !remq.insert(name) {
                                   ::core::panicking::panic("assertion failed: remq.insert(name)")
                               };
                           ret
                       }
                }
            }
            mod queue {
                //! An extremely simple queue implementation which adds more items to the queue
                //! freely and once the threshold limit is reached, it pops off the oldest element and returns it
                //!
                //! This implementation is specifically built for use with the snapshotting utility
                use super::QStore;
                use crate::corestore::iarray;
                pub struct Queue {
                    queue: QStore,
                    maxlen: usize,
                    dontpop: bool,
                }
                #[automatically_derived]
                #[allow(unused_qualifications)]
                impl ::core::fmt::Debug for Queue {
                    fn fmt(&self, f: &mut ::core::fmt::Formatter)
                        -> ::core::fmt::Result {
                        match *self {
                            Queue {
                                queue: ref __self_0_0,
                                maxlen: ref __self_0_1,
                                dontpop: ref __self_0_2 } => {
                                let debug_trait_builder =
                                    &mut ::core::fmt::Formatter::debug_struct(f, "Queue");
                                let _ =
                                    ::core::fmt::DebugStruct::field(debug_trait_builder,
                                        "queue", &&(*__self_0_0));
                                let _ =
                                    ::core::fmt::DebugStruct::field(debug_trait_builder,
                                        "maxlen", &&(*__self_0_1));
                                let _ =
                                    ::core::fmt::DebugStruct::field(debug_trait_builder,
                                        "dontpop", &&(*__self_0_2));
                                ::core::fmt::DebugStruct::finish(debug_trait_builder)
                            }
                        }
                    }
                }
                impl ::core::marker::StructuralPartialEq for Queue {}
                #[automatically_derived]
                #[allow(unused_qualifications)]
                impl ::core::cmp::PartialEq for Queue {
                    #[inline]
                    fn eq(&self, other: &Queue) -> bool {
                        match *other {
                            Queue {
                                queue: ref __self_1_0,
                                maxlen: ref __self_1_1,
                                dontpop: ref __self_1_2 } =>
                                match *self {
                                    Queue {
                                        queue: ref __self_0_0,
                                        maxlen: ref __self_0_1,
                                        dontpop: ref __self_0_2 } =>
                                        (*__self_0_0) == (*__self_1_0) &&
                                                (*__self_0_1) == (*__self_1_1) &&
                                            (*__self_0_2) == (*__self_1_2),
                                },
                        }
                    }
                    #[inline]
                    fn ne(&self, other: &Queue) -> bool {
                        match *other {
                            Queue {
                                queue: ref __self_1_0,
                                maxlen: ref __self_1_1,
                                dontpop: ref __self_1_2 } =>
                                match *self {
                                    Queue {
                                        queue: ref __self_0_0,
                                        maxlen: ref __self_0_1,
                                        dontpop: ref __self_0_2 } =>
                                        (*__self_0_0) != (*__self_1_0) ||
                                                (*__self_0_1) != (*__self_1_1) ||
                                            (*__self_0_2) != (*__self_1_2),
                                },
                        }
                    }
                }
                impl Queue {
                    pub const fn new(maxlen: usize, dontpop: bool) -> Self {
                        Queue { queue: iarray::new_const_iarray(), maxlen, dontpop }
                    }
                    pub fn push(&mut self, item: String) {
                        self.queue.push(item)
                    }
                    /// This returns a `String` only if the queue is full. Otherwise, a `None` is returned most of the time
                    pub fn add_new(&mut self, item: String) -> Option<String> {
                        if self.dontpop {
                                self.queue.push(item);
                                None
                            } else {
                               let x = if self.is_overflow() { self.pop() } else { None };
                               self.queue.push(item);
                               x
                           }
                    }
                    /// Check if we have reached the maximum queue size limit
                    fn is_overflow(&self) -> bool {
                        self.queue.len() == self.maxlen
                    }
                    /// Remove the last item inserted
                    fn pop(&mut self) -> Option<String> {
                        if self.queue.is_empty() {
                                None
                            } else { Some(unsafe { self.queue.remove(0) }) }
                    }
                    pub fn pop_last(&mut self) -> Option<String> {
                        self.queue.pop()
                    }
                }
            }
        }
        pub mod unflush {
            //! # Unflush routines
            //!
            //! Routines for unflushing data
            use super::bytemarks;
            use crate::{
                corestore::{
                    memstore::{
                        Keyspace, Memstore, ObjectID, SystemKeyspace, SYSTEM,
                    },
                    table::{SystemTable, Table},
                },
                storage::v1::{
                    de::DeserializeInto,
                    error::{
                        ErrorContext, StorageEngineError, StorageEngineResult,
                    },
                    flush::Autoflush, interface::DIR_KSROOT,
                    preload::LoadedPartfile, Coremap,
                },
                util::Wrapper,
            };
            use core::mem::transmute;
            use std::{fs, io::ErrorKind, path::Path, sync::Arc};
            type PreloadSet = std::collections::HashSet<ObjectID>;
            const PRELOAD_PATH: &str = "data/ks/PRELOAD";
            /// A keyspace that can be restored from disk storage
            pub trait UnflushableKeyspace: Sized {
                /// Unflush routine for a keyspace
                fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID)
                -> StorageEngineResult<Self>;
            }
            impl UnflushableKeyspace for Keyspace {
                fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID)
                    -> StorageEngineResult<Self> {
                    let ks: Coremap<ObjectID, Arc<Table>> =
                        Coremap::with_capacity(partmap.len());
                    for (tableid, (table_storage_type, model_code)) in
                        partmap.into_iter() {
                        if table_storage_type > 1 {
                                return Err(StorageEngineError::bad_metadata_in_table(ksid,
                                            &tableid));
                            }
                        let is_volatile =
                            table_storage_type == bytemarks::BYTEMARK_STORAGE_VOLATILE;
                        let tbl =
                            self::read_table::<Table>(ksid, &tableid, is_volatile,
                                    model_code)?;
                        ks.true_if_insert(tableid, Arc::new(tbl));
                    }
                    Ok(Keyspace::init_with_all_def_strategy(ks))
                }
            }
            impl UnflushableKeyspace for SystemKeyspace {
                fn unflush_keyspace(partmap: LoadedPartfile, ksid: &ObjectID)
                    -> StorageEngineResult<Self> {
                    let ks: Coremap<ObjectID, Wrapper<SystemTable>> =
                        Coremap::with_capacity(partmap.len());
                    for (tableid, (table_storage_type, model_code)) in
                        partmap.into_iter() {
                        if table_storage_type > 1 {
                                return Err(StorageEngineError::bad_metadata_in_table(ksid,
                                            &tableid));
                            }
                        let is_volatile =
                            table_storage_type == bytemarks::BYTEMARK_STORAGE_VOLATILE;
                        let tbl =
                            self::read_table::<SystemTable>(ksid, &tableid, is_volatile,
                                    model_code)?;
                        ks.true_if_insert(tableid, Wrapper::new(tbl));
                    }
                    Ok(SystemKeyspace::new(ks))
                }
            }
            /// Tables that can be restored from disk storage
            pub trait UnflushableTable: Sized {
                /// Procedure to restore (deserialize) table from disk storage
                fn unflush_table(filepath: impl AsRef<Path>, model_code: u8,
                volatile: bool)
                -> StorageEngineResult<Self>;
            }
            #[allow(clippy :: transmute_int_to_bool)]
            impl UnflushableTable for Table {
                fn unflush_table(filepath: impl AsRef<Path>, model_code: u8,
                    volatile: bool) -> StorageEngineResult<Self> {
                    let ret =
                        match model_code {
                            x if x < 4 => {
                                let data = decode(filepath, volatile)?;
                                let (k_enc, v_enc) =
                                    unsafe {
                                        let key: bool = transmute(model_code >> 1);
                                        let value: bool =
                                            transmute(((model_code >> 1) + (model_code & 1)) % 2);
                                        (key, value)
                                    };
                                Table::new_pure_kve_with_data(data, volatile, k_enc, v_enc)
                            }
                            x if x < 8 => {
                                let data = decode(filepath, volatile)?;
                                let (k_enc, v_enc) =
                                    unsafe {
                                        let code = model_code - 4;
                                        let key: bool = transmute(code >> 1);
                                        let value: bool = transmute(code % 2);
                                        (key, value)
                                    };
                                Table::new_kve_listmap_with_data(data, volatile, k_enc,
                                    v_enc)
                            }
                            _ => {
                                return Err(StorageEngineError::BadMetadata(filepath.as_ref().to_string_lossy().to_string()))
                            }
                        };
                    Ok(ret)
                }
            }
            impl UnflushableTable for SystemTable {
                fn unflush_table(filepath: impl AsRef<Path>, model_code: u8,
                    volatile: bool) -> StorageEngineResult<Self> {
                    match model_code {
                        0 => {
                            let authmap = decode(filepath, volatile)?;
                            Ok(SystemTable::new_auth(Arc::new(authmap)))
                        }
                        _ =>
                            Err(StorageEngineError::BadMetadata(filepath.as_ref().to_string_lossy().to_string())),
                    }
                }
            }
            #[inline(always)]
            fn decode<T: DeserializeInto>(filepath: impl AsRef<Path>,
                volatile: bool) -> StorageEngineResult<T> {
                if volatile {
                        Ok(T::new_empty())
                    } else {
                       let data =
                           fs::read(filepath.as_ref()).map_err_context({
                                       let res =
                                           ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["reading file "],
                                                   &[::core::fmt::ArgumentV1::new_display(&filepath.as_ref().to_string_lossy())]));
                                       res
                                   })?;
                       super::de::deserialize_into(&data).ok_or_else(||
                               {
                                   StorageEngineError::CorruptedFile(filepath.as_ref().to_string_lossy().to_string())
                               })
                   }
            }
            /// Read a given table into a [`Table`] object
            ///
            /// This will take care of volatility and the model_code. Just make sure that you pass the proper
            /// keyspace ID and a valid table ID
            pub fn read_table<T: UnflushableTable>(ksid: &ObjectID,
                tblid: &ObjectID, volatile: bool, model_code: u8)
                -> StorageEngineResult<T> {
                let filepath =
                    unsafe {
                        {
                            {
                                let mut path =
                                    std::path::PathBuf::with_capacity((DIR_KSROOT).len() +
                                                    (ksid.as_str()).len() + (tblid.as_str()).len() + 0);
                                path.push(DIR_KSROOT);
                                path.push(ksid.as_str());
                                path.push(tblid.as_str());
                                path
                            }
                        }
                    };
                let tbl = T::unflush_table(filepath, model_code, volatile)?;
                Ok(tbl)
            }
            /// Read an entire keyspace into a Coremap. You'll need to initialize the rest
            pub fn read_keyspace<K: UnflushableKeyspace>(ksid: &ObjectID)
                -> StorageEngineResult<K> {
                let partmap = self::read_partmap(ksid)?;
                K::unflush_keyspace(partmap, ksid)
            }
            /// Read the `PARTMAP` for a given keyspace
            pub fn read_partmap(ksid: &ObjectID)
                -> StorageEngineResult<LoadedPartfile> {
                let ksid_str = unsafe { ksid.as_str() };
                let filepath =
                    {
                        {
                            let mut path =
                                std::path::PathBuf::with_capacity((DIR_KSROOT).len() +
                                                (ksid_str).len() + ("PARTMAP").len() + 0);
                            path.push(DIR_KSROOT);
                            path.push(ksid_str);
                            path.push("PARTMAP");
                            path
                        }
                    };
                let partmap_raw =
                    fs::read(&filepath).map_err_context({
                                let res =
                                    ::alloc::fmt::format(::core::fmt::Arguments::new_v1(&["while reading "],
                                            &[::core::fmt::ArgumentV1::new_display(&filepath.to_string_lossy())]));
                                res
                            })?;
                super::de::deserialize_set_ctype_bytemark(&partmap_raw).ok_or_else(||
                        StorageEngineError::corrupted_partmap(ksid))
            }
            /// Read the `PRELOAD`
            pub fn read_preload() -> StorageEngineResult<PreloadSet> {
                let read =
                    fs::read(PRELOAD_PATH).map_err_context("reading PRELOAD")?;
                super::preload::read_preload_raw(read)
            }
            /// Read everything and return a [`Memstore`]
            ///
            /// If this is a new instance an empty store is returned while the directory tree
            /// is also created. If this is an already initialized instance then the store
            /// is read and returned (and any possible errors that are encountered are returned)
            pub fn read_full() -> StorageEngineResult<Memstore> {
                if is_new_instance()? {
                        {
                            let lvl = ::log::Level::Trace;
                            if lvl <= ::log::STATIC_MAX_LEVEL &&
                                        lvl <= ::log::max_level() {
                                    ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Detected new instance. Creating data directory"],
                                            &[]), lvl,
                                        &("skyd::storage::v1::unflush",
                                                "skyd::storage::v1::unflush",
                                                "server/src/storage/v1/unflush.rs", 221u32),
                                        ::log::__private_api::Option::None);
                                }
                        };
                        let store = Memstore::new_default();
                        let target = Autoflush;
                        super::interface::create_tree_fresh(&target, &store)?;
                        super::flush::oneshot::flush_preload(&target, &store)?;
                        super::flush::flush_full(target, &store)?;
                        return Ok(store);
                    }
                let mut preload = self::read_preload()?;
                if !preload.remove(&SYSTEM) {
                        ::core::panicking::panic("assertion failed: preload.remove(&SYSTEM)")
                    };
                let system_keyspace =
                    self::read_keyspace::<SystemKeyspace>(&SYSTEM)?;
                let ksmap = Coremap::with_capacity(preload.len());
                for ksid in preload {
                    let ks = self::read_keyspace::<Keyspace>(&ksid)?;
                    ksmap.upsert(ksid, Arc::new(ks));
                }
                ksmap.upsert(SYSTEM, Arc::new(Keyspace::empty()));
                Ok(Memstore::init_with_all(ksmap, system_keyspace))
            }
            /// Check if the `data` directory is non-empty (if not: we're on a new instance)
            pub fn is_new_instance() -> StorageEngineResult<bool> {
                match fs::read_dir("data") {
                    Ok(mut dir) => Ok(dir.next().is_none()),
                    Err(e) if e.kind().eq(&ErrorKind::NotFound) => Ok(true),
                    Err(e) =>
                        Err(StorageEngineError::ioerror_extra(e,
                                "while checking data directory")),
                }
            }
        }
        /// Get the raw bytes of anything.
        ///
        /// DISCLAIMER: THIS FUNCTION CAN DO TERRIBLE THINGS (especially when you think about padding)
        unsafe fn raw_byte_repr<'a, T: 'a>(len: &'a T) -> &'a [u8] {
            {
                let ptr: *const u8 = mem::transmute(len);
                slice::from_raw_parts::<'a>(ptr, mem::size_of::<T>())
            }
        }
        mod se {
            use super::*;
            use crate::kvengine::LockedVec;
            use crate::storage::v1::flush::FlushableKeyspace;
            use crate::storage::v1::flush::FlushableTable;
            use crate::IoResult;
            use core::ops::Deref;
            macro_rules! unsafe_sz_byte_repr {
                ($e : expr) =>
                { raw_byte_repr(& to_64bit_native_endian! ($e)) } ;
            }
            /// Serialize a map and write it to a provided buffer
            pub fn raw_serialize_map<W: Write, T: AsRef<[u8]>,
                U: AsRef<[u8]>>(map: &Coremap<T, U>, w: &mut W)
                -> IoResult<()> where W: Write, T: AsRef<[u8]> + Hash + Eq,
                U: AsRef<[u8]> {
                unsafe {
                    w.write_all(raw_byte_repr(&(map.len() as u64)))?;
                    for kv in map.iter() {
                        let (k, v) = (kv.key(), kv.value());
                        let kref = k.as_ref();
                        let vref = v.as_ref();
                        w.write_all(raw_byte_repr(&(kref.len() as u64)))?;
                        w.write_all(raw_byte_repr(&(vref.len() as u64)))?;
                        w.write_all(kref)?;
                        w.write_all(vref)?;
                    }
                }
                Ok(())
            }
            /// Serialize a set and write it to a provided buffer
            pub fn raw_serialize_set<W, K, V>(map: &Coremap<K, V>, w: &mut W)
                -> IoResult<()> where W: Write, K: Eq + Hash + AsRef<[u8]> {
                unsafe {
                    w.write_all(raw_byte_repr(&(map.len() as u64)))?;
                    for kv in map.iter() {
                        let key = kv.key().as_ref();
                        w.write_all(raw_byte_repr(&(key.len() as u64)))?;
                        w.write_all(key)?;
                    }
                }
                Ok(())
            }
            /// Generate a partition map for the given keyspace
            /// ```text
            /// [8B: EXTENT]([8B: LEN][?B: PARTITION ID][1B: Storage type][1B: Model type])*
            /// ```
            pub fn raw_serialize_partmap<W, U, Tbl,
                K>(w: &mut W, keyspace: &K) -> IoResult<()> where W: Write,
                U: Deref<Target = Tbl>, Tbl: FlushableTable,
                K: FlushableKeyspace<Tbl, U> {
                unsafe {
                    w.write_all(raw_byte_repr(&(keyspace.table_count() as
                                            u64)))?;
                    for table in keyspace.get_iter() {
                        w.write_all(raw_byte_repr(&(table.key().len() as u64)))?;
                        w.write_all(table.key())?;
                        w.write_all(raw_byte_repr(&table.storage_code()))?;
                        w.write_all(raw_byte_repr(&table.model_code()))?;
                    }
                }
                Ok(())
            }
            pub fn raw_serialize_list_map<W>(data: &Coremap<Data, LockedVec>,
                w: &mut W) -> IoResult<()> where W: Write {
                unsafe {
                    w.write_all(raw_byte_repr(&(data.len() as u64)))?;
                    '_1:
                        for key in data.iter() {
                        let k = key.key();
                        let vread = key.value().read();
                        let v: &Vec<Data> = &vread;
                        w.write_all(raw_byte_repr(&(k.len() as u64)))?;
                        w.write_all(k)?;
                        self::raw_serialize_nested_list(w, &v)?;
                    }
                }
                Ok(())
            }
            /// Serialize a `[[u8]]` (i.e a slice of slices)
            pub fn raw_serialize_nested_list<'a, W, T: 'a + ?Sized,
                U: 'a>(w: &mut W, inp: &'a T) -> IoResult<()> where
                T: AsRef<[U]>, U: AsRef<[u8]>, W: Write {
                let inp = inp.as_ref();
                unsafe {
                    w.write_all(raw_byte_repr(&(inp.len() as u64)))?;
                    for element in inp.iter() {
                        let element = element.as_ref();
                        w.write_all(raw_byte_repr(&(element.len() as u64)))?;
                        w.write_all(element)?;
                    }
                }
                Ok(())
            }
        }
        mod de {
            use super::iter::{RawSliceIter, RawSliceIterBorrowed};
            use super::{Array, Coremap, Data, Hash, HashSet};
            use crate::kvengine::LockedVec;
            use core::ptr;
            use parking_lot::RwLock;
            use std::collections::HashMap;
            pub trait DeserializeFrom {
                fn is_expected_len(clen: usize)
                -> bool;
                fn from_slice(slice: &[u8])
                -> Self;
            }
            pub trait DeserializeInto: Sized {
                fn new_empty()
                -> Self;
                fn from_slice(slice: &[u8])
                -> Option<Self>;
            }
            impl DeserializeInto for Coremap<Data, Data> {
                fn new_empty() -> Self { Coremap::new() }
                fn from_slice(slice: &[u8]) -> Option<Self> {
                    self::deserialize_map(slice)
                }
            }
            impl DeserializeInto for Coremap<Data, LockedVec> {
                fn new_empty() -> Self { Coremap::new() }
                fn from_slice(slice: &[u8]) -> Option<Self> {
                    self::deserialize_list_map(slice)
                }
            }
            impl<T, U> DeserializeInto for Coremap<T, U> where T: Hash + Eq +
                DeserializeFrom, U: DeserializeFrom {
                fn new_empty() -> Self { Coremap::new() }
                fn from_slice(slice: &[u8]) -> Option<Self> {
                    self::deserialize_map_ctype(slice)
                }
            }
            pub fn deserialize_into<T: DeserializeInto>(input: &[u8])
                -> Option<T> {
                T::from_slice(input)
            }
            impl<const N : usize> DeserializeFrom for Array<u8, N> {
                fn is_expected_len(clen: usize) -> bool { clen <= N }
                fn from_slice(slice: &[u8]) -> Self {
                    unsafe { Self::from_slice(slice) }
                }
            }
            impl<const N : usize> DeserializeFrom for [u8; N] {
                fn is_expected_len(clen: usize) -> bool { clen == N }
                fn from_slice(slice: &[u8]) -> Self {
                    slice.try_into().unwrap()
                }
            }
            pub fn deserialize_map_ctype<T, U>(data: &[u8])
                -> Option<Coremap<T, U>> where T: Eq + Hash + DeserializeFrom,
                U: DeserializeFrom {
                let mut rawiter = RawSliceIter::new(data);
                let len = rawiter.next_64bit_integer_to_usize()?;
                let map = Coremap::new();
                for _ in 0..len {
                    let (lenkey, lenval) =
                        rawiter.next_64bit_integer_pair_to_usize()?;
                    if !(T::is_expected_len(lenkey) &&
                                        U::is_expected_len(lenval)) {
                            return None;
                        }
                    let key =
                        T::from_slice(rawiter.next_borrowed_slice(lenkey)?);
                    let value =
                        U::from_slice(rawiter.next_borrowed_slice(lenval)?);
                    if !map.true_if_insert(key, value) { return None; }
                }
                Some(map)
            }
            /// Deserialize a set to a custom type
            pub fn deserialize_set_ctype<T>(data: &[u8]) -> Option<HashSet<T>>
                where T: DeserializeFrom + Eq + Hash {
                let mut rawiter = RawSliceIter::new(data);
                let len = rawiter.next_64bit_integer_to_usize()?;
                let mut set = HashSet::new();
                set.try_reserve(len).ok()?;
                for _ in 0..len {
                    let lenkey = rawiter.next_64bit_integer_to_usize()?;
                    if !T::is_expected_len(lenkey) { return None; }
                    let key =
                        T::from_slice(rawiter.next_borrowed_slice(lenkey)?);
                    if !set.insert(key) { return None; }
                }
                if rawiter.end_of_allocation() { Some(set) } else { None }
            }
            /// Deserializes a map-like set which has an 2x1B _bytemark_ for every entry
            pub fn deserialize_set_ctype_bytemark<T>(data: &[u8])
                -> Option<HashMap<T, (u8, u8)>> where T: DeserializeFrom +
                Eq + Hash {
                let mut rawiter = RawSliceIter::new(data);
                let len = rawiter.next_64bit_integer_to_usize()?;
                let mut set = HashMap::new();
                set.try_reserve(len).ok()?;
                for _ in 0..len {
                    let lenkey = rawiter.next_64bit_integer_to_usize()?;
                    if !T::is_expected_len(lenkey) { return None; }
                    let key =
                        T::from_slice(rawiter.next_borrowed_slice(lenkey)?);
                    let bytemark_a = rawiter.next_8bit_integer()?;
                    let bytemark_b = rawiter.next_8bit_integer()?;
                    if set.insert(key, (bytemark_a, bytemark_b)).is_some() {
                            return None;
                        }
                }
                if rawiter.end_of_allocation() { Some(set) } else { None }
            }
            /// Deserialize a file that contains a serialized map. This also returns the model code
            pub fn deserialize_map(data: &[u8])
                -> Option<Coremap<Data, Data>> {
                let mut rawiter = RawSliceIter::new(data);
                let len = rawiter.next_64bit_integer_to_usize()?;
                let hm = Coremap::try_with_capacity(len).ok()?;
                for _ in 0..len {
                    let (lenkey, lenval) =
                        rawiter.next_64bit_integer_pair_to_usize()?;
                    let key = rawiter.next_owned_data(lenkey)?;
                    let val = rawiter.next_owned_data(lenval)?;
                    hm.upsert(key, val);
                }
                if rawiter.end_of_allocation() { Some(hm) } else { None }
            }
            pub fn deserialize_list_map(bytes: &[u8])
                -> Option<Coremap<Data, LockedVec>> {
                let mut rawiter = RawSliceIter::new(bytes);
                let len = rawiter.next_64bit_integer_to_usize()?;
                let map = Coremap::try_with_capacity(len).ok()?;
                for _ in 0..len {
                    let keylen = rawiter.next_64bit_integer_to_usize()?;
                    let key = rawiter.next_owned_data(keylen)?;
                    let borrowed_iter = rawiter.get_borrowed_iter();
                    let list = self::deserialize_nested_list(borrowed_iter)?;
                    map.true_if_insert(key, RwLock::new(list));
                }
                if rawiter.end_of_allocation() { Some(map) } else { None }
            }
            /// Deserialize a nested list: `[EXTENT]([EL_EXT][EL])*`
            ///
            pub fn deserialize_nested_list(mut iter: RawSliceIterBorrowed<'_>)
                -> Option<Vec<Data>> {
                let list_payload_extent = iter.next_64bit_integer_to_usize()?;
                let mut list = Vec::new();
                list.try_reserve(list_payload_extent).ok()?;
                for _ in 0..list_payload_extent {
                    let list_element_payload_size =
                        iter.next_64bit_integer_to_usize()?;
                    let element =
                        iter.next_owned_data(list_element_payload_size)?;
                    list.push(element);
                }
                Some(list)
            }
            #[allow(clippy :: needless_return)]
            pub(super) unsafe fn transmute_len(start_ptr: *const u8)
                -> usize {

                #[cfg(target_endian = "little")]
                { { return self::transmute_len_le(start_ptr); } };
            }
            #[allow(clippy :: needless_return)]
            pub(super) unsafe fn transmute_len_le(start_ptr: *const u8)
                -> usize {

                #[cfg(target_endian = "little")]
                {
                    {

                        #[cfg(target_pointer_width = "64")]
                        { { return ptr::read_unaligned(start_ptr.cast()); } };
                        ;
                    }
                };
                ;
            }
            #[allow(clippy :: needless_return)]
            pub(super) unsafe fn transmute_len_be(start_ptr: *const u8)
                -> usize {
                ;

                #[cfg(target_endian = "little")]
                {
                    {

                        #[cfg(target_pointer_width = "64")]
                        {
                            {
                                let ret: usize = ptr::read_unaligned(start_ptr.cast());
                                return ret.swap_bytes();
                            }
                        };
                        ;
                    }
                };
            }
        }
    }
    pub mod unflush {
        use crate::{
            corestore::memstore::Memstore,
            storage::v1::error::StorageEngineResult,
        };
        pub fn read_full() -> StorageEngineResult<Memstore> {
            super::v1::unflush::read_full()
        }
    }
}
const PID_FILE_PATH: &str = ".sky_pid";
#[cfg(all(not(target_env = "msvc"), not(miri)))]
use jemallocator::Jemalloc;
#[cfg(all(not(target_env = "msvc"), not(miri)))]
/// Jemallocator - this is the default memory allocator for platforms other than msvc
static GLOBAL: Jemalloc = Jemalloc;
const _: () =
    {
        #[rustc_std_internal_symbol]
        unsafe fn __rg_alloc(arg0: usize, arg1: usize) -> *mut u8 {
            ::core::alloc::GlobalAlloc::alloc(&GLOBAL,
                    ::core::alloc::Layout::from_size_align_unchecked(arg0,
                        arg1)) as *mut u8
        }
        #[rustc_std_internal_symbol]
        unsafe fn __rg_dealloc(arg0: *mut u8, arg1: usize, arg2: usize)
            -> () {
            ::core::alloc::GlobalAlloc::dealloc(&GLOBAL, arg0 as *mut u8,
                ::core::alloc::Layout::from_size_align_unchecked(arg1, arg2))
        }
        #[rustc_std_internal_symbol]
        unsafe fn __rg_realloc(arg0: *mut u8, arg1: usize, arg2: usize,
            arg3: usize) -> *mut u8 {
            ::core::alloc::GlobalAlloc::realloc(&GLOBAL, arg0 as *mut u8,
                    ::core::alloc::Layout::from_size_align_unchecked(arg1,
                        arg2), arg3) as *mut u8
        }
        #[rustc_std_internal_symbol]
        unsafe fn __rg_alloc_zeroed(arg0: usize, arg1: usize) -> *mut u8 {
            ::core::alloc::GlobalAlloc::alloc_zeroed(&GLOBAL,
                    ::core::alloc::Layout::from_size_align_unchecked(arg0,
                        arg1)) as *mut u8
        }
    };
/// The terminal art for `!noart` configurations
const TEXT: &str =
    "
███████ ██   ██ ██    ██ ████████  █████  ██████  ██      ███████
██      ██  ██   ██  ██     ██    ██   ██ ██   ██ ██      ██
███████ █████     ████      ██    ███████ ██████  ██      █████
     ██ ██  ██     ██       ██    ██   ██ ██   ██ ██      ██
███████ ██   ██    ██       ██    ██   ██ ██████  ███████ ███████
";
type IoResult<T> = std::io::Result<T>;
fn main() {
    Builder::new().parse_filters(&env::var("SKY_LOG").unwrap_or_else(|_|
                        "info".to_owned())).init();
    let runtime =
        tokio::runtime::Builder::new_multi_thread().thread_name("server").enable_all().build().unwrap();
    let (cfg, restore_file) = check_args_and_get_cfg();
    let pid_file = run_pre_startup_tasks();
    let db =
        runtime.block_on(async move
                { arbiter::run(cfg, restore_file).await });
    drop(runtime);
    let db =
        match db {
            Ok(d) => d,
            Err(e) => {
                {
                    let lvl = ::log::Level::Error;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&[""],
                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                &("skyd", "skyd", "server/src/main.rs", 114u32),
                                ::log::__private_api::Option::None);
                        }
                };
                services::pre_shutdown_cleanup(pid_file, None);
                process::exit(1);
            }
        };
    {
        let lvl = ::log::Level::Info;
        if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level() {
                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Stopped accepting incoming connections"],
                        &[]), lvl, &("skyd", "skyd", "server/src/main.rs", 119u32),
                    ::log::__private_api::Option::None);
            }
    };
    arbiter::finalize_shutdown(db, pid_file);
    {

        #[cfg(debug_assertions)]
        std::fs::remove_file(PID_FILE_PATH).unwrap();
    }
}
/// This function checks the command line arguments and either returns a config object
/// or prints an error to `stderr` and terminates the server
fn check_args_and_get_cfg() -> (ConfigurationSet, Option<String>) {
    match config::get_config() {
        Ok(cfg) => {
            if cfg.is_artful() {
                    {
                        let lvl = ::log::Level::Info;
                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                    lvl <= ::log::max_level() {
                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Skytable v",
                                                    " | ", "\n"],
                                        &[::core::fmt::ArgumentV1::new_display(&VERSION),
                                                    ::core::fmt::ArgumentV1::new_display(&URL),
                                                    ::core::fmt::ArgumentV1::new_display(&TEXT)]), lvl,
                                    &("skyd", "skyd", "server/src/main.rs", 134u32),
                                    ::log::__private_api::Option::None);
                            }
                    };
                } else {
                   {
                       let lvl = ::log::Level::Info;
                       if lvl <= ::log::STATIC_MAX_LEVEL &&
                                   lvl <= ::log::max_level() {
                               ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Skytable v",
                                                   " | "],
                                       &[::core::fmt::ArgumentV1::new_display(&VERSION),
                                                   ::core::fmt::ArgumentV1::new_display(&URL)]), lvl,
                                   &("skyd", "skyd", "server/src/main.rs", 136u32),
                                   ::log::__private_api::Option::None);
                           }
                   };
               }
            if cfg.is_custom() {
                    {
                        let lvl = ::log::Level::Info;
                        if lvl <= ::log::STATIC_MAX_LEVEL &&
                                    lvl <= ::log::max_level() {
                                ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Using settings from supplied configuration"],
                                        &[]), lvl, &("skyd", "skyd", "server/src/main.rs", 139u32),
                                    ::log::__private_api::Option::None);
                            }
                    };
                } else {
                   {
                       let lvl = ::log::Level::Warn;
                       if lvl <= ::log::STATIC_MAX_LEVEL &&
                                   lvl <= ::log::max_level() {
                               ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["No configuration file supplied. Using default settings"],
                                       &[]), lvl, &("skyd", "skyd", "server/src/main.rs", 141u32),
                                   ::log::__private_api::Option::None);
                           }
                   };
               }
            cfg.print_warnings();
            cfg.finish()
        }
        Err(e) => {
            {
                let lvl = ::log::Level::Error;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level()
                        {
                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&[""],
                                &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                            &("skyd", "skyd", "server/src/main.rs", 148u32),
                            ::log::__private_api::Option::None);
                    }
            };
            crate::exit_error();
        }
    }
}
/// On startup, we attempt to check if a `.sky_pid` file exists. If it does, then
/// this file will contain the kernel/operating system assigned process ID of the
/// skyd process. We will attempt to read that and log an error complaining that
/// the directory is in active use by another process. If the file doesn't then
/// we're free to create our own file and write our own PID to it. Any subsequent
/// processes will detect this and this helps us prevent two processes from writing
/// to the same directory which can cause potentially undefined behavior.
///
fn run_pre_startup_tasks() -> FileLock {
    let mut file =
        match FileLock::lock(PID_FILE_PATH) {
            Ok(fle) => fle,
            Err(e) => {
                {
                    let lvl = ::log::Level::Error;
                    if lvl <= ::log::STATIC_MAX_LEVEL &&
                                lvl <= ::log::max_level() {
                            ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Startup failure: Failed to lock pid file: "],
                                    &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                                &("skyd", "skyd", "server/src/main.rs", 166u32),
                                ::log::__private_api::Option::None);
                        }
                };
                crate::exit_error();
            }
        };
    if let Err(e) = file.write(process::id().to_string().as_bytes()) {
            {
                let lvl = ::log::Level::Error;
                if lvl <= ::log::STATIC_MAX_LEVEL && lvl <= ::log::max_level()
                        {
                        ::log::__private_api_log(::core::fmt::Arguments::new_v1(&["Startup failure: Failed to write to pid file: "],
                                &[::core::fmt::ArgumentV1::new_display(&e)]), lvl,
                            &("skyd", "skyd", "server/src/main.rs", 171u32),
                            ::log::__private_api::Option::None);
                    }
            };
            crate::exit_error();
        }
    file
}
