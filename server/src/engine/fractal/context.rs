/*
 * Created on Sun Oct 01 2023
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

#![allow(dead_code)]

use core::fmt;

/// The current engine context
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Subsystem {
    Init,     // the init system
    Storage,  // the storage engine
    Database, // the database engine
    Network,  // the network layer
}

impl Subsystem {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init system",
            Self::Storage => "storage error",
            Self::Database => "engine error",
            Self::Network => "network error",
        }
    }
}

/*
    diagnostics
*/

#[derive(Clone)]
/// A dmsg
pub enum Dmsg {
    A(Box<str>),
    B(&'static str),
}

impl PartialEq for Dmsg {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl AsRef<str> for Dmsg {
    fn as_ref(&self) -> &str {
        match self {
            Self::A(a) => a,
            Self::B(b) => b,
        }
    }
}

direct_from! {
    Dmsg => {
        String as A,
        Box<str> as A,
        &'static str as B,
    }
}

impl fmt::Display for Dmsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <str as fmt::Display>::fmt(self.as_ref(), f)
    }
}

impl fmt::Debug for Dmsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <str as fmt::Debug>::fmt(self.as_ref(), f)
    }
}

/*
    context
*/

macro_rules! exported {
    ($($vis:vis impl $ty:ty { $($(#[$attr:meta])* $fnvis:vis fn $fn:ident($($fnarg:ident: $fnarg_ty:ty),*) $(-> $fnret:ty)? $fnblock:block)*})*) => {
        $(impl $ty { $( $(#[$attr])* $fnvis fn $fn($($fnarg: $fnarg_ty),*) $( -> $fnret)? $fnblock )*}
        $($(#[$attr])* $vis fn $fn($($fnarg: $fnarg_ty),*) $( -> $fnret)? { <$ty>::$fn($($fnarg),*) })*)*
    }
}

struct LocalContext {
    origin: Option<Subsystem>,
    dmsg: Option<Dmsg>,
}

fn if_test(f: impl FnOnce()) {
    if cfg!(test) {
        f()
    }
}

/// A copy of the local context (might be either popped or cloned)
#[derive(Debug, PartialEq, Clone)]
pub struct LocalCtxInstance {
    origin: Option<Subsystem>,
    dmsg: Option<Dmsg>,
}

impl LocalCtxInstance {
    fn new(origin: Option<Subsystem>, dmsg: Option<Dmsg>) -> Self {
        Self { origin, dmsg }
    }
    pub fn origin(&self) -> Option<Subsystem> {
        self.origin
    }
    pub fn dmsg(&self) -> Option<&Dmsg> {
        self.dmsg.as_ref()
    }
}

impl From<LocalContext> for LocalCtxInstance {
    fn from(LocalContext { origin, dmsg }: LocalContext) -> Self {
        Self { origin, dmsg }
    }
}

exported! {
    pub impl LocalContext {
        // all
        fn set(origin: Subsystem, dmsg: impl Into<Dmsg>) { Self::_ctx(|ctx| { ctx.origin = Some(origin); ctx.dmsg = Some(dmsg.into()) }) }
        fn test_set(origin: Subsystem, dmsg: impl Into<Dmsg>) { if_test(|| Self::set(origin, dmsg)) }
        // dmsg
        /// set a local dmsg
        fn set_dmsg(dmsg: impl Into<Dmsg>) { Self::_ctx(|ctx| ctx.dmsg = Some(dmsg.into())) }
        /// (only in test) set a local dmsg
        fn test_set_dmsg(dmsg: impl Into<Dmsg>) { if_test(|| Self::set_dmsg(dmsg)) }
        /// Set a local dmsg iff not already set
        fn set_dmsg_if_unset(dmsg: impl Into<Dmsg>) { Self::_ctx(|ctx| { ctx.dmsg.get_or_insert(dmsg.into()); }) }
        /// (only in test) set a local dmsg iff not already set
        fn test_set_dmsg_if_unset(dmsg: impl Into<Dmsg>) { if_test(|| Self::set_dmsg_if_unset(dmsg)) }
        // origin
        /// set a local origin
        fn set_origin(origin: Subsystem) { Self::_ctx(|ctx| ctx.origin = Some(origin)) }
        /// (only in test) set a local origin
        fn test_set_origin(origin: Subsystem) { if_test(|| Self::set_origin(origin)) }
        /// set origin iff unset
        fn set_origin_if_unset(origin: Subsystem) { Self::_ctx(|ctx| { ctx.origin.get_or_insert(origin); }) }
        /// (only in test) set a local origin iff not already set
        fn test_set_origin_if_unset(origin: Subsystem) { if_test(|| Self::set_origin_if_unset(origin)) }
    }
    pub(super) impl LocalContext {
        // alter context
        /// pop the origin from the local context
        fn pop_origin() -> Option<Subsystem> { Self::_ctx(|ctx| ctx.origin.take()) }
        /// pop the dmsg from the local context
        fn pop_dmsg() -> Option<Dmsg> { Self::_ctx(|ctx| ctx.dmsg.take()) }
        /// pop the entire context
        fn pop() -> LocalCtxInstance { Self::_ctx(|ctx| core::mem::replace(ctx, LocalContext::null()).into()) }
        /// get the origin
        fn get_origin() -> Option<Subsystem> { Self::_ctx(|ctx| ctx.origin.clone()) }
        /// get the dmsg
        fn get_dmsg() -> Option<Dmsg> { Self::_ctx(|ctx| ctx.dmsg.clone()) }
        /// get a clone of the local context
        fn cloned() -> LocalCtxInstance { Self::_ctx(|ctx| LocalCtxInstance::new(ctx.origin.clone(), ctx.dmsg.clone())) }
    }
}

impl LocalContext {
    fn _new(origin: Option<Subsystem>, dmsg: Option<Dmsg>) -> Self {
        Self { origin, dmsg }
    }
    fn null() -> Self {
        Self::_new(None, None)
    }
    fn _ctx<T>(f: impl FnOnce(&mut Self) -> T) -> T {
        local! { static CTX: LocalContext = LocalContext::null(); }
        local_mut!(CTX, f)
    }
}
