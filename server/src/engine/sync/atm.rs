/*
 * Created on Fri Jan 20 2023
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

use core::{fmt, mem, ops::Deref, sync::atomic::Ordering};
use crossbeam_epoch::{Atomic as CBAtomic, CompareExchangeError, Pointable, Pointer};
// re-export here because we have some future plans ;) (@ohsayan)
pub use crossbeam_epoch::{pin as cpin, unprotected as upin, Guard, Owned, Shared};

pub(super) const ORD_RLX: Ordering = Ordering::Relaxed;
pub(super) const ORD_ACQ: Ordering = Ordering::Acquire;
pub(super) const ORD_REL: Ordering = Ordering::Release;
pub(super) const ORD_ACR: Ordering = Ordering::AcqRel;

type CxResult<'g, T, P> = Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>;

pub(super) const fn ensure_flag_align<T>(fsize: usize) {
    debug_assert!(mem::align_of::<T>().trailing_zeros() as usize >= fsize);
}

pub struct Atomic<T> {
    a: CBAtomic<T>,
}

// the derive is stupid, it will enforce a debug constraint on T
impl<T> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.a)
    }
}

impl<T: Pointable> Atomic<T> {
    // the compile time address size check ensures "first class" sanity
    const _ENSURE_FLAG_STATIC_CHECK: () = ensure_flag_align::<T>(0);
    /// Instantiates a new atomic
    ///
    /// **This will allocate**
    pub fn new_alloc(t: T) -> Self {
        let _ = Self::_ENSURE_FLAG_STATIC_CHECK;
        Self {
            a: CBAtomic::new(t),
        }
    }
    #[inline(always)]
    pub(super) const fn null() -> Self {
        Self {
            a: CBAtomic::null(),
        }
    }
    #[inline(always)]
    pub(super) fn cx<'g, P>(
        &self,
        o: Shared<'g, T>,
        n: P,
        s: Ordering,
        f: Ordering,
        g: &'g Guard,
    ) -> CxResult<'g, T, P>
    where
        P: Pointer<T>,
    {
        self.a.compare_exchange(o, n, s, f, g)
    }
    #[inline(always)]
    pub(super) fn cx_weak<'g, P>(
        &self,
        o: Shared<'g, T>,
        n: P,
        s: Ordering,
        f: Ordering,
        g: &'g Guard,
    ) -> CxResult<'g, T, P>
    where
        P: Pointer<T>,
    {
        self.a.compare_exchange_weak(o, n, s, f, g)
    }
    #[inline(always)]
    pub(super) fn cx_rel<'g, P>(&self, o: Shared<'g, T>, n: P, g: &'g Guard) -> CxResult<'g, T, P>
    where
        P: Pointer<T>,
    {
        self.cx(o, n, ORD_REL, ORD_RLX, g)
    }
    #[inline(always)]
    pub(super) fn ld<'g>(&self, o: Ordering, g: &'g Guard) -> Shared<'g, T> {
        self.a.load(o, g)
    }
    #[inline(always)]
    pub(super) fn ld_acq<'g>(&self, g: &'g Guard) -> Shared<'g, T> {
        self.ld(ORD_ACQ, g)
    }
    #[inline(always)]
    pub(crate) fn ld_rlx<'g>(&self, g: &'g Guard) -> Shared<'g, T> {
        self.ld(ORD_RLX, g)
    }
}

impl<T, A> From<A> for Atomic<T>
where
    A: Into<CBAtomic<T>>,
{
    fn from(t: A) -> Self {
        let _ = Self::_ENSURE_FLAG_STATIC_CHECK;
        Self { a: Into::into(t) }
    }
}

impl<T> Deref for Atomic<T> {
    type Target = CBAtomic<T>;
    fn deref(&self) -> &Self::Target {
        &self.a
    }
}
