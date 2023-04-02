/*
 * Created on Sat Jan 21 2023
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
    super::atm::{upin, Atomic, Guard, Owned, Shared, ORD_REL},
    core::{marker::PhantomData, ops::Deref},
    parking_lot::{Mutex, MutexGuard},
};

/// A [`TMCell`] provides atomic reads and serialized writes; the `static` is a CB hack
#[derive(Debug)]
pub struct TMCell<T: 'static> {
    a: Atomic<T>,
    g: Mutex<()>,
}

impl<T: 'static> TMCell<T> {
    pub fn new(v: T) -> Self {
        Self {
            a: Atomic::new_alloc(v),
            g: Mutex::new(()),
        }
    }
    pub fn begin_write_txn<'a, 'g>(&'a self, g: &'g Guard) -> TMCellWriteTxn<'a, 'g, T> {
        let wg = self.g.lock();
        let snapshot = self.a.ld_acq(g);
        let data: &'g T = unsafe {
            // UNSAFE(@ohsayan): first, non-null (TMCell is never null). second, the guard
            snapshot.deref()
        };
        TMCellWriteTxn::new(data, &self.a, wg)
    }
    pub fn begin_read_txn<'a, 'g>(&'a self, g: &'g Guard) -> TMCellReadTxn<'a, 'g, T> {
        let snapshot = self.a.ld_acq(g);
        let data: &'g T = unsafe {
            // UNSAFE(@ohsayan): non-null and the guard
            snapshot.deref()
        };
        TMCellReadTxn::new(data)
    }
}

impl<T> Drop for TMCell<T> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): Sole owner with mutable access
            let g = upin();
            let shptr = self.a.ld_rlx(g);
            g.defer_destroy(shptr);
        }
    }
}

unsafe impl<T: Send> Send for TMCell<T> {}
unsafe impl<T: Sync> Sync for TMCell<T> {}

#[derive(Debug)]
pub struct TMCellReadTxn<'a, 'g, T: 'static> {
    d: &'g T,
    _m: PhantomData<&'a TMCell<T>>,
}

impl<'a, 'g, T> TMCellReadTxn<'a, 'g, T> {
    #[inline(always)]
    pub fn new(d: &'g T) -> Self {
        Self { d, _m: PhantomData }
    }
    #[inline(always)]
    pub fn read(&self) -> &'g T {
        self.d
    }
}

impl<'a, 'g, T: Clone> TMCellReadTxn<'a, 'g, T> {
    #[inline(always)]
    pub fn read_copied(&self) -> T {
        self.read().clone()
    }
}

impl<'a, 'g, T: Copy> TMCellReadTxn<'a, 'g, T> {
    fn read_copy(&self) -> T {
        *self.d
    }
}

impl<'a, 'g, T> Deref for TMCellReadTxn<'a, 'g, T> {
    type Target = T;
    fn deref(&self) -> &'g Self::Target {
        self.d
    }
}

unsafe impl<'a, 'g, T: Send> Send for TMCellReadTxn<'a, 'g, T> {}
unsafe impl<'a, 'g, T: Sync> Sync for TMCellReadTxn<'a, 'g, T> {}

#[derive(Debug)]
pub struct TMCellWriteTxn<'a, 'g, T: 'static> {
    d: &'g T,
    a: &'a Atomic<T>,
    g: MutexGuard<'a, ()>,
}

impl<'a, 'g, T> TMCellWriteTxn<'a, 'g, T> {
    #[inline(always)]
    pub fn new(d: &'g T, a: &'a Atomic<T>, g: MutexGuard<'a, ()>) -> Self {
        Self { d, a, g }
    }
    pub fn publish_commit(self, new: T, g: &'g Guard) {
        self._commit(new, g, |p| {
            unsafe {
                // UNSAFE(@ohsayan): Unlinked
                g.defer_destroy(p);
            }
        })
    }
    fn _commit<F, R>(self, new: T, g: &'g Guard, f: F) -> R
    where
        F: FnOnce(Shared<T>) -> R,
    {
        let new = Owned::new(new);
        let r = self.a.swap(new, ORD_REL, g);
        f(r)
    }
    #[inline(always)]
    pub fn read(&self) -> &'g T {
        self.d
    }
}

impl<'a, 'g, T: Clone> TMCellWriteTxn<'a, 'g, T> {
    #[inline(always)]
    pub fn read_copied(&self) -> T {
        self.read().clone()
    }
}

impl<'a, 'g, T: Copy> TMCellWriteTxn<'a, 'g, T> {
    fn read_copy(&self) -> T {
        *self.d
    }
}

impl<'a, 'g, T> Deref for TMCellWriteTxn<'a, 'g, T> {
    type Target = T;
    fn deref(&self) -> &'g Self::Target {
        self.d
    }
}

unsafe impl<'a, 'g, T: Sync> Sync for TMCellWriteTxn<'a, 'g, T> {}
