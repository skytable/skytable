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
    super::{
        atm::{upin, Atomic, Guard, Owned, Shared, ORD_ACQ, ORD_REL, ORD_SEQ},
        Backoff,
    },
    core::{
        marker::PhantomData,
        mem,
        ops::Deref,
        ptr,
        sync::atomic::{AtomicBool, AtomicPtr},
    },
    parking_lot::{Mutex, MutexGuard},
};

/// A lazily intialized, or _call by need_ value
#[derive(Debug)]
pub struct Lazy<T, F> {
    /// the value (null at first)
    value: AtomicPtr<T>,
    /// the function that will init the value
    init_func: F,
    /// is some thread trying to initialize the value
    init_state: AtomicBool,
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

impl<T, F> Deref for Lazy<T, F>
where
    F: Fn() -> T,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        let value_ptr = self.value.load(ORD_ACQ);
        if !value_ptr.is_null() {
            // the value has already been initialized, return
            unsafe {
                // UNSAFE(@ohsayan): We've just asserted that the value is not null
                return &*value_ptr;
            }
        }
        // it's null, so it's useless

        // hold on until someone is trying to init
        let backoff = Backoff::new();
        while self
            .init_state
            .compare_exchange(false, true, ORD_SEQ, ORD_SEQ)
            .is_err()
        {
            // wait until the other thread finishes
            backoff.snooze();
        }
        /*
         see the value before the last store. while we were one the loop,
         some other thread could have initialized it already
        */
        let value_ptr = self.value.load(ORD_ACQ);
        if !value_ptr.is_null() {
            // no more init, someone initialized it already
            assert!(self.init_state.swap(false, ORD_SEQ));
            unsafe {
                // UNSAFE(@ohsayan): We've already loaded the value checked
                // that it isn't null
                &*value_ptr
            }
        } else {
            // so no one cared to initialize the value in between
            // fine, we'll init it
            let value = (self.init_func)();
            let value_ptr = Box::into_raw(Box::new(value));
            // now swap out the older value and check it for sanity
            assert!(self.value.swap(value_ptr, ORD_SEQ).is_null());
            // set trying to init flag to false
            assert!(self.init_state.swap(false, ORD_SEQ));
            unsafe {
                // UNSAFE(@ohsayan): We just initialized the value ourselves
                // so it is not null!
                &*value_ptr
            }
        }
    }
}

impl<T, F> Drop for Lazy<T, F> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            // this needs drop
            let value_ptr = self.value.load(ORD_ACQ);
            if !value_ptr.is_null() {
                unsafe {
                    // UNSAFE(@ohsayan): We've just checked if the value is null or not
                    mem::drop(Box::from_raw(value_ptr))
                }
            }
        }
    }
}

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
