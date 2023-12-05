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
        atm::{ORD_ACQ, ORD_SEQ},
        Backoff,
    },
    core::{
        mem,
        ops::Deref,
        ptr,
        sync::atomic::{AtomicBool, AtomicPtr},
    },
};

/// A lazily intialized, or _call by need_ value
#[derive(Debug)]
pub struct Lazy<T, F = fn() -> T> {
    /// the value (null at first)
    value: AtomicPtr<T>,
    /// the function that will init the value
    init_func: F,
    /// is some thread trying to initialize the value
    init_state: AtomicBool,
}

impl<T: Default> Default for Lazy<T> {
    fn default() -> Self {
        Self::new(T::default)
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
