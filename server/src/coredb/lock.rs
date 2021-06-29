/*
 * Created on Tue Jun 29 2021
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

//! # Locks
//!
//! In several scenarios, we may find `std`'s or other crates' implementations of synchronization
//! primitives to be either _too sophisticated_ or _not what we want_. For these cases, we use
//! the primitives that are defined here
//!

use core::hint::spin_loop as let_the_cpu_relax;
use std::cell::UnsafeCell;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

const ORD_ACQUIRE: Ordering = Ordering::Acquire;
const ORD_RELEASE: Ordering = Ordering::Release;

/// An extremely simple lock without the extra fuss: just the raw data and an atomic bool
#[derive(Debug)]
pub struct QuickLock<T> {
    rawdata: UnsafeCell<T>,
    lock_state: AtomicBool,
}

unsafe impl<T: Send> Sync for QuickLock<T> {}
unsafe impl<T: Send> Send for QuickLock<T> {}

/// A lock guard created by [`QuickLock`]
pub struct QLGuard<'a, T> {
    lck: &'a QuickLock<T>,
}

impl<'a, T> QLGuard<'a, T> {
    const fn init(lck: &'a QuickLock<T>) -> Self {
        Self { lck }
    }
}

/*
 * Acq/Rel semantics don't emit fences on Intel platforms, but on weakly ordered targets
 * things may look different.
*/

impl<T> QuickLock<T> {
    pub const fn new(rawdata: T) -> Self {
        Self {
            lock_state: AtomicBool::new(false),
            rawdata: UnsafeCell::new(rawdata),
        }
    }
    /// Try to acquire a lock
    #[allow(dead_code)] // TODO(@ohsayan): Keep or remove this lint
    pub fn try_lock(&self) -> Option<QLGuard<'_, T>> {
        let ret = self
            .lock_state
            .compare_exchange(false, true, ORD_ACQUIRE, ORD_ACQUIRE);
        if ret.is_ok() {
            Some(QLGuard::init(self))
        } else {
            None
        }
    }
    /// Check if already locked
    pub fn is_locked(&self) -> bool {
        self.lock_state.load(ORD_ACQUIRE)
    }
    /// Enter a _busy loop_ waiting to get an unlock. Behold, this is blocking!
    pub fn lock(&self) -> QLGuard<'_, T> {
        loop {
            let ret = self.lock_state.compare_exchange_weak(
                false,
                true,
                Ordering::SeqCst,
                Ordering::Relaxed,
            );
            match ret {
                Ok(_) => break QLGuard::init(self),
                Err(is_locked) => {
                    if !is_locked {
                        break QLGuard::init(self);
                    }
                }
            }
            let_the_cpu_relax()
        }
    }
}

impl<'a, T> Drop for QLGuard<'a, T> {
    fn drop(&mut self) {
        #[cfg(test)]
        assert!(self.lck.lock_state.swap(false, ORD_RELEASE));
        #[cfg(not(test))]
        let _ = self.lck.lock_state.swap(false, ORD_RELEASE);
    }
}

impl<'a, T> Deref for QLGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe {
            // UNSAFE(@ohsayan): Who doesn't like raw pointers anyway? (rustc: sigh)
            &*self.lck.rawdata.get()
        }
    }
}

impl<'a, T> DerefMut for QLGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe {
            // UNSAFE(@ohsayan): Who doesn't like raw pointers anyway? (rustc: sigh)
            &mut *self.lck.rawdata.get()
        }
    }
}

#[test]
fn test_lock() {
    let lck = QuickLock::new(100);
    assert!(!lck.is_locked());
}

#[test]
fn test_already_locked() {
    let lck = QuickLock::new(200);
    let _our_lock = lck.lock();
    assert!(lck.try_lock().is_none());
}

#[cfg(test)]
use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

#[cfg(test)]
fn panic_timeout<T, F>(dur: Duration, f: F) -> T
where
    T: Send + 'static,
    F: (FnOnce() -> T) + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<()>();
    let handle = thread::spawn(move || {
        let val = f();
        tx.send(()).unwrap();
        val
    });
    match rx.recv_timeout(dur) {
        Ok(_) => handle.join().expect("Thread paniced"),
        Err(_) => panic!("Thread passed timeout"),
    }
}

#[test]
#[should_panic]
fn test_two_lock_timeout() {
    let lck = Arc::new(QuickLock::new(1u8));
    let child_lock = lck.clone();
    let _lock = lck.lock();
    panic_timeout(Duration::from_micros(500), move || {
        let lck = child_lock;
        let _ret = lck.lock();
    });
}
