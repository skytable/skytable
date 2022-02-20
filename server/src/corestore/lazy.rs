/*
 * Created on Sat Jul 03 2021
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

use super::backoff::Backoff;
use core::mem;
use core::ops::Deref;
use core::ptr;
use core::sync::atomic::AtomicBool;
use core::sync::atomic::AtomicPtr;
use core::sync::atomic::AtomicU8;
use core::sync::atomic::Ordering;

const ORD_ACQ: Ordering = Ordering::Acquire;
const ORD_SEQ: Ordering = Ordering::SeqCst;
const ORD_REL: Ordering = Ordering::Release;
const ORD_RLX: Ordering = Ordering::Relaxed;

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

/*
 Note on memory leaks:
 Suddenly found a possible leak with a static created with the Lazy type? Well, we'll have to
 ignore it. That's because destructors aren't called on statics. A thunk leak. So what's to be
 done here? Well, the best we can do is implement a destructor but it is never guranteed that
 it will be called when used in global scope
*/

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

cfg_test!(
    use crate::corestore::Data;
    use crate::corestore::lazy;
    use std::collections::HashMap;
    use std::thread;

    #[allow(clippy::type_complexity)]
    static LAZY_VALUE: lazy::Lazy<HashMap<Data, Data>, fn() -> HashMap<Data, Data>> = lazy::Lazy::new(|| {
        #[allow(clippy::mutable_key_type)]
        let mut ht = HashMap::new();
        ht.insert("sayan".into(), "is doing something".into());
        ht
    });

    #[test]
    fn test_lazy() {
        assert_eq!(
            LAZY_VALUE.get("sayan".as_bytes()).unwrap().clone(),
            Data::from("is doing something")
        );
    }

    #[test]
    fn test_two_threads_trying_to_get_at_once() {
        let (t1, t2) = (
            thread::spawn(|| {
                assert_eq!(
                    LAZY_VALUE.get("sayan".as_bytes()).unwrap().clone(),
                    Data::from("is doing something")
                );}),
            thread::spawn(|| {
                assert_eq!(
                    LAZY_VALUE.get("sayan".as_bytes()).unwrap().clone(),
                    Data::from("is doing something")
                );
            })
        );
        {
            t1.join().unwrap();
            t2.join().unwrap();
        }
    }

    struct WeirdTestStruct(u8);
    impl Drop for WeirdTestStruct {
        fn drop(&mut self) {
            panic!("PANIC ON DROP! THIS IS OKAY!");
        }
    }
    #[test]
    #[should_panic]
    fn test_drop() {
        // this is only when the lazy is initialized in local scope and not global scope
        let x: Lazy<WeirdTestStruct, fn() -> WeirdTestStruct> = Lazy::new(|| {
            WeirdTestStruct(0)
        });
        // just do an useless deref to make the pointer non null
        let _deref = &*x;
        drop(x); // we should panic right here
    }
    #[test]
    fn test_no_drop_null() {
        let x: Lazy<WeirdTestStruct, fn() -> WeirdTestStruct> = Lazy::new(|| {
            WeirdTestStruct(0)
        });
        drop(x); // no panic because it is null
    }
);

/// A "cell" that can be initialized once
pub struct Once<T> {
    value: AtomicPtr<T>,
    state: AtomicU8,
}

impl<T> Once<T> {
    const BIT_UNSET: u8 = 0b0000_0000;
    const BIT_WAIT: u8 = 0b0000_0001;
    const BIT_SET: u8 = 0b0000_0011;
    pub const fn new() -> Self {
        Self {
            value: AtomicPtr::new(ptr::null_mut()),
            state: AtomicU8::new(Self::BIT_UNSET),
        }
    }
    pub fn with_value(val: T) -> Self {
        Self {
            value: AtomicPtr::new(Box::into_raw(Box::new(val))),
            state: AtomicU8::new(Self::BIT_SET),
        }
    }
    pub fn get(&self) -> Option<&T> {
        // synchronizes with the store for BIT_SET; syncs with store for value ptr
        if self.state.load(ORD_ACQ) == Self::BIT_SET {
            unsafe { Some(&*self.value.load(ORD_ACQ)) }
        } else {
            None
        }
    }
    pub fn set(&self, val: T) -> bool {
        // synchronizes with the store for state
        let snapshot = self.state.load(ORD_ACQ);
        if snapshot == Self::BIT_UNSET {
            // let's try to init this
            let r = self.state.compare_exchange(
                snapshot,
                Self::BIT_WAIT,
                // we must use release ordering to sync with the acq
                ORD_REL,
                // on failure simply use relaxed because we don't use the value anyways
                // -- so why bother stressing out the processor?
                ORD_RLX,
            );
            if r.is_ok() {
                // proceed to init
                let v = Box::into_raw(Box::new(val));
                // sync with the load for get()
                self.value.store(v, ORD_REL);
                // release to sync with get()'s Acquire on ptr
                assert_eq!(self.state.swap(Self::BIT_SET, ORD_REL), Self::BIT_WAIT);
                true
            } else {
                // someone else initialized it already
                false
            }
        } else {
            false
        }
    }
}

impl<T> Drop for Once<T> {
    fn drop(&mut self) {
        match self.state.load(ORD_ACQ) {
            Self::BIT_UNSET => {}
            Self::BIT_SET => {
                // so we're set, let's destroy the value
                let ptr = self.value.load(ORD_ACQ);
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
            _ => {
                let backoff = Backoff::new();
                // someone is trying to init. wait for them to finish
                while self.state.load(ORD_ACQ) != Self::BIT_SET {
                    backoff.snooze();
                }
                // they're done with init, so we can destroy it
                unsafe {
                    drop(Box::from_raw(self.value.load(ORD_ACQ)));
                }
            }
        }
    }
}

#[test]
fn once_get_none() {
    let once: Once<u8> = Once::new();
    assert_eq!(once.get(), None);
}

cfg_test! {
    use std::sync::Arc;
    use std::time::Duration;
}

#[test]
fn once_set_get_some() {
    let once: Arc<Once<u8>> = Arc::new(Once::new());
    let t1 = once.clone();
    let t2 = once.clone();
    let hdl1 = thread::spawn(move || {
        assert!(t1.set(10));
    });
    thread::sleep(Duration::from_secs(3));
    let hdl2 = thread::spawn(move || {
        assert_eq!(*t2.get().unwrap(), 10);
    });
    assert_eq!(*once.get().unwrap(), 10);
    hdl1.join().unwrap();
    hdl2.join().unwrap();
}
