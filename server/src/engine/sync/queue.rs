/*
 * Created on Wed Aug 30 2023
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

#[cfg(test)]
use crossbeam_epoch::pin;
use {
    super::atm::Atomic,
    crate::engine::mem::CachePadded,
    crossbeam_epoch::{unprotected, Guard, Owned, Shared},
    std::{mem::MaybeUninit, sync::atomic::Ordering},
};

#[derive(Debug)]
struct QNode<T> {
    data: MaybeUninit<T>,
    next: Atomic<Self>,
}

impl<T> QNode<T> {
    fn new(data: MaybeUninit<T>, next: Atomic<Self>) -> Self {
        Self { data, next }
    }
    fn null() -> Self {
        Self::new(MaybeUninit::uninit(), Atomic::null())
    }
    fn new_data(val: T) -> Self {
        Self::new(MaybeUninit::new(val), Atomic::null())
    }
}

#[derive(Debug)]
pub struct Queue<T> {
    head: CachePadded<Atomic<QNode<T>>>,
    tail: CachePadded<Atomic<QNode<T>>>,
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        let slf = Self {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let g = unsafe { unprotected() };
        let sentinel = Owned::new(QNode::null()).into_shared(&g);
        slf.head.store(sentinel, Ordering::Relaxed);
        slf.tail.store(sentinel, Ordering::Relaxed);
        slf
    }
    pub fn blocking_enqueue(&self, new: T, g: &Guard) {
        let newptr = Owned::new(QNode::new_data(new)).into_shared(g);
        loop {
            // get current tail
            let tailptr = self.tail.load(Ordering::Acquire, g);
            let tail = unsafe { tailptr.deref() };
            let tail_nextptr = tail.next.load(Ordering::Acquire, g);
            if tail_nextptr.is_null() {
                // tail points to null which means this should ideally by the last LL node
                if tail
                    .next
                    .compare_exchange(
                        Shared::null(),
                        newptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                        g,
                    )
                    .is_ok()
                {
                    /*
                        CAS'd in but tail is *probably* lagging behind. This CAS might fail but we don't care since we're allowed to have a lagging tail
                    */
                    let _ = self.tail.compare_exchange(
                        tailptr,
                        newptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                        g,
                    );
                    break;
                }
            } else {
                // tail is lagging behind; attempt to help update it
                let _ = self.tail.compare_exchange(
                    tailptr,
                    tail_nextptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                    g,
                );
            }
        }
    }
    pub fn blocking_try_dequeue(&self, g: &Guard) -> Option<T> {
        loop {
            // get current head
            let headptr = self.head.load(Ordering::Acquire, g);
            let head = unsafe { headptr.deref() };
            let head_nextptr = head.next.load(Ordering::Acquire, g);
            if head_nextptr.is_null() {
                // this is the sentinel; queue is empty
                return None;
            }
            // we observe at this point in time that there is atleast one element in the list
            // let us swing that into sentinel position
            if self
                .head
                .compare_exchange(
                    headptr,
                    head_nextptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                    g,
                )
                .is_ok()
            {
                // good so we were able to update the head
                let tailptr = self.tail.load(Ordering::Acquire, g);
                // but wait, was this the last node? in that case, we need to update the tail before we destroy it.
                // this is fine though, as nothing will go boom right now since the tail is allowed to lag by one
                if headptr == tailptr {
                    // right so this was the last node uh oh
                    let _ = self.tail.compare_exchange(
                        tailptr,
                        head_nextptr,
                        Ordering::Release,
                        Ordering::Relaxed,
                        g,
                    );
                }
                // now we're in a position to happily destroy this
                unsafe { g.defer_destroy(headptr) }
                // read out the ptr
                return Some(unsafe { head_nextptr.deref().data.as_ptr().read() });
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let g = unsafe { unprotected() };
        while self.blocking_try_dequeue(g).is_some() {}
        // dealloc sentinel
        unsafe {
            self.head.load(Ordering::Relaxed, g).into_owned();
        }
    }
}

#[cfg(test)]
type StringQueue = Queue<String>;

#[sky_macros::test]
fn empty() {
    let q = StringQueue::new();
    drop(q);
}

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn empty_deq() {
    let g = pin();
    let q = StringQueue::new();
    assert_eq!(q.blocking_try_dequeue(&g), None);
}

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn empty_enq() {
    let g = pin();
    let q = StringQueue::new();
    q.blocking_enqueue("hello".into(), &g);
}

#[sky_macros::miri_leaky_test] // FIXME(@ohsayan): leak due to EBR
fn multi_eq_dq() {
    const ITEMS_L: usize = 100;
    use std::{sync::Arc, thread};
    let q = Arc::new(StringQueue::new());
    let producer_q = q.clone();
    let consumer_q = q.clone();
    let producer = thread::spawn(move || {
        let mut sent = vec![];
        let g = pin();
        for i in 0..ITEMS_L {
            let item = format!("time-{i}");
            // send a message and then sleep for two seconds
            producer_q.blocking_enqueue(item.clone(), &g);
            sent.push(item);
        }
        sent
    });
    let consumer = thread::spawn(move || {
        let g = pin();
        let mut received = vec![];
        loop {
            if received.len() == ITEMS_L {
                break;
            }
            if let Some(item) = consumer_q.blocking_try_dequeue(&g) {
                received.push(item);
            }
        }
        received
    });
    let sent = producer.join().unwrap();
    let received = consumer.join().unwrap();
    assert_eq!(sent, received);
}
