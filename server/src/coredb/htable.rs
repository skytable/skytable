/*
 * Created on Sun May 09 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use bytes::Bytes;
use libsky::TResult;
use parking_lot::Condvar;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

const ORDERING_RELAXED: Ordering = Ordering::Relaxed;

#[derive(Debug)]
/// A thread-safe in-memory hashtable
///
/// This wraps around a [`Coremap`] object in an [`Arc`] to make it shareable across threads. Clones
/// are cheap because it just increments the atomic reference counter
pub struct HTable<K: Eq + Hash, V>
where
    K: Eq + Hash,
{
    inner: Arc<Coremap<K, V>>,
    _marker_key: PhantomData<K>,
    _marker_value: PhantomData<V>,
}

impl<K: Eq + Hash + Clone + Serialize, V: Clone + Serialize> HTable<K, V> {
    /// Create a new, empty in-memory table
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Coremap::new()),
            _marker_key: PhantomData,
            _marker_value: PhantomData,
        }
    }
    /// Initialize a new HTable instance from an existing [`Coremap`]
    pub fn from_raw(inner: Coremap<K, V>) -> Self {
        Self {
            inner: Arc::new(inner),
            _marker_key: PhantomData,
            _marker_value: PhantomData,
        }
    }
}

impl<K, V> Clone for HTable<K, V>
where
    K: Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _marker_key: self._marker_key,
            _marker_value: self._marker_value,
        }
    }
}

/// A [`CVar`] is a conditional variable that uses zero CPU time while waiting on a condition
///
/// This Condvar was specifically built for use with [`Coremap`] which uses a [`TableLockstateGuard`]
/// object to temporarily deny all writes
#[derive(Debug)]
struct Cvar {
    c: Condvar,
    m: Mutex<()>,
}

impl Cvar {
    fn new() -> Self {
        Self {
            c: Condvar::new(),
            m: Mutex::new(()),
        }
    }
    /// Notify all the threads waiting on this condvar that the state has changed
    fn notify_all(&self) {
        let _ = self.c.notify_all();
    }
    /// Wait for a notification on the conditional variable
    fn wait(&self, locked_state: &AtomicBool) {
        while locked_state.load(ORDERING_RELAXED) {
            // only wait if locked_state is true
            let guard = self.m.lock();
            let mut owned_guard = guard;
            self.c.wait(&mut owned_guard);
        }
    }
    /// Wait for a notification and then immediately run a closure as soon as the `locked_state`
    /// is false
    fn wait_and_then_immediately<T, F>(&self, locked_state: &AtomicBool, and_then: F) -> T
    where
        F: Fn() -> T,
    {
        while locked_state.load(ORDERING_RELAXED) {
            // only wait if locked_state is true
            let guard = self.m.lock();
            let mut owned_guard = guard;
            self.c.wait(&mut owned_guard);
        }
        and_then()
    }
}

use dashmap::iter::Iter;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
pub type HashTable<K, V> = DashMap<K, V>;

#[derive(Debug)]
/// The Coremap contains the actual key/value pairs along with additional fields for data safety
/// and protection
pub struct Coremap<K, V>
where
    K: Eq + Hash,
{
    inner: HashTable<K, V>,
    state_lock: AtomicBool,
    state_condvar: Cvar,
}

impl<K: Eq + Hash, V> Deref for HTable<K, V> {
    type Target = Coremap<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A table lock state guard
///
/// This object holds a locked [`Coremap`] object. The locked state corresponds to the internal `state_lock`
/// `AtomicBool`'s value. You can use the [`TableLockStateGuard`] to reference the actual table and do any operations
/// on it. It is recommended that whenever you're about to do a BGSAVE operation, call [`Coremap::lock_writes()`]
/// and you'll get this object. Use this object to mutate/read the data of the inner hashtable and then as soon
/// as this lock state goes out of scope, you can be sure that all threads waiting to write will get access.
///
/// ## Undefined Behavior (UB)
///
/// It is **absolutely undefined behavior to hold two lock states** for the same table because each one will
/// attempt to notify the other waiting threads. This will never happen unless you explicitly attempt to do it
/// as [`Coremap`] will wait for a [`TableLockStateGuard`] to be available before it gives you one
pub struct TableLockStateGuard<'a, K, V>
where
    K: Eq + Hash + Serialize,
    V: Serialize,
{
    inner: &'a Coremap<K, V>,
}

impl<'a, K: Eq + Hash + Serialize, V: Serialize> Deref for TableLockStateGuard<'a, K, V> {
    type Target = Coremap<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, K: Hash + Eq + Serialize, V: Serialize> Drop for TableLockStateGuard<'a, K, V> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): we know that no such guards exist, so indicate that the guards has been released
            self.inner._force_unlock_writes();
        }
    }
}

impl<K, V> Coremap<K, V>
where
    K: Eq + Hash + Serialize,
    V: Serialize,
{
    /// Create an empty coremap
    pub fn new() -> Self {
        Coremap {
            inner: HashTable::new(),
            state_lock: AtomicBool::new(false),
            state_condvar: Cvar::new(),
        }
    }
    /// Returns the total number of key value pairs
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    /// Returns the removed value for key, it it existed
    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.wait_for_write_unlock();
        self.inner.remove(key)
    }
    /// Returns true if an existent key was removed
    pub fn true_if_removed<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.wait_for_write_unlock();
        self.inner.remove(key).is_some()
    }
    /// Check if a table contains a key
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains_key(key)
    }
    /// Clears the inner table!
    pub fn clear(&self) {
        self.wait_for_write_unlock();
        self.inner.clear()
    }
    /// Return a non-consuming iterator
    pub fn iter(&self) -> Iter<'_, K, V> {
        self.inner.iter()
    }
    /// Get a reference to the value of a key, if it exists
    pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }
    /// Returns true if the non-existent key was assigned to a value
    pub fn true_if_insert(&self, k: K, v: V) -> bool {
        self.wait_for_write_unlock();
        if let Entry::Vacant(ve) = self.inner.entry(k) {
            ve.insert(v);
            true
        } else {
            false
        }
    }
    /// Update or insert
    pub fn upsert(&self, k: K, v: V) {
        self.wait_for_write_unlock();
        let _ = self.inner.insert(k, v);
    }
    /// Returns true if the value was updated
    pub fn true_if_update(&self, k: K, v: V) -> bool {
        self.wait_for_write_unlock();
        if let Entry::Occupied(mut oe) = self.inner.entry(k) {
            oe.insert(v);
            true
        } else {
            false
        }
    }
    /// Serialize the hashtable into a `Vec<u8>` that can be saved to a file
    pub fn serialize(&self) -> TResult<Vec<u8>> {
        bincode::serialize(&self.inner).map_err(|e| e.into())
    }
    /// Force lock writes on the underlying table
    ///
    /// ## Safety
    /// This function is unsafe to be called directly and may result in undefined behavior (UB).
    /// Instead, call [`Coremap::lock_writes`]
    unsafe fn _force_lock_writes(&self) -> TableLockStateGuard<'_, K, V> {
        self.state_lock.store(true, ORDERING_RELAXED);
        self.state_condvar.notify_all();
        TableLockStateGuard { inner: &self }
    }
    /// Force unlock writes on the underlying table
    ///
    /// ## Safety
    /// This function is unsafe to be called directly and may result in undefined behavior (UB).
    /// Instead, call [`Coremap::lock_writes`] and then drop the [`TableLockStateGuard`] to unlock
    /// writes on the table (will be dropped as soon as it goes out of scope)
    unsafe fn _force_unlock_writes(&self) {
        self.state_lock.store(false, ORDERING_RELAXED);
        self.state_condvar.notify_all();
    }
    /// Blocks the current thread, waiting for an unlock on writes
    fn wait_for_write_unlock(&self) {
        self.state_condvar.wait(&self.state_lock);
    }
    /// Wait for an unlock on writes and then immediately run the provided closure (`then`)
    fn wait_for_write_unlock_and_then<T, F>(&self, then: F) -> T
    where
        F: Fn() -> T,
    {
        self.state_condvar
            .wait_and_then_immediately(&self.state_lock, then)
    }
    /// Lock writes on the table
    ///
    /// This will immediately return a [`TableLockStateGuard`] if the table is in an unlocked state,
    /// but however **will block if the table is already locked** and then return when a guard is available
    pub fn lock_writes(&self) -> TableLockStateGuard<'_, K, V> {
        self.wait_for_write_unlock_and_then(|| unsafe {
            // UNSAFE(@ohsayan): This is safe because we're running it exactly after acquiring a lock
            // since we've got a write unlock at this exact point, we're free to lock the table
            // so this _should be_ safe
            // FIXME: UB/race condition here? What if exactly after the write unlock another thread does a lock_writes?
            self._force_lock_writes()
        })
    }
}

impl Coremap<Data, Data> {
    /// Returns a `Coremap<Data, Data>` from the provided file (as a `Vec<u8>`)
    pub fn deserialize(src: Vec<u8>) -> TResult<Self> {
        let h: HashTable<Data, Data> = bincode::deserialize(&src)?;
        Ok(Self {
            inner: h,
            state_lock: AtomicBool::new(false),
            state_condvar: Cvar::new(),
        })
    }
    /// Returns atleast `count` number of keys from the hashtable
    pub fn get_keys(&self, count: usize) -> Vec<Bytes> {
        let mut v = Vec::with_capacity(count);
        self.iter()
            .take(count)
            .map(|kv| kv.key().get_blob().clone())
            .for_each(|key| v.push(key));
        v
    }
}
impl<K: Eq + Hash, V> IntoIterator for Coremap<K, V> {
    type Item = (K, V);
    type IntoIter = dashmap::iter::OwningIter<K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl Deref for Data {
    type Target = [u8];
    fn deref(&self) -> &<Self>::Target {
        &self.blob
    }
}

impl Borrow<[u8]> for Data {
    fn borrow(&self) -> &[u8] {
        &self.blob.borrow()
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        &self.blob
    }
}

impl<K, V> FromIterator<(K, V)> for HTable<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        Self {
            inner: Arc::new(Coremap {
                inner: DashMap::from_iter(iter),
                state_lock: AtomicBool::new(false),
                state_condvar: Cvar::new(),
            }),
            _marker_value: PhantomData,
            _marker_key: PhantomData,
        }
    }
}

impl<K, V> FromIterator<(K, V)> for Coremap<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        Coremap {
            inner: DashMap::from_iter(iter),
            state_lock: AtomicBool::new(false),
            state_condvar: Cvar::new(),
        }
    }
}

/// A wrapper for `Bytes`
#[derive(Debug, PartialEq, Clone, Hash)]
pub struct Data {
    /// The blob of data
    blob: Bytes,
}

impl Data {
    /// Create a new blob from a string
    pub fn from_string(val: String) -> Self {
        Data {
            blob: Bytes::from(val.into_bytes()),
        }
    }
    /// Create a new blob from an existing `Bytes` instance
    pub const fn from_blob(blob: Bytes) -> Self {
        Data { blob }
    }
    /// Get the inner blob (raw `Bytes`)
    pub const fn get_blob(&self) -> &Bytes {
        &self.blob
    }
}

impl Eq for Data {}

impl<T> From<T> for Data
where
    T: Into<Bytes>,
{
    fn from(dat: T) -> Self {
        Self { blob: dat.into() }
    }
}

use serde::ser::{SerializeSeq, Serializer};

impl Serialize for Data {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.blob.len()))?;
        for e in self.blob.iter() {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

use serde::de::{Deserializer, SeqAccess, Visitor};

struct DataVisitor;
impl<'de> Visitor<'de> for DataVisitor {
    type Value = Data;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Expecting a coredb::htable::Data object")
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut bytes = Vec::new();
        while let Some(unsigned_8bit_int) = seq.next_element()? {
            bytes.push(unsigned_8bit_int);
        }
        Ok(Data::from_blob(Bytes::from(bytes)))
    }
}

impl<'de> Deserialize<'de> for Data {
    fn deserialize<D>(deserializer: D) -> Result<Data, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(DataVisitor)
    }
}

#[test]
fn test_de() {
    let x = HTable::new();
    x.upsert(
        Data::from("sayan"),
        Data::from_string("is writing open-source code".to_owned()),
    );
    let ser = x.serialize().unwrap();
    let de = Coremap::deserialize(ser).unwrap();
    assert!(de.contains_key(&Data::from("sayan")));
    assert!(de.len() == x.len());
    let hmap: Coremap<Data, Data> = Coremap::new();
    hmap.upsert(Data::from("sayan"), Data::from("writes code"));
    assert!(hmap.get("sayan".as_bytes()).is_some());
}

#[cfg(test)]
mod concurrency_tests {
    use super::HTable;
    #[test]
    fn test_race_and_multiple_table_lock_state_guards() {
        // this will test for a race condition and should take approximately 40 seconds to complete
        // although that doesn't include any possible delays involved
        // Uncomment the `println`s for seeing the thing in action (or to debug)
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;
        let skymap: HTable<&str, &str> = HTable::new();
        for _ in 0..1000 {
            let c1 = skymap.clone();
            let c2 = skymap.clone();
            let c3 = skymap.clone();
            let c4 = skymap.clone();
            // all producers will send a +1 on acquiring a lock and -1 on releasing a lock
            let (tx, rx) = mpsc::channel::<isize>();
            // this variable maintains the number of table wide write locks that are currently held
            let mut number_of_table_wide_locks = 0;
            let thread_2_sender = tx.clone();
            let thread_3_sender = tx.clone();
            let thread_4_sender = tx.clone();
            let (h1, h2, h3, h4) = (
                thread::spawn(move || {
                    // println!("[T1] attempting acquire/waiting on lock");
                    let lck = c1.lock_writes();
                    tx.send(1).unwrap();
                    // println!("[T1] Acquired lock now");
                    for _i in 0..10 {
                        // println!("[T1] Sleeping for {}/10ms", i + 1);
                        thread::sleep(Duration::from_millis(1));
                    }
                    drop(lck);
                    tx.send(-1).unwrap();
                    drop(tx);
                    // println!("[T1] Dropped lock");
                }),
                thread::spawn(move || {
                    let tx = thread_2_sender;
                    // println!("[T2] attempting acquire/waiting on lock");
                    let lck = c2.lock_writes();
                    tx.send(1).unwrap();
                    // println!("[T2] Acquired lock now");
                    for _i in 0..10 {
                        // println!("[T2] Sleeping for {}/10ms", i + 1);
                        thread::sleep(Duration::from_millis(1));
                    }
                    drop(lck);
                    tx.send(-1).unwrap();
                    drop(tx);
                    // println!("[T2] Dropped lock")
                }),
                thread::spawn(move || {
                    let tx = thread_3_sender;
                    // println!("[T3] attempting acquire/waiting on lock");
                    let lck = c3.lock_writes();
                    tx.send(1).unwrap();
                    // println!("[T3] Acquired lock now");
                    for _i in 0..10 {
                        // println!("[T3] Sleeping for {}/10ms", i + 1);
                        thread::sleep(Duration::from_millis(1));
                    }
                    drop(lck);
                    tx.send(-1).unwrap();
                    drop(tx);
                    // println!("[T3] Dropped lock");
                }),
                thread::spawn(move || {
                    let tx = thread_4_sender;
                    // println!("[T4] attempting acquire/waiting on lock");
                    let lck = c4.lock_writes();
                    tx.send(1).unwrap();
                    // println!("[T4] Acquired lock now");
                    for _i in 0..10 {
                        // println!("[T4] Sleeping for {}/10ms", i + 1);
                        thread::sleep(Duration::from_millis(1));
                    }
                    drop(lck);
                    tx.send(-1).unwrap();
                    drop(tx);
                    // println!("[T4] Dropped lock");
                }),
            );
            // allow this because we're just trying to make sure that all threads are terminate at the same time
            #[allow(clippy::drop_copy)]
            drop((
                h1.join().unwrap(),
                h2.join().unwrap(),
                h3.join().unwrap(),
                h4.join().unwrap(),
            ));
            // allow this lint because this is a test where we just want to keep things simple
            #[allow(clippy::for_loops_over_fallibles)]
            // wait in a loop to receive notifications on this mpsc channel
            // all received messages are in the same order as they were produced
            for msg in rx.recv() {
                // add the sent isize to the counter of number of table wide write locks
                number_of_table_wide_locks += msg;
                if number_of_table_wide_locks >= 2 {
                    // if there are more than/same as 2 writes at the same time, then that's trouble
                    // for us
                    panic!("Two threads acquired lock");
                }
            }
        }
    }

    #[test]
    fn test_wait_on_one_thread_insert_others() {
        use devtimer::DevTime;
        use std::thread;
        use std::time::Duration;
        let skymap: HTable<&str, &str> = HTable::new();
        assert!(skymap.true_if_insert("sayan", "wrote some dumb stuff"));
        let c1 = skymap.clone();
        let c2 = skymap.clone();
        let c3 = skymap.clone();
        let c4 = skymap.clone();
        let c5 = skymap.clone();
        let h1 = thread::spawn(move || {
            let x = c1.lock_writes();
            for _i in 0..10 {
                // println!("Waiting to unlock write: {}/10", i + 1);
                thread::sleep(Duration::from_secs(1));
            }
            drop(x);
        });
        /*
          wait for h1 to start up; 2s wait
          the other threads will have to wait atleast 7.5x10^9 nanoseconds before
          they can do anything useful. Atleast because thread::sleep can essentially sleep for
          longer but not lesser. So let's say the sleep is actually 2s, then each thread will have to wait for 8s,
          if the sleep is longer, say 2.5ms (we'll **assume** a maximum delay of 500ms in the sleep duration)
          then each thread will have to wait for ~7.5s. This is the basis of this test, to ensure that the waiting
          threads are notified in a timely fashion, approximate of course. The only exception is the get that
          doesn't need to mutate anything. Uncomment the `println`s for seeing the thing in action (or to debug)
          If anyone sees too many test failures with this duration, adjust it one the basis of the knowledge
          that you have acquired here.
        */
        thread::sleep(Duration::from_millis(2000));
        let h2 = thread::spawn(move || {
            let mut dt = DevTime::new_simple();
            // println!("[T2] Waiting to insert value");
            dt.start();
            c2.true_if_insert("sayan1", "writes-code");
            dt.stop();
            assert!(dt.time_in_nanos().unwrap() >= 7_500_000_000);
            // println!("[T2] Finished inserting");
        });
        let h3 = thread::spawn(move || {
            let mut dt = DevTime::new_simple();
            // println!("[T3] Waiting to insert value");
            dt.start();
            c3.true_if_insert("sayan2", "writes-code");
            dt.stop();
            assert!(dt.time_in_nanos().unwrap() >= 7_500_000_000);
            // println!("[T3] Finished inserting");
        });
        let h4 = thread::spawn(move || {
            let mut dt = DevTime::new_simple();
            // println!("[T4] Waiting to insert value");
            dt.start();
            c4.true_if_insert("sayan3", "writes-code");
            dt.stop();
            assert!(dt.time_in_nanos().unwrap() >= 7_500_000_000);
            // println!("[T4] Finished inserting");
        });
        let h5 = thread::spawn(move || {
            let mut dt = DevTime::new_simple();
            // println!("[T3] Waiting to get value");
            dt.start();
            let _got = c5.get("sayan").map(|v| *v).unwrap_or("<none>");
            dt.stop();
            assert!(dt.time_in_nanos().unwrap() <= 1_000_000_000);
            // println!("Got: '{:?}'", got);
            // println!("[T3] Finished reading. Returned immediately from now");
        });
        // allow this because we're just trying to make sure that all threads are terminate at the same time
        #[allow(clippy::drop_copy)]
        drop((
            h1.join().unwrap(),
            h2.join().unwrap(),
            h3.join().unwrap(),
            h4.join().unwrap(),
            h5.join().unwrap(),
        ));
    }
}
