/*
 * Created on Fri May 07 2021
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

//! # Skymap &mdash; A concurrent hashmap
//!
//! This module implements [`Skymap`] an extremely fast concurrent Hashmap (or Hashtable). The primary goal
//! of this Hashmap is to reduce lock contentions when in a concurrent environment. This is achieved by using
//! bucket-level multi reader locks.
//!
//! ## Behind the implementation
//! Skymap itself isn't lockless but attempts to distribute the locks so as to reduce lock contentions (which
//! is the culprit for poor performance). In a Hashmap, you have buckets (or has buckets) which store the actual
//! data. The bucket your data will go into depends on its has that is computed by a hash function. In our use
//! case for a database, this is strictly a non-cryptographic hash function &mdash; and it is so for obvious reasons.
//! By holding a R/W lock for each bucket instead of the entire table, locks are distributed.
//!
//! ### Reallocation
//! However locks may be distributed, reallocations are likely to happen as we fill up the Skymap. This will require
//! us to hold a global lock across the table (effectively blocking off all reads/writes) and then the entire table
//! is rehashed. This is quite an expensive task but is better than increasing the load factor as that will pose
//! performance penalties. However, reallocations will only happen when the first few keys are inserted
//!
//! ### Collision Resolution
//! When two hashes for a given `Hash`able type `T` collide, we have to do something because they can't share the
//! same bucket. This is where Skymap uses an algorithm called [linear probing](https://en.wikipedia.org/wiki/Linear_probing)
//! as first suggested by G. Amdahl, Elaine M. McGraw and Arthur Samuel and first analyzed by Donald Knuth.
//! In this _strategy_ we move to the next bucket following the bucket where the hash collided and keep on moving
//! from then on until we find an empty bucket. The same happens while searching through the buckets
//!

use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::borrow::Borrow;
use std::cmp;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter;
use std::mem;
use std::ops;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The memory ordering that we'll follow throughout
const MEMORY_ORDERING: Ordering = Ordering::Relaxed;

/// Length-to-capacity factor; i.e when reallocating, if size is x, we'll increase capacity for 4x items
const MULTIPLICATION_FACTOR: usize = 4;

/// The numerator of the maximum load factor
/// We keep this at 85% (this has to be adjusted to see what gives the best performance).
///
/// A very low load factor would cause too many rehashes while a very high one could risk low performance. So it's
/// best to keep it _towards the higher end_
const MAX_LOAD_FACTOR_TOP: usize = MAX_LOAD_FACTOR_DENOM - 15;

/// The denominator of the maximum load factor
const MAX_LOAD_FACTOR_DENOM: usize = 100;

/// We choose the initial capacity to be 128
///
/// For this case, a very high initial capacity attracts faster OOMs while a very low initial capacity would
/// cause too many rehashes. Again, _keep it balanced_
const DEF_INIT_CAPACITY: usize = 128;

/// The smallest hashtable that we can have
const DEF_MIN_CAPACITY: usize = 16;

/// A `HashBucket` is a single entry (or _brick in a wall_) in a hashtable and represents the state
/// of the bucket
#[derive(Clone)]
enum HashBucket<K, V> {
    /// This bucket currently holds a K/V pair
    Contains(K, V),
    /// This bucket is empty and has never been used
    ///
    /// As linear probing resolves hash collisions by moving to the next bucket, it can cause
    /// clustering across the underlying structure. An `Empty` state indicates that it is the
    /// end of such a cluster
    Empty,
    /// This bucket is **not empty** but **is free for new data** and was removed
    ///
    /// It is very important for us to distinguish between `Empty` and `Removed` buckets; here's why:
    /// - An `Empty` bucket indicates that it has never been used; so while running a linear probe as
    /// part of a search, if we encounter an `Empty` field for a hash, we can safely consider that
    /// there won't be any buckets beyond that point for this hash.
    /// - However, if it is in a `Removed` state, it indicates that some data was stored in it initially
    /// and is now removed, but it **doesn't mean that there won't be any data beyond this bucket** for this
    /// hash
    Removed,
}

impl<K, V> HashBucket<K, V> {
    /// Check if this bucket has an `Empty` state
    const fn is_empty(&self) -> bool {
        if let Self::Empty = self {
            true
        } else {
            false
        }
    }
    /// Check if this bucket has a `Removed` state
    const fn is_removed(&self) -> bool {
        if let Self::Removed = self {
            true
        } else {
            false
        }
    }
    /// Check if the bucket is available (or free) for insertions
    const fn is_available(&self) -> bool {
        if let Self::Removed | Self::Empty = self {
            true
        } else {
            false
        }
    }
    /// Get a reference to the value if `Self` has a `Contains` state
    ///
    /// This will return `Some(value)` if the value exists or `None` if the bucket has no value
    const fn get_value_ref(&self) -> Option<&V> {
        if let Self::Contains(_, ref val) = self {
            Some(val)
        } else {
            None
        }
    }
    // don't try to const this; destructors aren't known at compile time!
    /// Same return as [`BucketState::get_value_ref()`] except for this function dropping the bucket
    fn get_value(self) -> Option<V> {
        if let Self::Contains(_, val) = self {
            Some(val)
        } else {
            None
        }
    }
}

/// The low-level _inner_ hashtable
struct Table<K, V> {
    /// The buckets
    buckets: Vec<RwLock<HashBucket<K, V>>>,
    /// The hasher
    hasher: RandomState,
}

impl<K, V> Table<K, V> {
    /// Initialize a new low-level table with a number of given buckets
    fn new(count: usize) -> Self {
        // First create and allocate the buckets with the HashBucket state to empty
        let mut buckets = Vec::with_capacity(count);
        (0..count)
            .into_iter()
            .for_each(|_| buckets.push(RwLock::new(HashBucket::Empty)));
        Table {
            buckets,
            hasher: RandomState::new(),
        }
    }
    /// Initialize a new low-level table with space for atleast `cap` keys
    fn with_capacity(cap: usize) -> Self {
        // This table will hold at least `cap` keys
        Table::new(cmp::max(
            DEF_MIN_CAPACITY,
            cap * MAX_LOAD_FACTOR_DENOM / MAX_LOAD_FACTOR_TOP + 1,
        ))
    }
}

impl<K, V> Table<K, V>
where
    K: PartialEq + Hash,
{
    /// Hash a key using `HashMap`'s `DefaultHasher`
    fn hash<T>(&self, key: &T) -> usize
    where
        T: Hash + ?Sized,
    {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
    /// Look for a `key` that matches a `predicate` `F` and return an immutable guard to it
    ///
    /// This is a low-level operation for matching keys and shouldn't be used until you know what
    /// you're doing!
    fn scan<F, Q>(&self, key: &Q, predicate: F) -> RwLockReadGuard<HashBucket<K, V>>
    where
        F: Fn(&HashBucket<K, V>) -> bool,
        Q: ?Sized + Hash,
    {
        let hash = self.hash(key);
        for i in 0..self.buckets.len() {
            /*
              The hashes are distributed across the buckets. We start scanning from the bottom of the table
              and start going up. Our hash index = (hash + bucket_we_are_at) % number of buckets
              Why the modulus (%) and all that -- well, hashes can get SUPER LARGE and like 2^64 large, so
              you possibly won't have that many buckets; that's why we shard them across the limited space we
              have. Why +i? Well, we just checked one bucket, it didn't match the predicate, so we'll obviously
              have to move away ... that's what linear probing does, doesn't it?
            */
            let lock = self.buckets[(hash + i) % self.buckets.len()].read();
            if predicate(&lock) {
                return lock;
            }
        }
        panic!("The given predicate doesn't match any bucket in our hash range");
    }
    /// Same as [`Self::scan`] except for this returning a mutable guard
    fn scan_mut<F, Q>(&self, key: &Q, predicate: F) -> RwLockWriteGuard<HashBucket<K, V>>
    where
        F: Fn(&HashBucket<K, V>) -> bool,
        Q: ?Sized + Hash,
    {
        let hash = self.hash(key);
        for i in 0..self.buckets.len() {
            // To understand what's going on here, see my comment for `Self::scan`
            let lock = self.buckets[(hash + i) % self.buckets.len()].write();
            if predicate(&lock) {
                return lock;
            }
        }
        panic!("The given predicate doesn't match any bucket in our hash range");
    }
    /// Look up a `key`
    ///
    /// This will either return an immutable reference to a [`HashBucket`] containing the k/v pair
    /// or it will return an empty bucket
    fn lookup<Q>(&self, key: &Q) -> RwLockReadGuard<HashBucket<K, V>>
    where
        Q: ?Sized + PartialEq + Hash,
        K: Borrow<Q>,
        // The `Borrow<Q>` just tells the compiler that Q can be used to search for K; this is because you
        // always don't have a `K` to lookup some given key; to state it 'properly', K can be borrowed as Q
    {
        self.scan(key, |val| match *val {
            // Check if the keys DO match; remember fella -- same hash doesn't mean the keys have to
            // be the same -- we're linear probing
            HashBucket::Contains(ref target_key, _) if key == target_key.borrow() => true,
            // Good, so there's nothing ahead; this predicate rets true, so we'll get an empty bucket
            HashBucket::Empty => true,
            // Nah, that doesn't work
            _ => false,
        })
    }
    /// Same as [`Self::lookup`] except that it returns a mutable guard to the bucket
    fn lookup_mut<Q>(&self, key: &Q) -> RwLockWriteGuard<HashBucket<K, V>>
    where
        Q: ?Sized + PartialEq + Hash,
        K: Borrow<Q>,
    {
        self.scan_mut(key, |val| match *val {
            // Check if the keys DO match
            HashBucket::Contains(ref target_key, _) if key == target_key.borrow() => true,
            // we'll get an empty bucket mutable bucket
            HashBucket::Empty => true,
            // Nah, that doesn't work
            _ => false,
        })
    }
    /// Returns a free bucket available to store a key
    fn find_free_mut(&self, key: &K) -> RwLockWriteGuard<HashBucket<K, V>> {
        self.scan_mut(key, |bucket| bucket.is_available())
    }
}

impl<K: Clone, V: Clone> Clone for Table<K, V> {
    fn clone(&self) -> Self {
        Table {
            hasher: self.hasher.clone(),
            buckets: self
                .buckets
                .iter()
                .map(|bucket| RwLock::new(bucket.read().clone()))
                .collect(),
        }
    }
}
