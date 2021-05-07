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

//! # A concurrent hashmap
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

/// A `HashBucket` is a single entry (or _brick in a wall_) in a hashtable and represents the state
/// of the bucket
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
