/*
 * Created on Wed Jan 11 2023
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

use core::{borrow::Borrow, hash::Hash};

/// Any type implementing this trait can be used as a key inside memory engine structures
pub trait AsKey: Hash + Eq + Clone {
    /// Read the key
    fn read_key(&self) -> &Self;
    /// Read the key and return a clone
    fn read_key_clone(&self) -> Self;
}

impl<T: Hash + Eq + Clone> AsKey for T {
    #[inline(always)]
    fn read_key(&self) -> &Self {
        self
    }
    #[inline(always)]
    fn read_key_clone(&self) -> Self {
        Clone::clone(self)
    }
}

pub trait AsKeyRef: Hash + Eq {}
impl<T: Hash + Eq + ?Sized> AsKeyRef for T {}

/// Any type implementing this trait can be used as a value inside memory engine structures
pub trait AsValue: Clone {
    /// Read the value
    fn read_value(&self) -> &Self;
    /// Read the value and return a clone
    fn read_value_clone(&self) -> Self;
}

impl<T: Clone> AsValue for T {
    #[inline(always)]
    fn read_value(&self) -> &Self {
        self
    }
    #[inline(always)]
    fn read_value_clone(&self) -> Self {
        Clone::clone(self)
    }
}

#[cfg(debug_assertions)]
/// A dummy metrics object
pub struct DummyMetrics;

/// The base spec for any index. Iterators have meaningless order, and that is intentional and oftentimes
/// consequential. For more specialized impls, use the [`STIndex`], [`MTIndex`] or [`STIndexSeq`] traits
pub trait IndexBaseSpec<K, V>
where
    K: AsKey,
    V: AsValue,
{
    /// Index supports prealloc?
    const PREALLOC: bool;
    #[cfg(debug_assertions)]
    /// A type representing debug metrics
    type Metrics;
    /// An iterator over the keys and values
    type IterKV<'a>: Iterator<Item = (&'a K, &'a V)>
    where
        Self: 'a,
        K: 'a,
        V: 'a;
    /// An iterator over the keys
    type IterKey<'a>: Iterator<Item = &'a K>
    where
        Self: 'a,
        K: 'a;
    /// An iterator over the values
    type IterValue<'a>: Iterator<Item = &'a V>
    where
        Self: 'a,
        V: 'a;
    // init
    /// Initialize an empty instance of the index
    fn idx_init() -> Self;
    /// Initialize a pre-loaded instance of the index
    fn idx_init_with(s: Self) -> Self;
    // iter
    /// Returns an iterator over a tuple of keys and values
    fn idx_iter_kv<'a>(&'a self) -> Self::IterKV<'a>;
    /// Returns an iterator over the keys
    fn idx_iter_key<'a>(&'a self) -> Self::IterKey<'a>;
    /// Returns an iterator over the values
    fn idx_iter_value<'a>(&'a self) -> Self::IterValue<'a>;
    #[cfg(debug_assertions)]
    /// Returns a reference to the index metrics
    fn idx_metrics(&self) -> &Self::Metrics;
}

/// An unordered MTIndex
pub trait MTIndex<K, V>: IndexBaseSpec<K, V>
where
    K: AsKey,
    V: AsValue,
{
    /// Attempts to compact the backing storage
    fn mt_compact(&self) {}
    /// Clears all the entries in the MTIndex
    fn mt_clear(&self);
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn mt_insert(&self, key: K, val: V) -> bool;
    /// Updates or inserts the given value
    fn mt_upsert(&self, key: K, val: V);
    // read
    /// Returns a reference to the value corresponding to the key, if it exists
    fn mt_get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn mt_get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    // update
    /// Returns true if the entry is updated
    fn mt_update<Q>(&self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    /// Updates the entry and returns the old value, if it exists
    fn mt_update_return<Q>(&self, key: &Q, val: V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    // delete
    /// Returns true if the entry was deleted
    fn mt_delete<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    /// Removes the entry and returns it, if it exists
    fn mt_delete_return<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
}

/// An unordered STIndex
pub trait STIndex<K, V>: IndexBaseSpec<K, V>
where
    K: AsKey,
    V: AsValue,
{
    /// Attempts to compact the backing storage
    fn st_compact(&mut self) {}
    /// Clears all the entries in the STIndex
    fn st_clear(&mut self);
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn st_insert(&mut self, key: K, val: V) -> bool;
    /// Updates or inserts the given value
    fn st_upsert(&mut self, key: K, val: V);
    // read
    /// Returns a reference to the value corresponding to the key, if it exists
    fn st_get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn st_get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    // update
    /// Returns true if the entry is updated
    fn st_update<Q>(&mut self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    /// Updates the entry and returns the old value, if it exists
    fn st_update_return<Q>(&mut self, key: &Q, val: V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    // delete
    /// Returns true if the entry was deleted
    fn st_delete<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
    /// Removes the entry and returns it, if it exists
    fn st_delete_return<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef;
}

pub trait STIndexSeq<K, V>: STIndex<K, V>
where
    K: AsKey,
    V: AsValue,
{
    /// An ordered iterator over the keys and values
    type IterOrdKV<'a>: Iterator<Item = (&'a K, &'a V)> + DoubleEndedIterator<Item = (&'a K, &'a V)>
    where
        Self: 'a,
        K: 'a,
        V: 'a;
    /// An ordered iterator over the keys
    type IterOrdKey<'a>: Iterator<Item = &'a K> + DoubleEndedIterator<Item = &'a K>
    where
        Self: 'a,
        K: 'a;
    /// An ordered iterator over the values
    type IterOrdValue<'a>: Iterator<Item = &'a V> + DoubleEndedIterator<Item = &'a V>
    where
        Self: 'a,
        V: 'a;
    /// Returns an ordered iterator over the KV pairs
    fn stseq_ord_kv<'a>(&'a self) -> Self::IterOrdKV<'a>;
    /// Returns an ordered iterator over the keys
    fn stseq_ord_key<'a>(&'a self) -> Self::IterOrdKey<'a>;
    /// Returns an ordered iterator over the values
    fn stseq_ord_value<'a>(&'a self) -> Self::IterOrdValue<'a>;
}
