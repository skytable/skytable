/*
 * Created on Thu Jan 19 2023
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

#![deny(unreachable_patterns)]

mod meta;
mod mtchm;
mod stdhm;
mod stord;
#[cfg(test)]
mod tests;

use {
    crate::engine::sync::atm::Guard,
    core::{borrow::Borrow, hash::Hash},
};

// re-exports
pub type IndexSTSeqCns<K, V> = stord::IndexSTSeqDll<K, V, stord::config::ConservativeConfig<K, V>>;
pub type IndexSTSeqLib<K, V> = stord::IndexSTSeqDll<K, V, stord::config::LiberalConfig<K, V>>;
pub type IndexMTRC<K, V> = mtchm::imp::ChmArc<K, V, mtchm::meta::DefConfig>;
pub type IndexMTCp<K, V> = mtchm::imp::ChmCopy<K, V, mtchm::meta::DefConfig>;

/// Any type implementing this trait can be used as a key inside memory engine structures
pub trait AsKey: Hash + Eq {
    /// Read the key
    fn read_key(&self) -> &Self;
}

impl<T: Hash + Eq + ?Sized> AsKey for T {
    fn read_key(&self) -> &Self {
        self
    }
}

/// If your T can be cloned/copied and implements [`AsKey`], then this trait will automatically be implemented
pub trait AsKeyClone: AsKey + Clone {
    /// Read the key and return a clone
    fn read_key_clone(&self) -> Self;
}

impl<T: AsKey + Clone + ?Sized> AsKeyClone for T {
    #[inline(always)]
    fn read_key_clone(&self) -> Self {
        Clone::clone(self)
    }
}

pub trait AsValue {
    fn read_value(&self) -> &Self;
}
impl<T: ?Sized> AsValue for T {
    fn read_value(&self) -> &Self {
        self
    }
}

/// Any type implementing this trait can be used as a value inside memory engine structures
pub trait AsValueClone: AsValue + Clone {
    /// Read the value and return a clone
    fn read_value_clone(&self) -> Self;
}

impl<T: AsValue + Clone + ?Sized> AsValueClone for T {
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
pub trait IndexBaseSpec<K, V>: Sized {
    /// Index supports prealloc?
    const PREALLOC: bool;
    #[cfg(debug_assertions)]
    /// A type representing debug metrics
    type Metrics;
    // init
    /// Initialize an empty instance of the index
    fn idx_init() -> Self;
    /// Initialize a pre-loaded instance of the index
    fn idx_init_with(s: Self) -> Self;
    /// Init the idx with the given cap
    ///
    /// By default doesn't attempt to allocate
    fn idx_init_cap(_: usize) -> Self {
        if Self::PREALLOC {
            panic!("expected prealloc");
        }
        Self::idx_init()
    }
    #[cfg(debug_assertions)]
    /// Returns a reference to the index metrics
    fn idx_metrics(&self) -> &Self::Metrics;
}

/// An unordered MTIndex
pub trait MTIndex<K, V>: IndexBaseSpec<K, V> {
    type IterKV<'t, 'g, 'v>: Iterator<Item = (&'v K, &'v V)>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        V: 'v,
        Self: 't;
    type IterKey<'t, 'g, 'v>: Iterator<Item = &'v K>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        Self: 't;
    type IterVal<'t, 'g, 'v>: Iterator<Item = &'v V>
    where
        'g: 't + 'v,
        't: 'v,
        V: 'v,
        Self: 't;
    /// Attempts to compact the backing storage
    fn mt_compact(&self) {}
    /// Clears all the entries in the MTIndex
    fn mt_clear(&self, g: &Guard);
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn mt_insert(&self, key: K, val: V, g: &Guard) -> bool
    where
        K: AsKeyClone,
        V: AsValue;
    /// Updates or inserts the given value
    fn mt_upsert(&self, key: K, val: V, g: &Guard)
    where
        K: AsKeyClone,
        V: AsValue;
    // read
    fn mt_contains<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        K: Borrow<Q> + AsKeyClone,
        Q: ?Sized + AsKey;
    /// Returns a reference to the value corresponding to the key, if it exists
    fn mt_get<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey,
        't: 'v,
        'g: 't + 'v;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn mt_get_cloned<Q>(&self, key: &Q, g: &Guard) -> Option<V>
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey,
        V: AsValueClone;
    // update
    /// Returns true if the entry is updated
    fn mt_update(&self, key: K, val: V, g: &Guard) -> bool
    where
        K: AsKeyClone,
        V: AsValue;
    /// Updates the entry and returns the old value, if it exists
    fn mt_update_return<'t, 'g, 'v>(&'t self, key: K, val: V, g: &'g Guard) -> Option<&'v V>
    where
        K: AsKeyClone,
        V: AsValue,
        't: 'v,
        'g: 't + 'v;
    // delete
    /// Returns true if the entry was deleted
    fn mt_delete<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey;
    /// Removes the entry and returns it, if it exists
    fn mt_delete_return<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey,
        't: 'v,
        'g: 't + 'v;
}

/// An unordered STIndex
pub trait STIndex<K, V>: IndexBaseSpec<K, V> {
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
    /// Attempts to compact the backing storage
    fn st_compact(&mut self) {}
    /// Clears all the entries in the STIndex
    fn st_clear(&mut self);
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn st_insert(&mut self, key: K, val: V) -> bool
    where
        K: AsKeyClone,
        V: AsValue;
    /// Updates or inserts the given value
    fn st_upsert(&mut self, key: K, val: V)
    where
        K: AsKeyClone,
        V: AsValue;
    // read
    fn st_contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + AsKeyClone,
        Q: ?Sized + AsKey;
    /// Returns a reference to the value corresponding to the key, if it exists
    fn st_get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn st_get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey,
        V: AsValueClone;
    // update
    /// Returns true if the entry is updated
    fn st_update<Q>(&mut self, key: &Q, val: V) -> bool
    where
        K: AsKeyClone + Borrow<Q>,
        V: AsValue,
        Q: ?Sized + AsKey;
    /// Updates the entry and returns the old value, if it exists
    fn st_update_return<Q>(&mut self, key: &Q, val: V) -> Option<V>
    where
        K: AsKeyClone + Borrow<Q>,
        V: AsValue,
        Q: ?Sized + AsKey;
    // delete
    /// Returns true if the entry was deleted
    fn st_delete<Q>(&mut self, key: &Q) -> bool
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey;
    /// Removes the entry and returns it, if it exists
    fn st_delete_return<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: AsKeyClone + Borrow<Q>,
        Q: ?Sized + AsKey;
    // iter
    /// Returns an iterator over a tuple of keys and values
    fn st_iter_kv<'a>(&'a self) -> Self::IterKV<'a>;
    /// Returns an iterator over the keys
    fn st_iter_key<'a>(&'a self) -> Self::IterKey<'a>;
    /// Returns an iterator over the values
    fn st_iter_value<'a>(&'a self) -> Self::IterValue<'a>;
}

pub trait STIndexSeq<K, V>: STIndex<K, V> {
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
