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

pub mod meta;
pub mod mtchm;
mod stdhm;
mod stord;
#[cfg(test)]
mod tests;

use {
    self::meta::Comparable,
    crate::engine::sync::atm::Guard,
    core::{borrow::Borrow, hash::Hash},
};

pub mod stdord_iter {
    pub use super::stord::iter::IndexSTSeqDllIterOrdKV;
}

// re-exports
pub type IndexSTSeqCns<K, V> = stord::IndexSTSeqDll<K, V, stord::config::ConservativeConfig<K, V>>;
#[cfg(test)]
pub type IndexSTSeqLib<K, V> = stord::IndexSTSeqDll<K, V, stord::config::LiberalConfig<K, V>>;
pub type IndexMTRaw<E> = mtchm::imp::Raw<E, mtchm::meta::DefConfig>;
pub type IndexST<K, V, S = std::collections::hash_map::RandomState> =
    std::collections::hash_map::HashMap<K, V, S>;

/// Any type implementing this trait can be used as a key inside memory engine structures
pub trait AsKey: Hash + Eq + 'static {
    /// Read the key
    fn read_key(&self) -> &Self;
}

impl<T: Hash + Eq + ?Sized + 'static> AsKey for T {
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

pub trait AsValue: 'static {
    fn read_value(&self) -> &Self;
}
impl<T: ?Sized + 'static> AsValue for T {
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
pub trait IndexBaseSpec: Sized {
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
pub trait MTIndex<E, K, V>: IndexBaseSpec {
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
    fn mt_iter_kv<'t, 'g, 'v>(&'t self, g: &'g Guard) -> Self::IterKV<'t, 'g, 'v>;
    fn mt_iter_key<'t, 'g, 'v>(&'t self, g: &'g Guard) -> Self::IterKey<'t, 'g, 'v>;
    fn mt_iter_val<'t, 'g, 'v>(&'t self, g: &'g Guard) -> Self::IterVal<'t, 'g, 'v>;
    /// Returns the length of the index
    fn mt_len(&self) -> usize;
    /// Attempts to compact the backing storage
    fn mt_compact(&self) {}
    /// Clears all the entries in the MTIndex
    fn mt_clear(&self, g: &Guard);
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn mt_insert(&self, e: E, g: &Guard) -> bool
    where
        V: AsValue;
    /// Updates or inserts the given value
    fn mt_upsert(&self, e: E, g: &Guard)
    where
        V: AsValue;
    // read
    fn mt_contains<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        Q: ?Sized + Comparable<K>;
    /// Returns a reference to the value corresponding to the key, if it exists
    fn mt_get<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v;
    fn mt_get_element<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v E>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn mt_get_cloned<Q>(&self, key: &Q, g: &Guard) -> Option<V>
    where
        Q: ?Sized + Comparable<K>,
        V: AsValueClone;
    // update
    /// Returns true if the entry is updated
    fn mt_update(&self, e: E, g: &Guard) -> bool
    where
        K: AsKeyClone,
        V: AsValue;
    /// Updates the entry and returns the old value, if it exists
    fn mt_update_return<'t, 'g, 'v>(&'t self, e: E, g: &'g Guard) -> Option<&'v V>
    where
        K: AsKeyClone,
        V: AsValue,
        't: 'v,
        'g: 't + 'v;
    // delete
    /// Returns true if the entry was deleted
    fn mt_delete<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        Q: ?Sized + Comparable<K>;
    /// Removes the entry and returns it, if it exists
    fn mt_delete_return<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v;
    fn mt_delete_return_entry<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v E>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v;
}

pub trait MTIndexExt<E, K, V>: MTIndex<E, K, V> {
    type IterEntry<'t, 'g, 'v>: Iterator<Item = &'v E>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        V: 'v,
        E: 'v,
        Self: 't;
    fn mt_iter_entry<'t, 'g, 'v>(&'t self, g: &'g Guard) -> Self::IterEntry<'t, 'g, 'v>;
}

/// An unordered STIndex
pub trait STIndex<K: ?Sized, V>: IndexBaseSpec {
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
    /// returns the length of the idx
    fn st_len(&self) -> usize;
    /// Attempts to compact the backing storage
    fn st_compact(&mut self) {}
    /// Clears all the entries in the STIndex
    fn st_clear(&mut self);
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn st_insert(&mut self, key: K, val: V) -> bool
    where
        K: AsKey,
        V: AsValue;
    /// Updates or inserts the given value
    fn st_upsert(&mut self, key: K, val: V)
    where
        K: AsKey,
        V: AsValue;
    // read
    fn st_contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + AsKey,
        Q: ?Sized + AsKey;
    /// Returns a reference to the value corresponding to the key, if it exists
    fn st_get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn st_get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey,
        V: AsValueClone;
    fn st_get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey;
    // update
    /// Returns true if the entry is updated
    fn st_update<Q>(&mut self, key: &Q, val: V) -> bool
    where
        K: AsKey + Borrow<Q>,
        V: AsValue,
        Q: ?Sized + AsKey;
    /// Updates the entry and returns the old value, if it exists
    fn st_update_return<Q>(&mut self, key: &Q, val: V) -> Option<V>
    where
        K: AsKey + Borrow<Q>,
        V: AsValue,
        Q: ?Sized + AsKey;
    // delete
    /// Returns true if the entry was deleted
    fn st_delete<Q>(&mut self, key: &Q) -> bool
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey;
    /// Removes the entry and returns it, if it exists
    fn st_delete_return<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey;
    fn st_delete_if<Q>(&mut self, key: &Q, iff: impl Fn(&V) -> bool) -> Option<bool>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey;
    // iter
    /// Returns an iterator over a tuple of keys and values
    fn st_iter_kv<'a>(&'a self) -> Self::IterKV<'a>;
    /// Returns an iterator over the keys
    fn st_iter_key<'a>(&'a self) -> Self::IterKey<'a>;
    /// Returns an iterator over the values
    fn st_iter_value<'a>(&'a self) -> Self::IterValue<'a>;
}

pub trait STIndexExt<K, V>: STIndex<K, V> {
    fn stext_get_key_value<Q>(&self, k: &Q) -> Option<(&K, &V)>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey;
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
    type OwnedIterKV: Iterator<Item = (K, V)> + DoubleEndedIterator<Item = (K, V)>;
    type OwnedIterKeys: Iterator<Item = K> + DoubleEndedIterator<Item = K>;
    type OwnedIterValues: Iterator<Item = V> + DoubleEndedIterator<Item = V>;
    /// Returns an ordered iterator over the KV pairs
    fn stseq_ord_kv<'a>(&'a self) -> Self::IterOrdKV<'a>;
    /// Returns an ordered iterator over the keys
    fn stseq_ord_key<'a>(&'a self) -> Self::IterOrdKey<'a>;
    /// Returns an ordered iterator over the values
    fn stseq_ord_value<'a>(&'a self) -> Self::IterOrdValue<'a>;
    // owned
    fn stseq_owned_kv(self) -> Self::OwnedIterKV;
    fn stseq_owned_keys(self) -> Self::OwnedIterKeys;
    fn stseq_owned_values(self) -> Self::OwnedIterValues;
}
