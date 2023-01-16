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

pub trait MTIndex<K, V>
where
    K: AsKey,
    V: AsValue,
{
    /// State whether the underlying structure provides any ordering on the iterators
    const HAS_ORDER: bool;
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
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn me_insert(&self, key: K, val: V) -> bool;
    /// Updates or inserts the given value
    fn me_upsert(&self, key: K, val: V);

    // read
    /// Returns a reference to the value corresponding to the key, if it exists
    fn me_get<Q>(&self, key: &Q) -> Option<&K>
    where
        K: Borrow<Q>;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn me_get_cloned<Q>(&self, key: &Q) -> Option<K>
    where
        K: Borrow<Q>;

    // update
    /// Returns true if the entry is updated
    fn me_update<Q>(&self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>;
    /// Updates the entry and returns the old value, if it exists
    fn me_update_return<Q>(&self, key: &Q, val: V) -> Option<K>
    where
        K: Borrow<Q>;

    // delete
    /// Returns true if the entry was deleted
    fn me_delete<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>;
    /// Removes the entry and returns it, if it exists
    fn me_delete_return<Q>(&self, key: &Q) -> Option<K>
    where
        K: Borrow<Q>;

    // iter
    /// Returns an iterator over a tuple of keys and values
    fn me_iter_kv<'a>(&'a self) -> Self::IterKV<'a>;
    /// Returns an iterator over the keys
    fn me_iter_k<'a>(&'a self) -> Self::IterKey<'a>;
    /// Returns an iterator over the values
    fn me_iter_v<'a>(&'a self) -> Self::IterValue<'a>;
}

pub trait STIndex<K, V> {
    /// State whether the underlying structure provides any ordering on the iterators
    const HAS_ORDER: bool;
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
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn st_insert(&mut self, key: K, val: V) -> bool;
    /// Updates or inserts the given value
    fn st_upsert(&mut self, key: K, val: V);

    // read
    /// Returns a reference to the value corresponding to the key, if it exists
    fn st_get<Q>(&self, key: &Q) -> Option<&K>
    where
        K: Borrow<Q>;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn st_get_cloned<Q>(&self, key: &Q) -> Option<K>
    where
        K: Borrow<Q>;

    // update
    /// Returns true if the entry is updated
    fn st_update<Q>(&mut self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>;
    /// Updates the entry and returns the old value, if it exists
    fn st_update_return<Q>(&mut self, key: &Q, val: V) -> Option<K>
    where
        K: Borrow<Q>;

    // delete
    /// Returns true if the entry was deleted
    fn st_delete<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>;
    /// Removes the entry and returns it, if it exists
    fn st_delete_return<Q>(&mut self, key: &Q) -> Option<K>
    where
        K: Borrow<Q>;

    // iter
    /// Returns an iterator over a tuple of keys and values
    fn st_iter_kv<'a>(&'a self) -> Self::IterKV<'a>;
    /// Returns an iterator over the keys
    fn st_iter_k<'a>(&'a self) -> Self::IterKey<'a>;
    /// Returns an iterator over the values
    fn st_iter_v<'a>(&'a self) -> Self::IterValue<'a>;
}
