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

pub trait MemoryEngine<K, V>
where
    K: AsKey,
    V: AsValue,
{
    const HAS_ORDER: bool;
    type IterKV<'a>: Iterator<Item = (&'a K, &'a V)>
    where
        Self: 'a,
        K: 'a,
        V: 'a;
    type IterKey<'a>: Iterator<Item = &'a K>
    where
        Self: 'a,
        K: 'a;
    // write
    /// Returns true if the entry was inserted successfully; returns false if the uniqueness constraint is
    /// violated
    fn insert(&self, key: K, val: V) -> bool;
    /// Returns a reference to the value corresponding to the key, if it exists

    // read
    fn get<Q>(&self, key: &Q) -> Option<&K>
    where
        K: Borrow<Q>;
    /// Returns a clone of the value corresponding to the key, if it exists
    fn get_cloned<Q>(&self, key: &Q) -> Option<K>
    where
        K: Borrow<Q>;

    // update
    /// Returns true if the entry is updated
    fn update<Q>(&self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>;
    /// Updates the entry and returns the old value, if it exists
    fn update_return<Q>(&self, key: &Q, val: V) -> Option<K>
    where
        K: Borrow<Q>;

    // delete
    /// Returns true if the entry was deleted
    fn delete<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>;
    /// Removes the entry and returns it, if it exists
    fn delete_return<Q>(&self, key: &Q) -> Option<K>
    where
        K: Borrow<Q>;
}
