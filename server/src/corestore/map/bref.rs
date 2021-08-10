/*
 * Created on Mon Aug 09 2021
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

use super::LowMap;
use crate::util::compiler;
use crate::util::Unwrappable;
use core::hash::BuildHasher;
use core::hash::Hash;
use core::mem;
use core::ops::Deref;
use core::ops::DerefMut;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::collections::hash_map::RandomState;
use std::sync::Arc;

/// A read-only reference to a bucket
pub struct Ref<'a, K, V> {
    _g: RwLockReadGuard<'a, LowMap<K, V>>,
    k: &'a K,
    v: &'a V,
}

impl<'a, K, V> Ref<'a, K, V> {
    /// Create a new reference
    pub(super) const fn new(_g: RwLockReadGuard<'a, LowMap<K, V>>, k: &'a K, v: &'a V) -> Self {
        Self { _g, k, v }
    }
    /// Get a ref to the key
    pub const fn key(&self) -> &K {
        self.k
    }
    /// Get a ref to the value
    pub const fn value(&self) -> &V {
        self.v
    }
    /// Get a ref to the key/value pair
    pub const fn pair(&self) -> (&K, &V) {
        let Self { k, v, .. } = self;
        (k, v)
    }
}

impl<'a, K, V> Deref for Ref<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<'a, K: Send, V: Send> Send for Ref<'a, K, V> {}
unsafe impl<'a, K: Sync, V: Sync> Sync for Ref<'a, K, V> {}

/// A r/w ref to a bucket
pub struct RefMut<'a, K, V> {
    _g: RwLockWriteGuard<'a, LowMap<K, V>>,
    k: &'a K,
    v: &'a mut V,
}

impl<'a, K, V> RefMut<'a, K, V> {
    /// Create a new ref
    pub(super) fn new(_g: RwLockWriteGuard<'a, LowMap<K, V>>, k: &'a K, v: &'a mut V) -> Self {
        Self { _g, k, v }
    }
    /// Get a ref to the key
    pub const fn key(&self) -> &K {
        self.k
    }
    /// Get a ref to the value
    pub const fn value(&self) -> &V {
        self.v
    }
    /// Get a mutable ref to the value
    pub fn value_mut(&mut self) -> &mut V {
        self.v
    }
    /// Get a ref to the k/v pair
    pub fn pair(&mut self) -> (&K, &V) {
        let Self { k, v, .. } = self;
        (k, v)
    }
}

impl<'a, K, V> Deref for RefMut<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl<'a, K, V> DerefMut for RefMut<'a, K, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.value_mut()
    }
}

unsafe impl<'a, K: Send, V: Send> Send for RefMut<'a, K, V> {}
unsafe impl<'a, K: Sync, V: Sync> Sync for RefMut<'a, K, V> {}

/// A reference to an occupied entry
pub struct OccupiedEntry<'a, K, V, S> {
    guard: RwLockWriteGuard<'a, LowMap<K, V>>,
    elem: (&'a K, &'a mut V),
    key: K,
    hasher: S,
}

impl<'a, K: Hash + Eq, V, S: BuildHasher> OccupiedEntry<'a, K, V, S> {
    /// Create a new occupied entry ref
    pub(super) fn new(
        guard: RwLockWriteGuard<'a, LowMap<K, V>>,
        key: K,
        elem: (&'a K, &'a mut V),
        hasher: S,
    ) -> Self {
        Self {
            guard,
            elem,
            key,
            hasher,
        }
    }
    /// Get a ref to the key
    pub fn key(&self) -> &K {
        self.elem.0
    }
    /// Get a ref to the value
    pub fn value(&self) -> &V {
        self.elem.1
    }
    /// Insert a value into this bucket
    pub fn insert(&mut self, other: V) -> V {
        mem::replace(self.elem.1, other)
    }
    /// Remove this element from the map
    pub fn remove(mut self) -> V {
        let hash = super::make_hash::<K, K, S>(&self.hasher, &self.key);
        unsafe {
            self.guard
                .remove_entry(hash, super::ceq(self.elem.0))
                .unsafe_unwrap()
        }
        .1
    }
}

unsafe impl<'a, K: Send, V: Send, S> Send for OccupiedEntry<'a, K, V, S> {}
unsafe impl<'a, K: Sync, V: Sync, S> Sync for OccupiedEntry<'a, K, V, S> {}

/// A ref to a vacant entry
pub struct VacantEntry<'a, K, V, S> {
    guard: RwLockWriteGuard<'a, LowMap<K, V>>,
    key: K,
    hasher: S,
}

impl<'a, K: Hash + Eq, V, S: BuildHasher> VacantEntry<'a, K, V, S> {
    /// Create a vacant entry ref
    pub(super) fn new(guard: RwLockWriteGuard<'a, LowMap<K, V>>, key: K, hasher: S) -> Self {
        Self { guard, key, hasher }
    }
    /// Insert a value into this bucket
    pub fn insert(mut self, value: V) -> RefMut<'a, K, V> {
        unsafe {
            let hash = super::make_insert_hash::<K, S>(&self.hasher, &self.key);
            let &mut (ref mut k, ref mut v) = self.guard.insert_entry(
                hash,
                (self.key, value),
                super::make_hasher::<K, _, V, S>(&self.hasher),
            );
            let kptr = compiler::extend_lifetime(k);
            let vptr = compiler::extend_lifetime_mut(v);
            RefMut::new(self.guard, kptr, vptr)
        }
    }
    /// Turns self into a key (effectively freeing up the entry for another thread)
    pub fn into_key(self) -> K {
        self.key
    }
    /// Get a ref to the key
    pub fn key(&self) -> &K {
        &self.key
    }
}

/// An entry, either occupied or vacant
pub enum Entry<'a, K, V, S = RandomState> {
    Occupied(OccupiedEntry<'a, K, V, S>),
    Vacant(VacantEntry<'a, K, V, S>),
}

impl<'a, K, V, S> Entry<'a, K, V, S> {
    /// Check if an entry is occupied
    pub const fn is_occupied(&self) -> bool {
        matches!(self, Self::Occupied(_))
    }
    /// Check if an entry is vacant
    pub const fn is_vacant(&self) -> bool {
        matches!(self, Self::Vacant(_))
    }
}

/// A shared ref to a key
pub struct RefMulti<'a, K, V> {
    _g: Arc<RwLockReadGuard<'a, LowMap<K, V>>>,
    k: &'a K,
    v: &'a V,
}

impl<'a, K, V> RefMulti<'a, K, V> {
    /// Create a new shared ref
    pub const fn new(_g: Arc<RwLockReadGuard<'a, LowMap<K, V>>>, k: &'a K, v: &'a V) -> Self {
        Self { _g, k, v }
    }
    /// Get a ref to the key
    pub const fn key(&self) -> &K {
        self.k
    }
    /// Get a ref to the value
    pub const fn value(&self) -> &V {
        self.v
    }
    /// Get a ref to the k/v pair
    pub const fn pair(&self) -> (&K, &V) {
        let Self { k, v, .. } = self;
        (k, v)
    }
}

impl<'a, K, V> Deref for RefMulti<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<'a, K: Sync, V: Sync> Sync for RefMulti<'a, K, V> {}
unsafe impl<'a, K: Send, V: Send> Send for RefMulti<'a, K, V> {}

/// A shared r/w ref to a bucket
pub struct RefMultiMut<'a, K, V> {
    _g: Arc<RwLockWriteGuard<'a, LowMap<K, V>>>,
    k: &'a K,
    v: &'a mut V,
}

impl<'a, K, V> RefMultiMut<'a, K, V> {
    /// Create a new shared r/w ref
    pub fn new(_g: Arc<RwLockWriteGuard<'a, LowMap<K, V>>>, k: &'a K, v: &'a mut V) -> Self {
        Self { _g, k, v }
    }
    /// Get a ref to the key
    pub const fn key(&self) -> &K {
        self.k
    }
    /// Get a ref to the value
    pub const fn value(&self) -> &V {
        self.v
    }
    /// Get a mutable ref to the value
    pub fn value_mut(&mut self) -> &mut V {
        self.v
    }
    /// Get a ref to the k/v pair
    pub fn pair(&self) -> (&K, &V) {
        let Self { k, v, .. } = self;
        (k, v)
    }
    /// Get a mutable ref to the k/v (k, mut v) pair
    pub fn pair_mut(&mut self) -> (&K, &mut V) {
        let Self { k, v, .. } = self;
        (k, v)
    }
}

impl<'a, K, V> Deref for RefMultiMut<'a, K, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl<'a, K, V> DerefMut for RefMultiMut<'a, K, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.value_mut()
    }
}

unsafe impl<'a, K: Sync, V: Sync> Sync for RefMultiMut<'a, K, V> {}
unsafe impl<'a, K: Send, V: Send> Send for RefMultiMut<'a, K, V> {}
