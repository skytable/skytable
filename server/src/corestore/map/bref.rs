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

pub struct Ref<'a, K, V> {
    _guard: RwLockReadGuard<'a, LowMap<K, V>>,
    k: &'a K,
    v: &'a V,
}

impl<'a, K, V> Ref<'a, K, V> {
    pub const fn new(_guard: RwLockReadGuard<'a, LowMap<K, V>>, k: &'a K, v: &'a V) -> Self {
        Self { _guard, k, v }
    }
    pub const fn key(&self) -> &K {
        self.k
    }
    pub const fn value(&self) -> &V {
        self.v
    }
    pub const fn pair(&self) -> (&K, &V) {
        (self.k, self.v)
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

pub struct RefMut<'a, K, V> {
    guard: RwLockWriteGuard<'a, LowMap<K, V>>,
    k: &'a K,
    v: &'a mut V,
}

impl<'a, K, V> RefMut<'a, K, V> {
    pub fn new(guard: RwLockWriteGuard<'a, LowMap<K, V>>, k: &'a K, v: &'a mut V) -> Self {
        Self { guard, k, v }
    }
    pub const fn key(&self) -> &K {
        self.k
    }
    pub const fn value(&self) -> &V {
        self.v
    }
    pub fn value_mut(&mut self) -> &mut V {
        self.v
    }
    pub fn pair(&mut self) -> (&K, &V) {
        (self.k, self.v)
    }
    pub fn downgrade_ref(self) -> Ref<'a, K, V> {
        Ref::new(RwLockWriteGuard::downgrade(self.guard), self.k, self.v)
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

pub struct OccupiedEntry<'a, K, V, S> {
    guard: RwLockWriteGuard<'a, LowMap<K, V>>,
    elem: (&'a K, &'a mut V),
    key: K,
    hasher: S,
}

impl<'a, K: Hash + Eq, V, S: BuildHasher> OccupiedEntry<'a, K, V, S> {
    pub fn new(
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
    pub fn key(&self) -> &K {
        self.elem.0
    }
    pub fn value(&self) -> &V {
        self.elem.1
    }
    pub fn insert(&mut self, other: V) -> V {
        mem::replace(self.elem.1, other)
    }
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

pub struct VacantEntry<'a, K, V, S> {
    guard: RwLockWriteGuard<'a, LowMap<K, V>>,
    key: K,
    hasher: S,
}

impl<'a, K: Hash + Eq, V, S: BuildHasher> VacantEntry<'a, K, V, S> {
    pub fn new(guard: RwLockWriteGuard<'a, LowMap<K, V>>, key: K, hasher: S) -> Self {
        Self { guard, key, hasher }
    }
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
    pub fn into_key(self) -> K {
        self.key
    }
    pub fn key(&self) -> &K {
        &self.key
    }
}

pub enum Entry<'a, K, V, S = RandomState> {
    Occupied(OccupiedEntry<'a, K, V, S>),
    Vacant(VacantEntry<'a, K, V, S>),
}

impl<'a, K, V, S> Entry<'a, K, V, S> {
    pub const fn is_occupied(&self) -> bool {
        matches!(self, Self::Occupied(_))
    }
    pub const fn is_vacant(&self) -> bool {
        matches!(self, Self::Vacant(_))
    }
}
