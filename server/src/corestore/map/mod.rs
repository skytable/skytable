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

#![allow(clippy::manual_map)] // avoid LLVM bloat

use crate::util::compiler;
use core::borrow::Borrow;
use core::fmt;
use core::hash::BuildHasher;
use core::hash::Hash;
use core::hash::Hasher;
use core::iter::FromIterator;
use core::mem;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::collections::hash_map::RandomState;
pub mod bref;
use iter::{BorrowedIter, BorrowedIterMut, OwnedIter};
pub mod iter;
use bref::{Entry, OccupiedEntry, Ref, RefMut, VacantEntry};

type LowMap<K, V> = hashbrown::raw::RawTable<(K, V)>;
type ShardSlice<K, V> = [RwLock<LowMap<K, V>>];
type SRlock<'a, K, V> = RwLockReadGuard<'a, hashbrown::raw::RawTable<(K, V)>>;
type SWlock<'a, K, V> = RwLockWriteGuard<'a, hashbrown::raw::RawTable<(K, V)>>;
const BITS_IN_USIZE: usize = mem::size_of::<usize>() * 8;
const DEFAULT_CAP: usize = 128;

fn make_hash<K, Q, S>(hash_builder: &S, val: &Q) -> u64
where
    K: Borrow<Q>,
    Q: Hash + ?Sized,
    S: BuildHasher,
{
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

fn make_insert_hash<K, S>(hash_builder: &S, val: &K) -> u64
where
    K: Hash,
    S: BuildHasher,
{
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

fn make_hasher<K, Q, V, S>(hash_builder: &S) -> impl Fn(&(Q, V)) -> u64 + '_
where
    K: Borrow<Q>,
    Q: Hash,
    S: BuildHasher,
{
    move |val| make_hash::<K, Q, S>(hash_builder, &val.0)
}

fn ceq<Q, K, V>(k: &Q) -> impl Fn(&(K, V)) -> bool + '_
where
    K: Borrow<Q>,
    Q: ?Sized + Eq,
{
    move |x| k.eq(x.0.borrow())
}

fn get_shard_count() -> usize {
    (num_cpus::get() * 4).next_power_of_two()
}

const fn cttz(amount: usize) -> usize {
    amount.trailing_zeros() as usize
}

pub struct Skymap<K, V, S = RandomState> {
    shards: Box<ShardSlice<K, V>>,
    hasher: S,
    shift: usize,
}

impl<K, V> Default for Skymap<K, V, RandomState> {
    fn default() -> Self {
        Self::with_hasher(RandomState::default())
    }
}

impl<K: fmt::Debug, V: fmt::Debug, S: BuildHasher + Default> fmt::Debug for Skymap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for s in self.get_iter() {
            map.entry(s.key(), s.value());
        }
        map.finish()
    }
}

impl<K, V, S> FromIterator<(K, V)> for Skymap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default + Clone,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        let map = Skymap::new();
        iter.into_iter().for_each(|(k, v)| {
            let _ = map.insert(k, v);
        });
        map
    }
}

// basic impls
impl<K, V, S> Skymap<K, V, S>
where
    S: BuildHasher + Default,
{
    pub fn new() -> Self {
        Self::with_hasher(S::default())
    }
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, S::default())
    }
    pub fn with_capacity_and_hasher(mut cap: usize, hasher: S) -> Self {
        let shard_count = get_shard_count();
        let shift = BITS_IN_USIZE - cttz(shard_count);
        if cap != 0 {
            cap = (cap + (shard_count - 1)) & !(shard_count - 1);
        }

        let cap_per_shard = cap / shard_count;
        Self {
            shards: (0..shard_count)
                .map(|_| RwLock::new(LowMap::with_capacity(cap_per_shard)))
                .collect(),
            hasher,
            shift,
        }
    }
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(DEFAULT_CAP, hasher)
    }
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }
    pub fn capacity(&self) -> usize {
        self.shards.iter().map(|s| s.read().capacity()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn get_iter(&self) -> BorrowedIter<K, V, S> {
        BorrowedIter::new(self)
    }
    pub fn get_iter_mut(&self) -> BorrowedIterMut<K, V, S> {
        BorrowedIterMut::new(self)
    }
    pub fn get_owned_iter(self) -> OwnedIter<K, V, S> {
        OwnedIter::new(self)
    }
}

// const impls
impl<K, V, S> Skymap<K, V, S> {
    const fn shards(&self) -> &ShardSlice<K, V> {
        &self.shards
    }
    const fn determine_shard(&self, hash: usize) -> usize {
        (hash << 7) >> self.shift
    }
    const fn h(&self) -> &S {
        &self.hasher
    }
}

// insert/get/remove impls

impl<K, V, S> Skymap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Clone,
{
    pub fn insert(&self, k: K, v: V) -> Option<V> {
        let hash = make_insert_hash::<K, S>(&self.hasher, &k);
        let idx = self.determine_shard(hash as usize);
        unsafe {
            // begin critical section
            let mut lowtable = self.get_wshard_unchecked(idx);
            if let Some((_, item)) = lowtable.get_mut(hash, ceq(&k)) {
                Some(mem::replace(item, v))
            } else {
                lowtable.insert(hash, (k, v), make_hasher::<K, _, V, S>(self.h()));
                None
            }
            // end critical section
        }
    }
    pub fn remove<Q>(&self, k: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = make_hash::<K, Q, S>(self.h(), k);
        let idx = self.determine_shard(hash as usize);
        unsafe {
            // begin critical section
            let mut lowtable = self.get_wshard_unchecked(idx);
            match lowtable.remove_entry(hash, ceq(k)) {
                Some(kv) => Some(kv),
                None => None,
            }
            // end critical section
        }
    }
    pub fn remove_if<Q>(&self, k: &Q, f: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = make_hash::<K, Q, S>(self.h(), k);
        let idx = self.determine_shard(hash as usize);
        unsafe {
            // begin critical section
            let mut lowtable = self.get_wshard_unchecked(idx);
            match lowtable.find(hash, ceq(k)) {
                Some(bucket) => {
                    let (kptr, vptr) = bucket.as_ref();
                    if f(kptr, vptr) {
                        Some(lowtable.remove(bucket))
                    } else {
                        None
                    }
                }
                None => None,
            }
            // end critical section
        }
    }
}

// lt impls
impl<'a, K: 'a + Hash + Eq, V: 'a, S: BuildHasher + Clone> Skymap<K, V, S> {
    pub fn get<Q>(&'a self, k: &Q) -> Option<Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = make_hash::<K, Q, S>(self.h(), k);
        let idx = self.determine_shard(hash as usize);
        unsafe {
            // begin critical section
            let lowtable = self.get_rshard_unchecked(idx);
            match lowtable.get(hash, ceq(k)) {
                Some((ref kptr, ref vptr)) => {
                    let kptr = compiler::extend_lifetime(kptr);
                    let vptr = compiler::extend_lifetime(vptr);
                    Some(Ref::new(lowtable, kptr, vptr))
                }
                None => None,
            }
            // end critical section
        }
    }

    pub fn get_mut<Q>(&'a self, k: &Q) -> Option<RefMut<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = make_hash::<K, Q, S>(self.h(), k);
        let idx = self.determine_shard(hash as usize);
        unsafe {
            // begin critical section
            let mut lowtable = self.get_wshard_unchecked(idx);
            match lowtable.get_mut(hash, ceq(k)) {
                Some(&mut (ref kptr, ref mut vptr)) => {
                    let kptr = compiler::extend_lifetime(kptr);
                    let vptr = compiler::extend_lifetime_mut(vptr);
                    Some(RefMut::new(lowtable, kptr, vptr))
                }
                None => None,
            }
            // end critical section
        }
    }
    pub fn entry(&'a self, key: K) -> Entry<'a, K, V, S> {
        let hash = make_insert_hash::<K, S>(self.h(), &key);
        let idx = self.determine_shard(hash as usize);
        unsafe {
            // begin critical section
            let lowtable = self.get_wshard_unchecked(idx);
            if let Some(elem) = lowtable.find(hash, ceq(&key)) {
                let (kptr, vptr) = elem.as_mut();
                let kptr = compiler::extend_lifetime(kptr);
                let vptr = compiler::extend_lifetime_mut(vptr);
                Entry::Occupied(OccupiedEntry::new(
                    lowtable,
                    key,
                    (kptr, vptr),
                    self.hasher.clone(),
                ))
            } else {
                Entry::Vacant(VacantEntry::new(lowtable, key, self.hasher.clone()))
            }
            // end critical section
        }
    }
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }
    pub fn clear(&self) {
        self.shards().iter().for_each(|shard| shard.write().clear())
    }
}

// inner impls
impl<'a, K: 'a, V: 'a, S> Skymap<K, V, S> {
    unsafe fn get_rshard_unchecked(&'a self, shard: usize) -> SRlock<'a, K, V> {
        self.shards.get_unchecked(shard).read()
    }
    unsafe fn get_wshard_unchecked(&'a self, shard: usize) -> SWlock<'a, K, V> {
        self.shards.get_unchecked(shard).write()
    }
}

#[test]
fn test_insert_remove() {
    let map = Skymap::default();
    map.insert("hello", "world");
    assert_eq!(map.remove("hello").unwrap().1, "world");
}

#[test]
fn test_remove_if() {
    let map = Skymap::default();
    map.insert("hello", "world");
    assert!(map
        .remove_if("hello", |_k, v| { (*v).eq("notworld") })
        .is_none());
}

#[test]
fn test_insert_get() {
    let map = Skymap::default();
    map.insert("sayan", "likes computational dark arts");
    let _ref = map.get("sayan").unwrap();
    assert_eq!(*_ref, "likes computational dark arts")
}

#[test]
fn test_entry() {
    let map = Skymap::default();
    map.insert("hello", "world");
    assert!(map.entry("hello").is_occupied());
    assert!(map.entry("world").is_vacant());
}
