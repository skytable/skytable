/*
 * Created on Sun May 09 2021
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

#![allow(unused)] // TODO(@ohsayan): Plonk this

use crate::corestore::map::{
    bref::{Entry, OccupiedEntry, Ref, VacantEntry},
    iter::{BorrowedIter, OwnedIter},
    Skymap,
};
use ahash::RandomState;
use bytes::Bytes;
use std::borrow::Borrow;
use std::hash::Hash;
use std::iter::FromIterator;
use std::ops::Deref;

type HashTable<K, V> = Skymap<K, V, RandomState>;

#[derive(Debug)]
/// The Coremap contains the actual key/value pairs along with additional fields for data safety
/// and protection
pub struct Coremap<K, V>
where
    K: Eq + Hash,
{
    pub(crate) inner: HashTable<K, V>,
}

impl<K: Eq + Hash, V> Default for Coremap<K, V> {
    fn default() -> Self {
        Coremap {
            inner: HashTable::new_ahash(),
        }
    }
}

impl<K: Eq + Hash, V> Coremap<K, V> {
    /// Create an empty coremap
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(cap: usize) -> Self {
        Coremap {
            inner: HashTable::with_capacity(cap),
        }
    }
}

impl<K, V> Coremap<K, V>
where
    K: Eq + Hash,
{
    /// Returns the total number of key value pairs
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    /// Returns the removed value for key, it it existed
    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(key)
    }
    /// Returns true if an existent key was removed
    pub fn true_if_removed<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(key).is_some()
    }
    /// Check if a table contains a key
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains_key(key)
    }
    /// Clears the inner table!
    pub fn clear(&self) {
        self.inner.clear()
    }
    /// Return a non-consuming iterator
    pub fn iter(&self) -> BorrowedIter<'_, K, V, RandomState> {
        self.inner.get_iter()
    }
    /// Get a reference to the value of a key, if it exists
    pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }
    /// Returns true if the non-existent key was assigned to a value
    pub fn true_if_insert(&self, k: K, v: V) -> bool {
        if let Entry::Vacant(ve) = self.inner.entry(k) {
            ve.insert(v);
            true
        } else {
            false
        }
    }
    pub fn true_remove_if<Q>(&self, key: &Q, exec: impl FnOnce(&K, &V) -> bool) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove_if(key, exec).is_some()
    }
    pub fn remove_if<Q>(&self, key: &Q, exec: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove_if(key, exec)
    }
    /// Update or insert
    pub fn upsert(&self, k: K, v: V) {
        let _ = self.inner.insert(k, v);
    }
    /// Returns true if the value was updated
    pub fn true_if_update(&self, k: K, v: V) -> bool {
        if let Entry::Occupied(mut oe) = self.inner.entry(k) {
            oe.insert(v);
            true
        } else {
            false
        }
    }
    pub fn mut_entry(&self, key: K) -> Option<OccupiedEntry<K, V, RandomState>> {
        if let Entry::Occupied(oe) = self.inner.entry(key) {
            Some(oe)
        } else {
            None
        }
    }
    pub fn fresh_entry(&self, key: K) -> Option<VacantEntry<K, V, RandomState>> {
        if let Entry::Vacant(ve) = self.inner.entry(key) {
            Some(ve)
        } else {
            None
        }
    }
}

impl<K: Eq + Hash, V: Clone> Coremap<K, V> {
    pub fn get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get_cloned(key)
    }
}

impl<K: Eq + Hash + Clone, V> Coremap<K, V> {
    /// Returns atleast `count` number of keys from the hashtable
    pub fn get_keys(&self, count: usize) -> Vec<K> {
        let mut v = Vec::with_capacity(count);
        self.iter()
            .take(count)
            .map(|kv| kv.key().clone())
            .for_each(|key| v.push(key));
        v
    }
}

impl<K: Eq + Hash, V> IntoIterator for Coremap<K, V> {
    type Item = (K, V);
    type IntoIter = OwnedIter<K, V, RandomState>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.get_owned_iter()
    }
}

impl Deref for Data {
    type Target = [u8];
    fn deref(&self) -> &<Self>::Target {
        &self.blob
    }
}

impl Borrow<[u8]> for Data {
    fn borrow(&self) -> &[u8] {
        self.blob.borrow()
    }
}

impl Borrow<Bytes> for Data {
    fn borrow(&self) -> &Bytes {
        &self.blob
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        &self.blob
    }
}

impl<K, V> FromIterator<(K, V)> for Coremap<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        Coremap {
            inner: Skymap::from_iter(iter),
        }
    }
}

/// A wrapper for `Bytes`
#[derive(Debug, PartialEq, Clone, Hash)]
pub struct Data {
    /// The blob of data
    blob: Bytes,
}

impl PartialEq<str> for Data {
    fn eq(&self, oth: &str) -> bool {
        self.blob.eq(oth)
    }
}

impl Data {
    /// Create a new blob from a string
    pub fn from_string(val: String) -> Self {
        Data {
            blob: Bytes::from(val.into_bytes()),
        }
    }
    /// Create a new blob from an existing `Bytes` instance
    pub const fn from_blob(blob: Bytes) -> Self {
        Data { blob }
    }
    /// Get the inner blob (raw `Bytes`)
    pub const fn get_blob(&self) -> &Bytes {
        &self.blob
    }
    pub fn into_inner(self) -> Bytes {
        self.blob
    }
    #[allow(clippy::needless_lifetimes)]
    pub fn copy_from_slice<'a>(slice: &'a [u8]) -> Self {
        Self {
            blob: Bytes::copy_from_slice(slice),
        }
    }
}

impl Eq for Data {}

impl<T> From<T> for Data
where
    T: Into<Bytes>,
{
    fn from(dat: T) -> Self {
        Self { blob: dat.into() }
    }
}
