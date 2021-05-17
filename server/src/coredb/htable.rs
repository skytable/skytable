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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
pub use std::collections::hash_map::Entry;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::iter::FromIterator;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HTable<K, V>
where
    K: Eq + Hash,
{
    inner: HashMap<K, V>,
}

impl<K, V> HTable<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        HTable {
            inner: HashMap::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn remove<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove_entry(key)
    }
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains_key(key)
    }
    pub fn clear(&mut self) {
        self.inner.clear()
    }
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        self.inner.entry(key)
    }
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.inner.insert(k, v)
    }
    pub fn keys(&self) -> Keys<'_, K, V> {
        self.inner.keys()
    }
    pub fn values(&self) -> Values<'_, K, V> {
        self.inner.values()
    }
}
impl<K: Eq + Hash, V> IntoIterator for HTable<K, V> {
    type Item = (K, V);
    type IntoIter = std::collections::hash_map::IntoIter<K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
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
        &self.blob.borrow()
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        &self.blob
    }
}

impl<K, V> FromIterator<(K, V)> for HTable<K, V>
where
    K: Eq + Hash,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        HTable {
            inner: HashMap::from_iter(iter),
        }
    }
}

/// A wrapper for `Bytes`
#[derive(Debug, PartialEq, Clone)]
pub struct Data {
    /// The blob of data
    blob: Bytes,
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
    /// Get the inner blob as an `u8` slice (coerced)
    pub fn get_inner_ref(&self) -> &[u8] {
        &self.blob
    }
}

impl Eq for Data {}
impl Hash for Data {
    fn hash<H>(&self, hasher: &mut H)
    where
        H: std::hash::Hasher,
    {
        self.blob.hash(hasher)
    }
}

impl<T> From<T> for Data
where
    T: Into<Bytes>,
{
    fn from(dat: T) -> Self {
        Self {
            blob: Bytes::from(dat.into()),
        }
    }
}

use serde::ser::{SerializeSeq, Serializer};

impl Serialize for Data {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.blob.len()))?;
        for e in self.blob.iter() {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

use serde::de::{Deserializer, SeqAccess, Visitor};

struct DataVisitor;
impl<'de> Visitor<'de> for DataVisitor {
    type Value = Data;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Expecting a coredb::htable::Data object")
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut bytes = Vec::new();
        while let Some(unsigned_8bit_int) = seq.next_element()? {
            bytes.push(unsigned_8bit_int);
        }
        Ok(Data::from_blob(Bytes::from(bytes)))
    }
}

impl<'de> Deserialize<'de> for Data {
    fn deserialize<D>(deserializer: D) -> Result<Data, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(DataVisitor)
    }
}

#[test]
fn test_de() {
    let mut x: HTable<String, Data> = HTable::new();
    x.insert(
        String::from("Sayan"),
        Data::from_string("is writing open-source code".to_owned()),
    );
    let ser = bincode::serialize(&x).unwrap();
    let de: HTable<String, Data> = bincode::deserialize(&ser).unwrap();
    assert_eq!(de, x);
    let mut hmap: HTable<Data, Data> = HTable::new();
    hmap.insert(Data::from("sayan"), Data::from("writes code"));
    assert!(hmap.get("sayan".as_bytes()).is_some());
}
