/*
 * Created on Tue Aug 10 2021
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

use super::bref::RefMulti;
use super::LowMap;
use super::Skymap;
use core::mem;
use hashbrown::raw::RawIntoIter;
use hashbrown::raw::RawIter;
use parking_lot::RwLockReadGuard;
use std::collections::hash_map::RandomState;
use std::sync::Arc;

/// An owned iterator for a [`Skymap`]
pub struct OwnedIter<K, V, S = RandomState> {
    map: Skymap<K, V, S>,
    cs: usize,
    current: Option<RawIntoIter<(K, V)>>,
}

impl<K, V, S> OwnedIter<K, V, S> {
    pub fn new(map: Skymap<K, V, S>) -> Self {
        Self {
            map,
            cs: 0usize,
            current: None,
        }
    }
}

impl<K, V, S> Iterator for OwnedIter<K, V, S> {
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(bucket) = current.next() {
                    return Some(bucket);
                }
            }
            if self.cs == self.map.shards().len() {
                return None;
            }
            let mut wshard = unsafe { self.map.get_wshard_unchecked(self.cs) };
            // get the next map's iterator
            let current_map = mem::replace(&mut *wshard, LowMap::new());
            drop(wshard);
            let iter = current_map.into_iter();
            self.current = Some(iter);
            self.cs += 1;
        }
    }
}

unsafe impl<K: Send, V: Send, S> Send for OwnedIter<K, V, S> {}
unsafe impl<K: Sync, V: Sync, S> Sync for OwnedIter<K, V, S> {}

type BorrowedIterGroup<'a, K, V> = (RawIter<(K, V)>, Arc<RwLockReadGuard<'a, LowMap<K, V>>>);

/// A borrowed iterator for a [`Skymap`]
pub struct BorrowedIter<'a, K, V, S = ahash::RandomState> {
    map: &'a Skymap<K, V, S>,
    cs: usize,
    citer: Option<BorrowedIterGroup<'a, K, V>>,
}

impl<'a, K, V, S> BorrowedIter<'a, K, V, S> {
    pub const fn new(map: &'a Skymap<K, V, S>) -> Self {
        Self {
            map,
            cs: 0usize,
            citer: None,
        }
    }
}

impl<'a, K, V, S> Iterator for BorrowedIter<'a, K, V, S> {
    type Item = RefMulti<'a, K, V>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.citer.as_mut() {
                if let Some(bucket) = current.0.next() {
                    let (kptr, vptr) = unsafe {
                        // we know that this is valid, and this guarantee is
                        // provided to us by the lifetime
                        bucket.as_ref()
                    };
                    let guard = current.1.clone();
                    return Some(RefMulti::new(guard, kptr, vptr));
                }
            }
            if self.cs == self.map.shards().len() {
                // end of shards
                return None;
            }
            // warning: the rawiter allows us to terribly violate conditions
            // you can mutate!
            let rshard = unsafe { self.map.get_rshard_unchecked(self.cs) };
            let iter = unsafe {
                // same thing: our lt params ensure validity
                rshard.iter()
            };
            self.citer = Some((iter, Arc::new(rshard)));
            self.cs += 1;
        }
    }
}

unsafe impl<'a, K: Send, V: Send, S> Send for BorrowedIter<'a, K, V, S> {}
unsafe impl<'a, K: Sync, V: Sync, S> Sync for BorrowedIter<'a, K, V, S> {}

#[test]
fn test_iter() {
    let map = Skymap::default();
    map.insert("hello1", "world");
    map.insert("hello2", "world");
    map.insert("hello3", "world");
    let collected: Vec<(&str, &str)> = map.get_owned_iter().collect();
    assert!(collected.len() == 3);
    assert!(collected.contains(&("hello1", "world")));
    assert!(collected.contains(&("hello2", "world")));
    assert!(collected.contains(&("hello3", "world")));
}
