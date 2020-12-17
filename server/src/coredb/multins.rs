/*
 * Created on Thu Dec 17 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

use super::Data;
use bytes::Bytes;
use libtdb::TResult;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::hint::unreachable_unchecked;
use std::sync::Arc;

/// The default namespace, which is called `DEF`
pub const DEF_NS: &'static str = "DEF";

/// The Database object holds multiple namespaces
///
/// Do note: The Database doesn't 'own' the namespaces, but only holds atomic references
/// to them.
pub struct Database {
    /// The namespaces which are kept in-memory
    ///
    /// Every namespace ID is given by a `String` and on hashing, this points
    /// us to an atomic reference of the corresponding `HashMap`
    namespaces: HashMap<String, Arc<Namespace>>,
    /// The current namespace
    current: Arc<Namespace>,
}

type Namespace = RwLock<HashMap<String, Data>>;

impl Database {
    /// Create a new `Database` object from existing namespace data
    pub fn new_from_existing(namespaces: HashMap<String, Arc<Namespace>>) -> TResult<Self> {
        if !namespaces.contains_key(DEF_NS) {
            return Err("Couldn't find default namespace!".into());
        } else {
            let current = namespaces
                .get(DEF_NS)
                .map(|hmap_ref| hmap_ref.clone())
                .unwrap_or_else(|| unsafe { unreachable_unchecked() });
            Ok(Database {
                namespaces,
                current,
            })
        }
    }
    /// # Create a 'fresh' `Database`
    ///
    /// This function creates a new `Database` object and initializes it with a default,
    /// empty namespace and sets it as the current namespace.
    pub fn new() -> Self {
        let mut namespaces = HashMap::new();
        let def_hm = Arc::new(RwLock::new(HashMap::<String, Data>::new()));
        let current = def_hm.clone();
        if namespaces.insert(DEF_NS.to_owned(), def_hm).is_some() {
            unsafe { unreachable_unchecked() }
        }
        Database {
            namespaces,
            current,
        }
    }
    /// Get an atomic reference to the current namespace
    pub fn get_current_ref(&self) -> Arc<Namespace> {
        self.current.clone()
    }
    /// Get a mutable reference to the current namespace
    pub fn get_mutable_ref(&self) -> RwLockWriteGuard<'_, HashMap<String, Data>> {
        self.current.write()
    }
    /// Get an immutable reference to the current namespace
    pub fn get_ref(&self) -> RwLockReadGuard<'_, HashMap<String, Data>> {
        self.current.read()
    }
    /// # Switch namespaces
    ///
    /// This function switches namespaces, provided that the namespace **exists**. If it doesn't
    /// exist, we simply return `false`. Otherwise, we'll return `true`.
    pub fn switch_namespace(&mut self, ks_id: String) -> bool {
        if let Some(namespace) = self.namespaces.get(&ks_id) {
            self.current = namespace.clone();
            true
        } else {
            false
        }
    }
    /// # Create a new namespace
    ///
    /// If this namespace didn't exist, a new one is created and added to set of namespaces held
    /// by the `Database` object; `true` is returned. However, if it did exist, then we return `false`.
    ///
    /// **Note:** This doesn't switch the current namespace; If you need to create a namespace and switch to it
    /// then use the `create_namespace_and_switch()` member function instead.
    pub fn create_namespace(&mut self, ks_id: String) -> bool {
        if self.namespaces.contains_key(&ks_id) {
            false
        } else {
            if self
                .namespaces
                .insert(ks_id, Arc::new(RwLock::new(HashMap::new())))
                .is_some()
            {
                unsafe {
                    // There's no way we can reach this, since we already know that nothing is going to be returned
                    unreachable_unchecked()
                }
            }
            true
        }
    }
    /// # Create a namespace and switch to it
    ///
    /// This function creates a new namespace and sets the current namespace to the newly
    /// created namespace. It will return `true` if this succeeded and `false` if the namespace
    /// already existed.
    ///
    pub fn create_namespace_and_switch(&mut self, ks_id: String) -> bool {
        if self.namespaces.contains_key(&ks_id) {
            false
        } else {
            let ns = Arc::new(RwLock::new(HashMap::new()));
            let ns_current = ns.clone();
            if self.namespaces.insert(ks_id, ns).is_some() {
                // There's no way on earth that will take us here!
                unsafe { unreachable_unchecked() }
            }
            self.current = ns_current;
            true
        }
    }
    /// Return the number of namespaces
    pub fn ns_count(&self) -> usize {
        self.namespaces.len()
    }
}

#[test]
fn test_database_empty_ns() {
    let database = Database::new();
    assert!(database.ns_count() == 1);
    assert!(database
        .get_mutable_ref()
        .insert(
            String::from("sayan"),
            Data::from_string(String::from("is writing code")),
        )
        .is_none());
}
