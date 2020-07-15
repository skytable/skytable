/*
 * Created on Mon Jul 13 2020
 *
 * This file is a part of the source code for the Terrabase database
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

use libcore::terrapipe::ResponseCodes;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, RwLock};

pub struct CoreDB {
    shared: Arc<Coretable>,
}

pub struct Coretable {
    coremap: RwLock<HashMap<String, String>>,
}

impl Coretable {
    pub fn get(&self, key: &str) -> Result<String, ResponseCodes> {
        if let Some(value) = self.coremap.read().unwrap().get(key) {
            Ok(value.to_string())
        } else {
            Err(ResponseCodes::NotFound)
        }
    }
    pub fn set(&self, key: &str, value: &str) -> Result<(), ResponseCodes> {
        match self.coremap.write().unwrap().entry(key.to_string()) {
            Entry::Occupied(_) => return Err(ResponseCodes::OverwriteError),
            Entry::Vacant(e) => {
                let _ = e.insert(value.to_string());
                Ok(())
            }
        }
    }
    pub fn update(&self, key: &str, value: &str) -> Result<(), ResponseCodes> {
        match self.coremap.write().unwrap().entry(key.to_string()) {
            Entry::Occupied(ref mut e) => {
                e.insert(value.to_string());
                Ok(())
            }
            Entry::Vacant(_) => Err(ResponseCodes::NotFound),
        }
    }
    pub fn del(&self, key: &str) -> Result<(), ResponseCodes> {
        if let Some(_) = self.coremap.write().unwrap().remove(&key.to_owned()) {
            Ok(())
        } else {
            Err(ResponseCodes::NotFound)
        }
    }
    pub fn print_debug_table(&self) {
        println!("{:#?}", *self.coremap.read().unwrap());
    }
}

impl CoreDB {
    pub fn new() -> Self {
        CoreDB {
            shared: Arc::new(Coretable {
                coremap: RwLock::new(HashMap::new()),
            }),
        }
    }
    pub fn get_handle(&self) -> Arc<Coretable> {
        Arc::clone(&self.shared)
    }
}
