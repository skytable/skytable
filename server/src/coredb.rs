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

use crate::protocol::Query;
use bincode;
use corelib::terrapipe::{tags, ActionType, RespBytes, RespCodes, ResponseBuilder};
use corelib::TResult;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{hash_map::Entry, HashMap};
use std::fs;
use std::io::{ErrorKind, Write};
use std::sync::Arc;

/// Results from actions on the Database
pub type ActionResult<T> = Result<T, RespCodes>;

/// This is a thread-safe database handle, which on cloning simply
/// gives another atomic reference to the `Coretable`
#[derive(Debug, Clone)]
pub struct CoreDB {
    shared: Arc<Coretable>,
    terminate: bool,
}

/// The `Coretable` holds all the key-value pairs in a `HashMap`
/// wrapped in a Read/Write lock
#[derive(Debug)]
pub struct Coretable {
    coremap: RwLock<HashMap<String, String>>,
}

impl CoreDB {
    /// GET a `key`
    pub fn get(&self, key: &str) -> ActionResult<String> {
        if let Some(value) = self.acquire_read().get(key) {
            Ok(value.to_string())
        } else {
            Err(RespCodes::NotFound)
        }
    }
    /// SET a `key` to `value`
    pub fn set(&self, key: &str, value: &str) -> ActionResult<()> {
        match self.acquire_write().entry(key.to_string()) {
            Entry::Occupied(_) => return Err(RespCodes::OverwriteError),
            Entry::Vacant(e) => {
                let _ = e.insert(value.to_string());
                Ok(())
            }
        }
    }
    /// UPDATE a `key` to `value`
    pub fn update(&self, key: &str, value: &str) -> ActionResult<()> {
        match self.acquire_write().entry(key.to_string()) {
            Entry::Occupied(ref mut e) => {
                e.insert(value.to_string());
                Ok(())
            }
            Entry::Vacant(_) => Err(RespCodes::NotFound),
        }
    }
    /// DEL a `key`
    pub fn del(&self, key: &str) -> ActionResult<()> {
        if let Some(_) = self.acquire_write().remove(&key.to_owned()) {
            Ok(())
        } else {
            Err(RespCodes::NotFound)
        }
    }
    #[cfg(Debug)]
    /// Flush the coretable entries when in debug mode
    pub fn print_debug_table(&self) {
        println!("{:#?}", *self.coremap.read().unwrap());
    }

    /// Execute a query that has already been validated by `Connection::read_query`
    pub fn execute_query(&self, df: Query) -> Vec<u8> {
        match df.actiontype {
            ActionType::Simple => self.execute_simple(df.data),
            // TODO(@ohsayan): Pipeline commands haven't been implemented yet
            ActionType::Pipeline => unimplemented!(),
        }
    }

    /// Execute a simple(*) query
    pub fn execute_simple(&self, buf: Vec<String>) -> Vec<u8> {
        let mut buf = buf.into_iter();
        while let Some(token) = buf.next() {
            match token.to_uppercase().as_str() {
                tags::TAG_GET => {
                    // This is a GET query
                    if let Some(key) = buf.next() {
                        if buf.next().is_none() {
                            let res = match self.get(&key.to_string()) {
                                Ok(v) => v,
                                Err(e) => return e.into_response(),
                            };
                            let mut resp = ResponseBuilder::new_simple(RespCodes::Okay);
                            resp.add_data(res.to_owned());
                            return resp.into_response();
                        }
                    }
                }
                tags::TAG_SET => {
                    // This is a SET query
                    if let Some(key) = buf.next() {
                        if let Some(value) = buf.next() {
                            if buf.next().is_none() {
                                match self.set(&key.to_string(), &value.to_string()) {
                                    Ok(_) => {
                                        #[cfg(Debug)]
                                        self.print_debug_table();
                                        return RespCodes::Okay.into_response();
                                    }
                                    Err(e) => return e.into_response(),
                                }
                            }
                        }
                    }
                }
                tags::TAG_UPDATE => {
                    // This is an UPDATE query
                    if let Some(key) = buf.next() {
                        if let Some(value) = buf.next() {
                            if buf.next().is_none() {
                                match self.update(&key.to_string(), &value.to_string()) {
                                    Ok(_) => {
                                        return {
                                            #[cfg(Debug)]
                                            self.print_debug_table();

                                            RespCodes::Okay.into_response()
                                        }
                                    }
                                    Err(e) => return e.into_response(),
                                }
                            }
                        }
                    }
                }
                tags::TAG_DEL => {
                    // This is a DEL query
                    if let Some(key) = buf.next() {
                        if buf.next().is_none() {
                            match self.del(&key.to_string()) {
                                Ok(_) => {
                                    #[cfg(Debug)]
                                    self.print_debug_table();

                                    return RespCodes::Okay.into_response();
                                }
                                Err(e) => return e.into_response(),
                            }
                        } else {
                        }
                    }
                }
                tags::TAG_HEYA => {
                    if buf.next().is_none() {
                        let mut resp = ResponseBuilder::new_simple(RespCodes::Okay);
                        resp.add_data("HEY!".to_owned());
                        return resp.into_response();
                    }
                }
                _ => {
                    return RespCodes::OtherError(Some("Unknown command".to_owned()))
                        .into_response()
                }
            }
        }
        RespCodes::ArgumentError.into_response()
    }
    /// Create a new `CoreDB` instance
    ///
    /// This also checks if a local backup of previously saved data is available.
    /// If it is - it restores the data. Otherwise it creates a new in-memory table
    pub fn new() -> TResult<Self> {
        let coretable = CoreDB::get_saved()?;
        if let Some(coretable) = coretable {
            Ok(CoreDB {
                shared: Arc::new(Coretable {
                    coremap: RwLock::new(coretable),
                }),
                terminate: false,
            })
        } else {
            Ok(CoreDB {
                shared: Arc::new(Coretable {
                    coremap: RwLock::new(HashMap::new()),
                }),
                terminate: false,
            })
        }
    }
    /// Acquire a write lock
    fn acquire_write(&self) -> RwLockWriteGuard<'_, HashMap<String, String>> {
        self.shared.coremap.write()
    }
    /// Acquire a read lock
    fn acquire_read(&self) -> RwLockReadGuard<'_, HashMap<String, String>> {
        self.shared.coremap.read()
    }
    /// Flush the contents of the in-memory table onto disk
    pub fn flush_db(&self) -> TResult<()> {
        let encoded = bincode::serialize(&*self.acquire_read())?;
        let mut file = fs::File::create("./data.bin")?;
        file.write_all(&encoded)?;
        Ok(())
    }
    /// Try to get the saved data from disk
    pub fn get_saved() -> TResult<Option<HashMap<String, String>>> {
        let file = match fs::read("./data.bin") {
            Ok(f) => f,
            Err(e) => match e.kind() {
                ErrorKind::NotFound => return Ok(None),
                _ => return Err("Couldn't read flushed data from disk".into()),
            },
        };
        let parsed: HashMap<String, String> = bincode::deserialize(&file)?;
        Ok(Some(parsed))
    }
}

impl Drop for CoreDB {
    // This prevents us from killing the database, in the event someone tries
    // to access it
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == 1 {
            // Acquire a lock to prevent anyone from writing something
            let coremap = self.shared.coremap.write();
            self.terminate = true;
            drop(coremap);
        }
    }
}
