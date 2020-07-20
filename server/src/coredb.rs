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

use corelib::terrapipe::QueryDataframe;
use corelib::terrapipe::{responses, tags, ActionType, RespBytes, RespCodes, ResponseBuilder};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, RwLock};

pub type DbResult<T> = Result<T, RespCodes>;

pub struct CoreDB {
    shared: Arc<Coretable>,
}

pub struct Coretable {
    coremap: RwLock<HashMap<String, String>>,
}

impl Coretable {
    pub fn get(&self, key: &str) -> DbResult<String> {
        if let Some(value) = self.coremap.read().unwrap().get(key) {
            Ok(value.to_string())
        } else {
            Err(RespCodes::NotFound)
        }
    }
    pub fn set(&self, key: &str, value: &str) -> DbResult<()> {
        match self.coremap.write().unwrap().entry(key.to_string()) {
            Entry::Occupied(_) => return Err(RespCodes::OverwriteError),
            Entry::Vacant(e) => {
                let _ = e.insert(value.to_string());
                Ok(())
            }
        }
    }
    pub fn update(&self, key: &str, value: &str) -> DbResult<()> {
        match self.coremap.write().unwrap().entry(key.to_string()) {
            Entry::Occupied(ref mut e) => {
                e.insert(value.to_string());
                Ok(())
            }
            Entry::Vacant(_) => Err(RespCodes::NotFound),
        }
    }
    pub fn del(&self, key: &str) -> DbResult<()> {
        if let Some(_) = self.coremap.write().unwrap().remove(&key.to_owned()) {
            Ok(())
        } else {
            Err(RespCodes::NotFound)
        }
    }
    #[cfg(Debug)]
    pub fn print_debug_table(&self) {
        println!("{:#?}", *self.coremap.read().unwrap());
    }

    pub fn execute_query(&self, df: QueryDataframe) -> Vec<u8> {
        match df.actiontype {
            ActionType::Simple => self.execute_simple(df.data),
            // TODO(@ohsayan): Pipeline commands haven't been implemented yet
            ActionType::Pipeline => unimplemented!(),
        }
    }
    pub fn execute_simple(&self, buf: Vec<String>) -> Vec<u8> {
        let mut buf = buf.into_iter();
        while let Some(token) = buf.next() {
            match token.to_uppercase().as_str() {
                tags::TAG_GET => {
                    // This is a GET request
                    if let Some(key) = buf.next() {
                        if buf.next().is_none() {
                            let res = match self.get(&key.to_string()) {
                                Ok(v) => v,
                                Err(e) => return e.into_response(),
                            };
                            let mut resp =
                                ResponseBuilder::new_simple(RespCodes::EmptyResponseOkay);
                            resp.add_data(res.to_owned());
                            return resp.into_response();
                        }
                    }
                }
                tags::TAG_SET => {
                    // This is a SET request
                    if let Some(key) = buf.next() {
                        if let Some(value) = buf.next() {
                            if buf.next().is_none() {
                                match self.set(&key.to_string(), &value.to_string()) {
                                    Ok(_) => {
                                        #[cfg(Debug)]
                                        self.print_debug_table();
                                        return RespCodes::EmptyResponseOkay.into_response();
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

                                            RespCodes::EmptyResponseOkay.into_response()
                                        }
                                    }
                                    Err(e) => return e.into_response(),
                                }
                            }
                        }
                    }
                }
                tags::TAG_DEL => {
                    // This is a GET request
                    if let Some(key) = buf.next() {
                        if buf.next().is_none() {
                            match self.del(&key.to_string()) {
                                Ok(_) => {
                                    #[cfg(Debug)]
                                    self.print_debug_table();

                                    return RespCodes::EmptyResponseOkay.into_response();
                                }
                                Err(e) => return e.into_response(),
                            }
                        }
                    }
                }
                tags::TAG_HEYA => {
                    let mut resp = ResponseBuilder::new_simple(RespCodes::EmptyResponseOkay);
                    resp.add_data("HEY!".to_owned());
                    return resp.into_response();
                }
                _ => return RespCodes::OtherError("Unknown command".to_owned()).into_response(),
            }
        }
        RespCodes::InvalidMetaframe.into_response()
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
