/*
 * Created on Fri Sep 17 2021
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

use crate::corestore::table::DataModel;
use crate::corestore::Data;
use crate::dbnet::connection::prelude::*;
use crate::resp::writer;
use crate::resp::writer::TypedArrayWriter;

const LEN: &[u8] = "LEN".as_bytes();
const LIMIT: &[u8] = "LIMIT".as_bytes();
const VALUEAT: &[u8] = "VALUEAT".as_bytes();
const LAST: &[u8] = "LAST".as_bytes();
const FIRST: &[u8] = "FIRST".as_bytes();
const RANGE: &[u8] = "RANGE".as_bytes();

struct Range {
    start: usize,
    stop: Option<usize>,
}

impl Range {
    pub fn new(start: usize) -> Self {
        Self { start, stop: None }
    }
    pub fn set_stop(&mut self, stop: usize) {
        self.stop = Some(stop);
    }
    pub fn into_vec(self, slice: &[Data]) -> Option<Vec<Data>> {
        match slice.get(self.start..self.stop.unwrap_or(slice.len())) {
            Some(slc) => Some(slc.iter().cloned().collect()),
            None => None,
        }
    }
}

action! {
    /// Handle an `LGET` query for the list model (KVExt)
    /// ## Syntax
    /// - `LGET <mylist>` will return the full list
    /// - `LGET <mylist> LEN` will return the length of the list
    /// - `LGET <mylist> LIMIT <limit>` will return a maximum of `limit` elements
    /// - `LGET <mylist> VALUEAT <index>` will return the value at the provided index
    /// - `LGET <mylist> FIRST` will return the first item
    /// - `LGET <mylist> LAST` will return the last item
    /// if it exists
    fn lget(handle: &Corestore, con: &mut T, mut act: ActionIter<'a>) {
        err_if_len_is!(act, con, lt 1);
        let table = get_tbl!(handle, con);
        let listmap = listmap!(table, con);
        // get the list name
        let listname = unsafe { act.next_unchecked() };
        // now let us see what we need to do
        macro_rules! get_numeric_count {
            () => {
                match unsafe { String::from_utf8_lossy(act.next_unchecked()) }.parse::<usize>() {
                    Ok(int) => int,
                    Err(_) => return conwrite!(con, groups::WRONGTYPE_ERR),
                }
            };
        }
        match act.next_uppercase().as_ref() {
            None => {
                // just return everything in the list
                let items: Vec<Data> = if let Some(list) = listmap.get(listname) {
                    list.value().read().iter().cloned().collect()
                } else {
                    return conwrite!(con, groups::NIL);
                };
                writelist!(con, listmap, items);
            }
            Some(subaction) => {
                match subaction.as_ref() {
                    LEN => {
                        err_if_len_is!(act, con, not 0);
                        if let Some(len) = listmap.len_of(listname) {
                            conwrite!(con, len)?;
                        } else {
                            conwrite!(con, groups::NIL)?;
                        }
                    }
                    LIMIT => {
                        err_if_len_is!(act, con, not 1);
                        let count = get_numeric_count!();
                        let items = if let Some(keys) = listmap.get_cloned(listname, count) {
                            keys
                        } else {
                            return conwrite!(con, groups::NIL);
                        };
                        writelist!(con, listmap, items);
                    }
                    VALUEAT => {
                        err_if_len_is!(act, con, not 1);
                        let idx = get_numeric_count!();
                        let maybe_value = listmap.get(listname).map(|list| {
                            let readlist = list.read();
                            let get = readlist.get(idx).cloned();
                            get
                        });
                        match maybe_value {
                            Some(Some(value)) => {
                                unsafe {
                                    // tsymbol is verified
                                    writer::write_raw_mono(con, listmap.get_payload_tsymbol(), &value)
                                        .await?;
                                }
                            }
                            Some(None) => {
                                // bad index
                                conwrite!(con, groups::LISTMAP_BAD_INDEX)?;
                            }
                            None => {
                                // not found
                                conwrite!(con, groups::NIL)?;
                            }
                        }
                    }
                    LAST => {
                        err_if_len_is!(act, con, not 0);
                        let maybe_value = listmap.get(listname).map(|list| {
                            list.read().last().cloned()
                        });
                        match maybe_value {
                            Some(Some(value)) => {
                                unsafe {
                                    writer::write_raw_mono(con, listmap.get_payload_tsymbol(), &value).await?;
                                }
                            },
                            Some(None) => conwrite!(con, groups::LISTMAP_LIST_IS_EMPTY)?,
                            None => conwrite!(con, groups::NIL)?,
                        }
                    }
                    FIRST => {
                        err_if_len_is!(act, con, not 0);
                        let maybe_value = listmap.get(listname).map(|list| {
                            list.read().first().cloned()
                        });
                        match maybe_value {
                            Some(Some(value)) => {
                                unsafe {
                                    writer::write_raw_mono(con, listmap.get_payload_tsymbol(), &value).await?;
                                }
                            },
                            Some(None) => conwrite!(con, groups::LISTMAP_LIST_IS_EMPTY)?,
                            None => conwrite!(con, groups::NIL)?,
                        }
                    }
                    RANGE => {
                        match act.next_string_owned() {
                            Some(start) => {
                                let start: usize = match start.parse() {
                                    Ok(v) => v,
                                    Err(_) => return conwrite!(con, groups::WRONGTYPE_ERR),
                                };
                                let mut range = Range::new(start);
                                if let Some(stop) = act.next_string_owned() {
                                    let stop: usize = match stop.parse() {
                                        Ok(v) => v,
                                        Err(_) => return conwrite!(con, groups::WRONGTYPE_ERR),
                                    };
                                    range.set_stop(stop);
                                };
                                match listmap.get(listname) {
                                    Some(list) => {
                                        let ret = range.into_vec(&list.read());
                                        match ret {
                                            Some(ret) => {
                                                writelist!(con, listmap, ret);
                                            },
                                            None => conwrite!(con, groups::LISTMAP_BAD_INDEX)?,
                                        }
                                    }
                                    None => conwrite!(con, groups::NIL)?,
                                }
                            }
                            None => conwrite!(con, groups::ACTION_ERR)?,
                        }
                    }
                    _ => conwrite!(con, groups::UNKNOWN_ACTION)?,
                }
            }
        }
        Ok(())
    }
}
