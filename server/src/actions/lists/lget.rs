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

use crate::{corestore::SharedSlice, dbnet::prelude::*};

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
    pub fn into_vec(self, slice: &[SharedSlice]) -> Option<Vec<SharedSlice>> {
        slice
            .get(self.start..self.stop.unwrap_or(slice.len()))
            .map(|slc| slc.to_vec())
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
    fn lget(handle: &Corestore, con: &mut Connection<C, P>, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len != 0)?;
        let listmap = handle.get_table_with::<P, KVEList>()?;
        // get the list name
        let listname = unsafe { act.next_unchecked() };
        // now let us see what we need to do
        macro_rules! get_numeric_count {
            () => {
                match unsafe { String::from_utf8_lossy(act.next_unchecked()) }.parse::<usize>() {
                    Ok(int) => int,
                    Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                }
            };
        }
        match act.next_uppercase().as_ref() {
            None => {
                // just return everything in the list
                let items = match listmap.list_cloned_full(listname) {
                    Ok(Some(list)) => list,
                    Ok(None) => return Err(P::RCODE_NIL.into()),
                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                };
                writelist!(con, listmap, items);
            }
            Some(subaction) => {
                match subaction.as_ref() {
                    LEN => {
                        ensure_length::<P>(act.len(), |len| len == 0)?;
                        match listmap.list_len(listname) {
                            Ok(Some(len)) => con.write_usize(len).await?,
                            Ok(None) => return Err(P::RCODE_NIL.into()),
                            Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                        }
                    }
                    LIMIT => {
                        ensure_length::<P>(act.len(), |len| len == 1)?;
                        let count = get_numeric_count!();
                        match listmap.list_cloned(listname, count) {
                            Ok(Some(items)) => writelist!(con, listmap, items),
                            Ok(None) => return Err(P::RCODE_NIL.into()),
                            Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                        }
                    }
                    VALUEAT => {
                        ensure_length::<P>(act.len(), |len| len == 1)?;
                        let idx = get_numeric_count!();
                        let maybe_value = listmap.get(listname).map(|list| {
                            list.map(|lst| lst.read().get(idx).cloned())
                        });
                        match maybe_value {
                            Ok(v) => match v {
                                Some(Some(value)) => {
                                    con.write_mono_length_prefixed_with_tsymbol(
                                        &value, listmap.get_value_tsymbol()
                                    ).await?;
                                }
                                Some(None) => {
                                    // bad index
                                    return Err(P::RSTRING_LISTMAP_BAD_INDEX.into());
                                }
                                None => {
                                    // not found
                                    return Err(P::RCODE_NIL.into());
                                }
                            }
                            Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                        }
                    }
                    LAST => {
                        ensure_length::<P>(act.len(), |len| len == 0)?;
                        let maybe_value = listmap.get(listname).map(|list| {
                            list.map(|lst| lst.read().last().cloned())
                        });
                        match maybe_value {
                            Ok(v) => match v {
                                Some(Some(value)) => {
                                    con.write_mono_length_prefixed_with_tsymbol(
                                        &value, listmap.get_value_tsymbol()
                                    ).await?;
                                },
                                Some(None) => return Err(P::RSTRING_LISTMAP_LIST_IS_EMPTY.into()),
                                None => return Err(P::RCODE_NIL.into()),
                            }
                            Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                        }
                    }
                    FIRST => {
                        ensure_length::<P>(act.len(), |len| len == 0)?;
                        let maybe_value = listmap.get(listname).map(|list| {
                            list.map(|lst| lst.read().first().cloned())
                        });
                        match maybe_value {
                            Ok(v) => match v {
                                Some(Some(value)) => {
                                    con.write_mono_length_prefixed_with_tsymbol(
                                        &value, listmap.get_value_tsymbol()
                                    ).await?;
                                },
                                Some(None) => return Err(P::RSTRING_LISTMAP_LIST_IS_EMPTY.into()),
                                None => return Err(P::RCODE_NIL.into()),
                            }
                            Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                        }
                    }
                    RANGE => {
                        match act.next_string_owned() {
                            Some(start) => {
                                let start: usize = match start.parse() {
                                    Ok(v) => v,
                                    Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                                };
                                let mut range = Range::new(start);
                                if let Some(stop) = act.next_string_owned() {
                                    let stop: usize = match stop.parse() {
                                        Ok(v) => v,
                                        Err(_) => return util::err(P::RCODE_WRONGTYPE_ERR),
                                    };
                                    range.set_stop(stop);
                                };
                                match listmap.get(listname) {
                                    Ok(Some(list)) => {
                                        let ret = range.into_vec(&list.read());
                                        match ret {
                                            Some(ret) => {
                                                writelist!(con, listmap, ret);
                                            },
                                            None => return Err(P::RSTRING_LISTMAP_BAD_INDEX.into()),
                                        }
                                    }
                                    Ok(None) => return Err(P::RCODE_NIL.into()),
                                    Err(()) => return Err(P::RCODE_ENCODING_ERROR.into()),
                                }
                            }
                            None => return Err(P::RCODE_ACTION_ERR.into()),
                        }
                    }
                    _ => return Err(P::RCODE_UNKNOWN_ACTION.into()),
                }
            }
        }
        Ok(())
    }
}
