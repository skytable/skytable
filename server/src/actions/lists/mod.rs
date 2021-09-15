/*
 * Created on Tue Sep 07 2021
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

use crate::corestore::booltable::BytesBoolTable;
use crate::corestore::booltable::BytesNicheLUT;
use crate::corestore::table::DataModel;
use crate::corestore::Data;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::KVTable;
use crate::kvengine::{encoding::ENCODING_LUT, listmap::LockedVec};
use crate::resp::writer;
use crate::resp::writer::TypedArrayWriter;
use crate::util::compiler;

const LEN: &[u8] = "LEN".as_bytes();
const LIMIT: &[u8] = "LIMIT".as_bytes();
const VALUEAT: &[u8] = "VALUEAT".as_bytes();
const CLEAR: &[u8] = "CLEAR".as_bytes();
const PUSH: &[u8] = "PUSH".as_bytes();
const REMOVE: &[u8] = "REMOVE".as_bytes();
const INSERT: &[u8] = "INSERT".as_bytes();
const POP: &[u8] = "POP".as_bytes();

const OKAY_OVW_BLUT: BytesBoolTable = BytesBoolTable::new(groups::OKAY, groups::OVERWRITE_ERR);
const OKAY_BADIDX_NIL_NLUT: BytesNicheLUT =
    BytesNicheLUT::new(groups::NIL, groups::OKAY, groups::LISTMAP_BAD_INDEX);

macro_rules! listmap {
    ($tbl:expr, $con:expr) => {
        match $tbl.get_model_ref() {
            DataModel::KVExtListmap(lm) => lm,
            _ => return conwrite!($con, groups::WRONG_MODEL),
        }
    };
}

macro_rules! writelist {
    ($con:expr, $listmap:expr, $items:expr) => {
        let mut typed_array_writer =
            unsafe { TypedArrayWriter::new($con, $listmap.get_payload_tsymbol(), $items.len()) }
                .await?;
        for item in $items {
            typed_array_writer.write_element(item).await?;
        }
    };
}

action! {
    /// Handle an `LSET` query for the list model
    /// Syntax: `LSET <listname> <values ...>`
    fn lset(handle: &Corestore, con: &mut T, mut act: ActionIter<'a>) {
        err_if_len_is!(act, con, lt 1);
        let table = get_tbl!(handle, con);
        let listmap = listmap!(table, con);
        let listname = unsafe { act.next_unchecked_bytes() };
        let list = listmap.kve_inner_ref();
        if registry::state_okay() {
            let did = if let Some(entry) = list.fresh_entry(listname.into()) {
                let v: Vec<Data> = act.map(Data::copy_from_slice).collect();
                entry.insert(LockedVec::new(v));
                true
            } else {
                false
            };
            conwrite!(con, OKAY_OVW_BLUT[did])?;
        } else {
            conwrite!(con, groups::SERVER_ERR)?;
        }
        Ok(())
    }

    /// Handle an `LGET` query for the list model (KVExt)
    /// ## Syntax
    /// - `LGET <mylist>` will return the full list
    /// - `LGET <mylist> LEN` will return the length of the list
    /// - `LGET <mylist> LIMIT <limit>` will return a maximum of `limit` elements
    /// - `LGET <mylist> VALUEAT <index>` will return the value at the provided index
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
                                    writer::write_raw_mono(con, listmap.get_payload_tsymbol(), &value).await?;
                                }
                            },
                            Some(None) => {
                                // bad index
                                conwrite!(con, groups::LISTMAP_BAD_INDEX)?;
                            },
                            None => {
                                // not found
                                conwrite!(con, groups::NIL)?;
                            }
                        }
                    }
                    _ => conwrite!(con, groups::UNKNOWN_ACTION)?,
                }
            }
        }
        Ok(())
    }

    /// Handle `LMOD` queries
    /// ## Syntax
    /// - `LMOD <mylist> push <value>`
    /// - `LMOD <mylist> pop <optional idx>`
    /// - `LMOD <mylist> insert <index> <value>`
    /// - `LMOD <mylist> remove <index>`
    /// - `LMOD <mylist> clear`
    fn lmod(handle: &Corestore, con: &mut T, mut act: ActionIter<'a>) {
        err_if_len_is!(act, con, lt 2);
        let table = get_tbl!(handle, con);
        let listmap = listmap!(table, con);
        // get the list name
        let listname = unsafe { act.next_unchecked() };
        macro_rules! get_numeric_count {
            () => {
                match unsafe { String::from_utf8_lossy(act.next_unchecked()) }.parse::<usize>() {
                    Ok(int) => int,
                    Err(_) => return conwrite!(con, groups::WRONGTYPE_ERR),
                }
            };
        }
        // now let us see what we need to do
        match unsafe { act.next_uppercase_unchecked() }.as_ref() {
            CLEAR => {
                err_if_len_is!(act, con, not 0);
                let list = match listmap.kve_inner_ref().get(listname) {
                    Some(l) => l,
                    _ => return conwrite!(con, groups::NIL),
                };
                let okay = if registry::state_okay() {
                    list.write().clear();
                    true
                } else {
                    false
                };
                conwrite!(con, OKAY_OVW_BLUT[okay])?;
            }
            PUSH => {
                err_if_len_is!(act, con, not 1);
                let list = match listmap.kve_inner_ref().get(listname) {
                    Some(l) => l,
                    _ => return conwrite!(con, groups::NIL),
                };
                let bts = unsafe { act.next_unchecked() };
                let ret = if compiler::likely(ENCODING_LUT[listmap.kve_payload_encoded()](bts)) {
                    if registry::state_okay() {
                        // good to try and insert
                        list.write().push(Data::copy_from_slice(bts));
                        groups::OKAY
                    } else {
                        // server broken; server err
                        groups::SERVER_ERR
                    }
                } else {
                    // encoding failed; uh
                    groups::ENCODING_ERROR
                };
                conwrite!(con, ret)?;
            }
            REMOVE => {
                err_if_len_is!(act, con, not 1);
                let idx_to_remove = get_numeric_count!();
                if registry::state_okay() {
                    let maybe_value = listmap.kve_inner_ref().get(listname).map(|list| {
                        let mut wlock = list.write();
                        if idx_to_remove < wlock.len() {
                            wlock.remove(idx_to_remove);
                            true
                        } else {
                            false
                        }
                    });
                    conwrite!(con, OKAY_BADIDX_NIL_NLUT[maybe_value])?;
                } else {
                    conwrite!(con, groups::SERVER_ERR)?;
                }
            }
            INSERT => {
                err_if_len_is!(act, con, not 2);
                let idx_to_insert_at = get_numeric_count!();
                let bts = unsafe { act.next_unchecked() };
                let ret = if compiler::likely(ENCODING_LUT[listmap.kve_payload_encoded()](bts)) {
                    if registry::state_okay() {
                        // okay state, good to insert
                        let maybe_insert = listmap.get(listname).map(|list| {
                            let mut wlock = list.write();
                            if idx_to_insert_at < wlock.len() {
                                // we can insert
                                wlock.insert(idx_to_insert_at, Data::copy_from_slice(bts));
                                true
                            } else {
                                // oops, out of bounds
                                false
                            }
                        });
                        OKAY_BADIDX_NIL_NLUT[maybe_insert]
                    } else {
                        // flush broken; server err
                        groups::SERVER_ERR
                    }
                } else {
                    // encoding failed, uh
                    groups::ENCODING_ERROR
                };
                conwrite!(con, ret)?;
            }
            POP => {
                err_if_len_is!(act, con, gt 1);
                let idx = if act.len() == 1 {
                    // we have an idx
                    Some(get_numeric_count!())
                } else {
                    // no idx
                    None
                };
                if registry::state_okay() {
                    let maybe_pop = listmap.get(listname).map(|list| {
                        let mut wlock = list.write();
                        if let Some(idx) = idx {
                            if idx < wlock.len() {
                                // so we can pop
                                Some(wlock.remove(idx))
                            } else {
                                None
                            }
                        } else {
                            wlock.pop()
                        }
                    });
                    match maybe_pop {
                        Some(Some(val)) => {
                            unsafe {
                                writer::write_raw_mono(con, listmap.get_payload_tsymbol(), &val).await?;
                            }
                        }
                        Some(None) => {
                            conwrite!(con, groups::LISTMAP_BAD_INDEX)?;
                        }
                        None => conwrite!(con, groups::NIL)?,
                    }
                } else {
                    conwrite!(con, groups::SERVER_ERR)?;
                }
            }
            _ => conwrite!(con, groups::UNKNOWN_ACTION)?,
        }
        Ok(())
    }
}
