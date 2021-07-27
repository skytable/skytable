/*
 * Created on Tue Jul 27 2021
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

use crate::corestore::lazy::Lazy;
use crate::corestore::memstore::DdlError;
use crate::corestore::memstore::ObjectID;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::encoding;
use core::str;
use regex::Regex;

const TABLE: &[u8] = "TABLE".as_bytes();
const KEYSPACE: &[u8] = "KEYSPACE".as_bytes();
const KEYMAP: &[u8] = "keymap".as_bytes();
const BINSTR: &[u8] = "binstr".as_bytes();
const STR: &[u8] = "str".as_bytes();

static VALID_CONTAINER_NAME: Lazy<Regex, fn() -> Regex> =
    Lazy::new(|| Regex::new("^[a-zA-Z_$][a-zA-Z_$0-9]*$").unwrap());

action!(
    /// Handle `create table <tableid> <model>(args)` and `create keyspace <ksid>`
    /// like queries
    fn create(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        // minlength is 2 (create has already been checked)
        err_if_len_is!(act, con, lt 2);
        let mut create_what = unsafe { act.next().unsafe_unwrap() }.to_vec();
        create_what.make_ascii_uppercase();
        match create_what.as_ref() {
            TABLE => create_table(handle, con, act).await?,
            KEYSPACE => create_keyspace(handle, con, act).await?,
            _ => {
                con.write_response(responses::groups::UNKNOWN_DDL_QUERY)
                    .await?;
            }
        }
        Ok(())
    }
);

action!(
    /// Handle `drop table <tableid>` and `drop keyspace <ksid>`
    /// like queries
    fn ddl_drop(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        // minlength is 2 (create has already been checked)
        err_if_len_is!(act, con, lt 2);
        let mut create_what = unsafe { act.next().unsafe_unwrap() }.to_vec();
        create_what.make_ascii_uppercase();
        match create_what.as_ref() {
            TABLE => drop_table(handle, con, act).await?,
            KEYSPACE => drop_keyspace(handle, con, act).await?,
            _ => {
                con.write_response(responses::groups::UNKNOWN_DDL_QUERY)
                    .await?;
            }
        }
        Ok(())
    }
);

pub(super) fn parse_table_args(mut act: ActionIter) -> Result<(ObjectID, u8), &'static [u8]> {
    let table_name = unsafe { act.next().unsafe_unwrap() };
    let model_name = unsafe { act.next().unsafe_unwrap() };
    if !encoding::is_utf8(&table_name) || !encoding::is_utf8(&model_name) {
        return Err(responses::groups::ENCODING_ERROR);
    }
    let table_name_str = unsafe { str::from_utf8_unchecked(&table_name) };
    let model_name_str = unsafe { str::from_utf8_unchecked(&model_name) };
    if !VALID_CONTAINER_NAME.is_match(table_name_str) {
        return Err(responses::groups::BAD_EXPRESSION);
    }
    let splits: Vec<&str> = model_name_str.split('(').collect();
    if splits.len() != 2 {
        return Err(responses::groups::BAD_EXPRESSION);
    }
    let model_name_split = unsafe { splits.get_unchecked(0) };
    let model_args_split = unsafe { splits.get_unchecked(1) };

    // model name has to have at least one char while model args should have
    // atleast `)` 1 chars (for example if the model takes no arguments: `smh()`)
    if model_name_split.is_empty() || model_args_split.is_empty() {
        return Err(responses::groups::BAD_EXPRESSION);
    }

    // THIS IS WHERE WE HANDLE THE NEWER MODELS
    if model_name_split.as_bytes() != KEYMAP {
        return Err(responses::groups::UNKNOWN_MODEL);
    }

    let non_bracketed_end = unsafe {
        *model_args_split
            .as_bytes()
            .get_unchecked(model_args_split.len() - 1)
            != b')'
    };

    if non_bracketed_end {
        return Err(responses::groups::BAD_EXPRESSION);
    }

    // should be (ty1, ty2)
    let model_args: Vec<&str> = model_args_split[..model_args_split.len() - 1]
        .split(',')
        .map(|v| v.trim())
        .collect();
    if model_args.len() != 2 {
        // nope, someone had fun with commas or they added more args
        // let's check if it was comma fun or if it was arg fun
        let all_nonzero = model_args.into_iter().all(|v| !v.is_empty());
        if all_nonzero {
            // arg fun
            return Err(responses::groups::TOO_MANY_ARGUMENTS);
        } else {
            // comma fun
            return Err(responses::groups::BAD_EXPRESSION);
        }
    }
    let key_ty = unsafe { model_args.get_unchecked(0) };
    let val_ty = unsafe { model_args.get_unchecked(1) };
    if !VALID_CONTAINER_NAME.is_match(key_ty) || !VALID_CONTAINER_NAME.is_match(val_ty) {
        return Err(responses::groups::BAD_EXPRESSION);
    }
    let key_ty = key_ty.as_bytes();
    let val_ty = val_ty.as_bytes();
    let model_code: u8 = match (key_ty, val_ty) {
        (BINSTR, BINSTR) => 0,
        (BINSTR, STR) => 1,
        (STR, STR) => 2,
        (STR, BINSTR) => 3,
        _ => return Err(responses::groups::UNKNOWN_DATA_TYPE),
    };

    if table_name_str.len() > 64 {
        return Err(responses::groups::CONTAINER_NAME_TOO_LONG);
    }

    Ok((unsafe { ObjectID::from_slice(table_name_str) }, model_code))
}

action!(
    /// We should have `<tableid> <model>(args)`
    fn create_table(handle: &Corestore, con: &mut T, act: ActionIter) {
        err_if_len_is!(act, con, not 2);
        let (table_name, model_code) = match parse_table_args(act) {
            Ok(v) => v,
            Err(e) => return con.write_response(e).await,
        };
        match handle.create_table(table_name, model_code, false) {
            Ok(_) => con.write_response(responses::groups::OKAY).await?,
            Err(DdlError::AlreadyExists) => {
                con.write_response(responses::groups::ALREADY_EXISTS)
                    .await?;
            }
            Err(DdlError::WrongModel) => unsafe {
                // we have already checked the model ourselves
                impossible!()
            },
            Err(DdlError::DefaultNotFound) => {
                con.write_response(responses::groups::DEFAULT_UNSET).await?
            }
            Err(_) => unsafe {
                // we know that Corestore::create_table won't return anything else
                impossible!()
            },
        }
        Ok(())
    }
);

action!(
    /// We should have `<ksid>`
    fn create_keyspace(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        err_if_len_is!(act, con, not 1);
        match act.next() {
            Some(ksid) => {
                if !encoding::is_utf8(&ksid) {
                    return con.write_response(responses::groups::ENCODING_ERROR).await;
                }
                let ksid_str = unsafe { str::from_utf8_unchecked(&ksid) };
                if !VALID_CONTAINER_NAME.is_match(ksid_str) {
                    return con.write_response(responses::groups::BAD_EXPRESSION).await;
                }
                if ksid.len() > 64 {
                    return con
                        .write_response(responses::groups::CONTAINER_NAME_TOO_LONG)
                        .await;
                }
                let ksid = unsafe { ObjectID::from_slice(ksid_str) };
                match handle.create_keyspace(ksid) {
                    Ok(()) => return con.write_response(responses::groups::OKAY).await,
                    Err(DdlError::AlreadyExists) => {
                        return con.write_response(responses::groups::ALREADY_EXISTS).await
                    }
                    Err(_) => unsafe {
                        // we already know that Corestore::create_keyspace doesn't return anything else
                        impossible!()
                    },
                }
            }
            None => return con.write_response(responses::groups::ACTION_ERR).await,
        }
    }
);

action! {
    /// Drop a table (`<tblid>` only)
    fn drop_table(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        match act.next() {
            Some(tbl) => {
                if tbl.len() > 64 {
                    return con.write_response(responses::groups::CONTAINER_NAME_TOO_LONG).await;
                }
                let ret = match handle.drop_table(unsafe {ObjectID::from_slice(tbl)}) {
                    Ok(()) => responses::groups::OKAY,
                    Err(DdlError::DefaultNotFound) => responses::groups::DEFAULT_UNSET,
                    Err(DdlError::ProtectedObject) => responses::groups::PROTECTED_OBJECT,
                    Err(DdlError::ObjectNotFound) => responses::groups::CONTAINER_NOT_FOUND,
                    Err(DdlError::StillInUse) => responses::groups::STILL_IN_USE,
                    Err(_) => unsafe {
                        // we know that Memstore::drop_table won't ever return anything else
                        impossible!()
                    }
                };
                con.write_response(ret).await?;
            },
            None => con.write_response(responses::groups::ACTION_ERR).await?,
        }
        Ok(())
    }
}

action! {
    /// Drop a keyspace (`<ksid>` only)
    fn drop_keyspace(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        match act.next() {
            Some(ksid) => {
                if ksid.len() > 64 {
                    return con.write_response(responses::groups::CONTAINER_NAME_TOO_LONG).await;
                }
                let ret = match handle.drop_keyspace(unsafe {ObjectID::from_slice(ksid)}) {
                    Ok(()) => responses::groups::OKAY,
                    Err(DdlError::ProtectedObject) => responses::groups::PROTECTED_OBJECT,
                    Err(DdlError::ObjectNotFound) => responses::groups::CONTAINER_NOT_FOUND,
                    Err(DdlError::StillInUse) => responses::groups::STILL_IN_USE,
                    Err(_) => unsafe {
                        // we know that Memstore::drop_table won't ever return anything else
                        impossible!()
                    }
                };
                con.write_response(ret).await?;
            },
            None => con.write_response(responses::groups::ACTION_ERR).await?,
        }
        Ok(())
    }
}
