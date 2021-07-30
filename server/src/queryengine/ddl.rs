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

use super::parser;
use super::parser::VALID_CONTAINER_NAME;
use crate::corestore::memstore::DdlError;
use crate::corestore::memstore::ObjectID;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::encoding;
use crate::registry;
use core::str;

pub const TABLE: &[u8] = "TABLE".as_bytes();
pub const KEYSPACE: &[u8] = "KEYSPACE".as_bytes();
const VOLATILE: &[u8] = "volatile".as_bytes();
const FORCE_REMOVE: &[u8] = "force".as_bytes();

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

action!(
    /// We should have `<tableid> <model>(args)`
    fn create_table(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        err_if_len_is!(con, act.len() > 3 || act.len() < 2);
        let (table_entity, model_code) = match parser::parse_table_args(&mut act) {
            Ok(v) => v,
            Err(e) => return con.write_response(e).await,
        };
        let is_volatile = match act.next() {
            Some(maybe_volatile) => {
                if maybe_volatile.eq(VOLATILE) {
                    true
                } else {
                    return conwrite!(con, responses::groups::UNKNOWN_PROPERTY);
                }
            }
            None => false,
        };
        if registry::state_okay() {
            match handle.create_table(table_entity, model_code, is_volatile) {
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
        } else {
            conwrite!(con, responses::groups::SERVER_ERR)?;
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
                if registry::state_okay() {
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
                } else {
                    return conwrite!(con, responses::groups::SERVER_ERR);
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
            Some(eg) => {
                let entity_group = match parser::get_query_entity(&eg) {
                    Ok(egroup) => egroup,
                    Err(e) => return con.write_response(e).await,
                };
                if registry::state_okay() {
                    let ret = match handle.drop_table(entity_group) {
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
                } else {
                    conwrite!(con, responses::groups::SERVER_ERR)?;
                }
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
                let force_remove = match act.next() {
                    Some(bts) if bts.eq(FORCE_REMOVE) => true,
                    None => false,
                    _ => return conwrite!(con, responses::groups::UNKNOWN_ACTION)
                };
                if registry::state_okay() {
                    let objid = unsafe {ObjectID::from_slice(ksid)};
                    let result = if force_remove {
                        handle.force_drop_keyspace(objid)
                    } else {
                        handle.drop_keyspace(objid)
                    };
                    let ret = match result {
                        Ok(()) => responses::groups::OKAY,
                        Err(DdlError::ProtectedObject) => responses::groups::PROTECTED_OBJECT,
                        Err(DdlError::ObjectNotFound) => responses::groups::CONTAINER_NOT_FOUND,
                        Err(DdlError::StillInUse) => responses::groups::STILL_IN_USE,
                        Err(DdlError::NotEmpty) => responses::groups::KEYSPACE_NOT_EMPTY,
                        Err(_) => unsafe {
                            // we know that Memstore::drop_table won't ever return anything else
                            impossible!()
                        }
                    };
                    con.write_response(ret).await?;
                } else {
                    conwrite!(con, responses::groups::SERVER_ERR)?;
                }
            },
            None => con.write_response(responses::groups::ACTION_ERR).await?,
        }
        Ok(())
    }
}
