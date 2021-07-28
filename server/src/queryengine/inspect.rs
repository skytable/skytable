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

use super::ddl::{KEYSPACE, TABLE};
use crate::corestore::memstore::ObjectID;
use crate::dbnet::connection::prelude::*;

const KEYSPACES: &[u8] = "KEYSPACES".as_bytes();
action! {
    fn inspect(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        match act.next() {
            Some(inspect_what) => {
                let mut inspect_what = inspect_what.to_vec();
                inspect_what.make_ascii_uppercase();
                match inspect_what.as_ref() {
                    KEYSPACE => inspect_keyspace(handle, con, act).await?,
                    TABLE => inspect_table(handle, con, act).await?,
                    KEYSPACES => {
                        // let's return what all keyspaces exist
                        let ks_list: Vec<ObjectID> = handle
                            .get_store()
                            .keyspaces
                            .iter()
                            .map(|kv| kv.key().clone())
                            .collect();
                        con.write_flat_array_length(ks_list.len()).await?;
                        for tbl in ks_list {
                            con.write_response(tbl).await?;
                        }
                    }
                    _ => conwrite!(con, responses::groups::UNKNOWN_INSPECT_QUERY)?,
                }
            }
            None => aerr!(con, aerr),
        }
        Ok(())
    }
}

action! {
    fn inspect_keyspace(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        match act.next() {
            Some(keyspace_name) => {
                let ksid = if keyspace_name.len() > 64 {
                    return conwrite!(con, responses::groups::BAD_CONTAINER_NAME);
                } else {
                    unsafe {
                        ObjectID::from_slice(keyspace_name)
                    }
                };
                let ks = match handle.get_keyspace(ksid) {
                    Some(kspace) => kspace,
                    None => return conwrite!(con, responses::groups::CONTAINER_NOT_FOUND),
                };
                let tbl_list: Vec<ObjectID> = ks.tables.iter().map(|kv| kv.key().clone()).collect();
                con.write_flat_array_length(tbl_list.len()).await?;
                for tbl in tbl_list {
                    con.write_response(tbl).await?;
                }
            },
            None => aerr!(con, aerr),
        }
        Ok(())
    }
}

action! {
    fn inspect_table(handle: &Corestore, con: &mut T, mut act: ActionIter) {
        match act.next() {
            Some(entity) => {
                let entity = handle_entity!(con, entity);
                conwrite!(con, get_tbl!(entity, handle, con).describe_self())?;
            },
            None => aerr!(con, aerr),
        }
        Ok(())
    }
}
