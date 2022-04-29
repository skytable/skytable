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
    /// Runs an inspect query:
    /// - `INSPECT KEYSPACES` is run by this function itself
    /// - `INSPECT TABLE <tblid>` is delegated to self::inspect_table
    /// - `INSPECT KEYSPACE <ksid>` is delegated to self::inspect_keyspace
    fn inspect(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        match act.next() {
            Some(inspect_what) => {
                let mut inspect_what = inspect_what.to_vec();
                inspect_what.make_ascii_uppercase();
                match inspect_what.as_ref() {
                    KEYSPACE => inspect_keyspace(handle, con, act).await?,
                    TABLE => inspect_table(handle, con, act).await?,
                    KEYSPACES => {
                        ensure_length(act.len(), |len| len == 0)?;
                        // let's return what all keyspaces exist
                        let ks_list: Vec<ObjectID> = handle
                            .get_store()
                            .keyspaces
                            .iter()
                            .map(|kv| kv.key().clone())
                            .collect();
                        con.write_typed_non_null_array_header(ks_list.len(), b'+').await?;
                        for ks in ks_list {
                            con.write_typed_non_null_array_element(&ks).await?;
                        }
                    }
                    _ => return util::err(groups::UNKNOWN_INSPECT_QUERY),
                }
            }
            None => return util::err(groups::ACTION_ERR),
        }
        Ok(())
    }

    /// INSPECT a keyspace. This should only have the keyspace ID
    fn inspect_keyspace(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len < 2)?;
        let tbl_list: Vec<ObjectID> =
        match act.next() {
            Some(keyspace_name) => {
                // inspect the provided keyspace
                let ksid = if keyspace_name.len() > 64 {
                    return util::err(groups::BAD_CONTAINER_NAME);
                } else {
                    keyspace_name
                };
                let ks = match handle.get_keyspace(ksid) {
                    Some(kspace) => kspace,
                    None => return util::err(groups::CONTAINER_NOT_FOUND),
                };
                ks.tables.iter().map(|kv| kv.key().clone()).collect()
            },
            None => {
                // inspect the current keyspace
                let cks = handle.get_cks()?;
                cks.tables.iter().map(|kv| kv.key().clone()).collect()
            },
        };
        con.write_typed_non_null_array_header(tbl_list.len(), b'+').await?;
        for tbl in tbl_list {
            con.write_typed_non_null_array_element(&tbl).await?;
        }
        Ok(())
    }

    /// INSPECT a table. This should only have the table ID
    fn inspect_table(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len < 2)?;
        match act.next() {
            Some(entity) => {
                let entity = handle_entity!(con, entity);
                con.write_string(get_tbl!(entity, handle, con).describe_self()).await?;
            },
            None => {
                // inspect the current table
                let tbl = handle.get_table_result()?;
                con.write_string(tbl.describe_self()).await?;
            },
        }
        Ok(())
    }
}
