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

use {
    super::ddl::{KEYSPACE, TABLE},
    crate::{corestore::table::Table, dbnet::connection::prelude::*},
};

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
                        ensure_length::<P>(act.len(), |len| len == 0)?;
                        // let's return what all keyspaces exist
                        con.write_typed_non_null_array(
                            handle.get_store().list_keyspaces(),
                            b'+'
                        ).await?
                    }
                    _ => return util::err(P::RSTRING_UNKNOWN_INSPECT_QUERY),
                }
            }
            None => return util::err(P::RCODE_ACTION_ERR),
        }
        Ok(())
    }

    /// INSPECT a keyspace. This should only have the keyspace ID
    fn inspect_keyspace(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len < 2)?;
        con.write_typed_non_null_array(handle.list_tables::<P>(act.next())?, b'+').await?;
        Ok(())
    }

    /// INSPECT a table. This should only have the table ID
    fn inspect_table(handle: &Corestore, con: &'a mut T, mut act: ActionIter<'a>) {
        ensure_length::<P>(act.len(), |len| len < 2)?;
        match act.next() {
            Some(entity) => {
                let entity = handle_entity!(con, entity);
                con.write_string(get_tbl!(entity, handle, con).describe_self()).await?;
            },
            None => {
                // inspect the current table
                let tbl = translate_ddl_error::<P, &Table>(handle.get_table_result())?;
                con.write_string(tbl.describe_self()).await?;
            },
        }
        Ok(())
    }
}
