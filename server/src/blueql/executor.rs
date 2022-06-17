/*
 * Created on Wed Jun 15 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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
    super::{
        ast::{Statement, StatementLT},
        error,
    },
    crate::{
        actions::{self, ActionResult},
        blueql,
        corestore::memstore::ObjectID,
        dbnet::connection::prelude::*,
    },
};

pub async fn execute<'a, P, Strm, T>(
    handle: &'a mut Corestore,
    con: &'a mut T,
    maybe_statement: &[u8],
) -> ActionResult<()>
where
    P: ProtocolSpec,
    T: ClientConnection<P, Strm>,
    Strm: Stream,
{
    let statement = error::map_ql_err_to_resp::<StatementLT, P>(blueql::compile(maybe_statement))?;
    let result = match statement.as_ref() {
        Statement::CreateSpace(space_name) => {
            handle.create_keyspace(unsafe { ObjectID::from_slice(space_name.as_slice()) })
        }
        Statement::DropSpace { entity, force } => {
            let entity = unsafe { ObjectID::from_slice(entity.as_slice()) };
            if *force {
                handle.force_drop_keyspace(entity)
            } else {
                handle.drop_keyspace(entity)
            }
        }
        Statement::CreateModel {
            entity,
            model,
            volatile,
        } => match model.get_model_code() {
            Ok(code) => handle.create_table(entity.into(), code, *volatile),
            Err(e) => return error::map_ql_err_to_resp::<(), P>(Err(e)),
        },
        Statement::DropModel { entity, force } => handle.drop_table(entity.into(), *force),
        Statement::InspectSpaces => {
            con.write_typed_non_null_array(&handle.get_store().list_keyspaces(), b'+')
                .await?;
            Ok(())
        }
        Statement::InspectSpace(space) => {
            con.write_typed_non_null_array(
                handle.list_tables::<P>(space.as_ref().map(|v| unsafe { v.as_slice() }))?,
                b'+',
            )
            .await?;
            Ok(())
        }
        Statement::InspectModel(model) => {
            con.write_string(&handle.describe_table::<P>(model.as_ref().map(|v| v.into()))?)
                .await?;
            Ok(())
        }
    };
    actions::translate_ddl_error::<P, ()>(result)?;
    con._write_raw(P::RCODE_OKAY).await?;
    Ok(())
}
