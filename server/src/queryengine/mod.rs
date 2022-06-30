/*
 * Created on Mon Aug 03 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! # The Query Engine

use crate::{
    actions::{self, ActionError, ActionResult},
    admin, auth, blueql,
    corestore::Corestore,
    dbnet::connection::prelude::*,
    protocol::{iter::AnyArrayIter, PipelinedQuery, SimpleQuery, UnsafeSlice},
};

pub type ActionIter<'a> = AnyArrayIter<'a>;

const ACTION_AUTH: &[u8] = b"auth";

macro_rules! gen_constants_and_matches {
    (
        $con:expr, $buf:ident, $db:ident, $($action:ident => $fns:path),*,
        {$($action2:ident => $fns2:expr),*}
    ) => {
        mod tags {
            //! This module is a collection of tags/strings used for evaluating queries
            //! and responses
            $(
                pub const $action: &[u8] = stringify!($action).as_bytes();
            )*
            $(
                pub const $action2: &[u8] = stringify!($action2).as_bytes();
            )*
        }
        let first_slice = $buf.next().unwrap_or_custom_aerr(P::RCODE_PACKET_ERR)?;
        let first = first_slice.to_ascii_uppercase();
        match first.as_ref() {
            $(
                tags::$action => $fns($db, $con, $buf).await?,
            )*
            $(
                tags::$action2 => $fns2.await?,
            )*
            _ => {
                blueql::execute($db, $con, first_slice, $buf.len()).await?;
            }
        }
    };
}

action! {
    /// Execute queries for an anonymous user
    fn execute_simple_noauth(
        _db: &mut Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        buf: SimpleQuery
    ) {
        let bufref = buf.as_slice();
        let mut iter = unsafe {
            // UNSAFE(@ohsayan): The presence of the connection guarantees that this
            // won't suddenly become invalid
            AnyArrayIter::new(bufref.iter())
        };
        match iter.next_lowercase().unwrap_or_custom_aerr(P::RCODE_PACKET_ERR)?.as_ref() {
            ACTION_AUTH => auth::auth_login_only(con, auth, iter).await,
            _ => util::err(P::AUTH_CODE_BAD_CREDENTIALS),
        }
    }
    //// Execute a simple query
    fn execute_simple(
        db: &mut Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        buf: SimpleQuery
    ) {
        self::execute_stage(db, con, auth, buf.as_slice()).await
    }
}

async fn execute_stage<'a, P: ProtocolSpec, T: 'a + ClientConnection<P, Strm>, Strm: Stream>(
    db: &mut Corestore,
    con: &'a mut T,
    auth: &mut AuthProviderHandle<'_, P, T, Strm>,
    buf: &[UnsafeSlice],
) -> ActionResult<()> {
    let mut iter = unsafe {
        // UNSAFE(@ohsayan): The presence of the connection guarantees that this
        // won't suddenly become invalid
        AnyArrayIter::new(buf.iter())
    };
    {
        gen_constants_and_matches!(
            con, iter, db,
            GET => actions::get::get,
            SET => actions::set::set,
            UPDATE => actions::update::update,
            DEL => actions::del::del,
            HEYA => actions::heya::heya,
            EXISTS => actions::exists::exists,
            MSET => actions::mset::mset,
            MGET => actions::mget::mget,
            MUPDATE => actions::mupdate::mupdate,
            SSET => actions::strong::sset,
            SDEL => actions::strong::sdel,
            SUPDATE => actions::strong::supdate,
            DBSIZE => actions::dbsize::dbsize,
            FLUSHDB => actions::flushdb::flushdb,
            USET => actions::uset::uset,
            KEYLEN => actions::keylen::keylen,
            MKSNAP => admin::mksnap::mksnap,
            LSKEYS => actions::lskeys::lskeys,
            POP => actions::pop::pop,
            MPOP => actions::mpop::mpop,
            LSET => actions::lists::lset,
            LGET => actions::lists::lget::lget,
            LMOD => actions::lists::lmod::lmod,
            WHEREAMI => actions::whereami::whereami,
            SYS => admin::sys::sys,
            {
                // actions that need other arguments
                AUTH => auth::auth(con, auth, iter)
            }
        );
    }
    Ok(())
}

/// Execute a stage **completely**. This means that action errors are never propagated
/// over the try operator
async fn execute_stage_pedantic<
    'a,
    P: ProtocolSpec,
    T: ClientConnection<P, Strm> + 'a,
    Strm: Stream + 'a,
>(
    handle: &mut Corestore,
    con: &mut T,
    auth: &mut AuthProviderHandle<'_, P, T, Strm>,
    stage: &[UnsafeSlice],
) -> crate::IoResult<()> {
    let ret = async {
        self::execute_stage(handle, con, auth, stage).await?;
        Ok(())
    };
    match ret.await {
        Ok(()) => Ok(()),
        Err(ActionError::ActionError(e)) => con._write_raw(e).await,
        Err(ActionError::IoError(ioe)) => Err(ioe),
    }
}

action! {
    /// Execute a basic pipelined query
    fn execute_pipeline(
        handle: &mut Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, P, T, Strm>,
        pipeline: PipelinedQuery
    ) {
        for stage in pipeline.into_inner().iter() {
            self::execute_stage_pedantic(handle, con, auth, stage).await?;
        }
        Ok(())
    }
}
