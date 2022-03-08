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

use crate::actions::ActionResult;
use crate::auth;
use crate::corestore::Corestore;
use crate::dbnet::connection::prelude::*;
use crate::protocol::{
    element::UnsafeElement, iter::AnyArrayIter, responses, PipelineQuery, SimpleQuery, UnsafeSlice,
};
use crate::{actions, admin};
use core::hint::unreachable_unchecked;
mod ddl;
mod inspect;
pub mod parser;
#[cfg(test)]
mod tests;

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
        let first = $buf.next_uppercase().unwrap_or_custom_aerr(groups::PACKET_ERR)?;
        match first.as_ref() {
            $(
                tags::$action => $fns($db, $con, $buf).await?,
            )*
            $(
                tags::$action2 => $fns2.await?,
            )*
            _ => {
                $con.write_response(responses::groups::UNKNOWN_ACTION).await?;
            }
        }
    };
}

action! {
    /// Execute queries for an anonymous user
    fn execute_simple_noauth(
        _db: &mut Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, T, Strm>,
        buf: SimpleQuery
    ) {
        if buf.is_any_array() {
            let bufref = unsafe { buf.into_inner() };
            let mut iter = unsafe { get_iter(&bufref) };
            match iter.next_uppercase().unwrap_or_custom_aerr(groups::PACKET_ERR)?.as_ref() {
                ACTION_AUTH => auth::auth_login_only(con, auth, iter).await,
                _ => util::err(auth::errors::AUTH_CODE_DENIED),
            }
        } else {
            util::err(groups::WRONGTYPE_ERR)
        }
    }
    //// Execute a simple query
    fn execute_simple(
        db: &mut Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, T, Strm>,
        buf: SimpleQuery
    ) {
        if buf.is_any_array() {
            unsafe {
                self::execute_stage(db, con, auth, &buf.into_inner()).await
            }
        } else {
            util::err(groups::WRONGTYPE_ERR)
        }
    }
}

unsafe fn get_iter<'a>(buf: &'a UnsafeElement) -> AnyArrayIter<'a> {
    let bufref: &'a Box<[UnsafeSlice]>;
    let iter;

    // this is the boxed slice
    bufref = {
        // SAFETY: execute_simple is called by execute_query which in turn is called
        // by ConnnectionHandler::run(). In all cases, the `Con` remains valid
        // ensuring that the source buffer exists as long as the connection does
        // so this is safe.
        match buf {
            UnsafeElement::AnyArray(arr) => arr,
            _ => unreachable_unchecked(),
        }
    };
    // this is our final iter
    iter = {
        // SAFETY: Again, this is guaranteed to be valid because the `con` is valid
        AnyArrayIter::new(bufref.iter())
    };
    iter
}

async fn execute_stage<'a, T: 'a + ClientConnection<Strm>, Strm: Stream>(
    db: &mut Corestore,
    con: &'a mut T,
    auth: &mut AuthProviderHandle<'_, T, Strm>,
    buf: &UnsafeElement,
) -> ActionResult<()> {
    let mut iter = unsafe {
        // UNSAFE(@ohsayan): Assumed to be guaranteed by the caller
        get_iter(buf)
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
            CREATE => ddl::create,
            DROP => ddl::ddl_drop,
            USE => self::entity_swap,
            INSPECT => inspect::inspect,
            MPOP => actions::mpop::mpop,
            LSET => actions::lists::lset,
            LGET => actions::lists::lget::lget,
            LMOD => actions::lists::lmod::lmod,
            WHEREAMI => actions::whereami::whereami,
            {
                // actions that need other arguments
                AUTH => auth::auth(con, auth, iter)
            }
        );
    }
    Ok(())
}

action! {
    /// Handle `use <entity>` like queries
    fn entity_swap(handle: &mut Corestore, con: &mut T, mut act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len == 1)?;
        let entity = unsafe {
            // SAFETY: Already checked len
            act.next_unchecked()
        };
        handle.swap_entity(parser::get_query_entity(entity)?)?;
        con.write_response(groups::OKAY).await?;
        Ok(())
    }
}

action! {
    /// Execute a basic pipelined query
    fn execute_pipeline(
        handle: &mut Corestore,
        con: &mut T,
        auth: &mut AuthProviderHandle<'_, T, Strm>,
        pipeline: PipelineQuery
    ) {
        for stage in pipeline.iter() {
            ensure_cond_or_err(stage.is_any_array(), groups::WRONGTYPE_ERR)?;
            self::execute_stage(handle, con, auth, stage).await?;
        }
        Ok(())
    }
}
