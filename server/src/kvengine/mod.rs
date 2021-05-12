/*
 * Created on Wed Aug 19 2020
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # The Key/Value Engine
//! This is Skytable's K/V engine. It contains utilities to interface with
//! Skytable's K/V store

pub mod dbsize;
pub mod del;
pub mod exists;
pub mod flushdb;
pub mod get;
pub mod jget;
pub mod keylen;
pub mod mget;
pub mod mset;
pub mod mupdate;
pub mod set;
pub mod strong;
pub mod update;
pub mod uset;
pub mod heya {
    //! Respond to `HEYA` queries
    use crate::dbnet::connection::prelude::*;
    use crate::protocol;
    use protocol::responses;
    /// Returns a `HEY!` `Response`
    pub async fn heya<T, Strm>(
        _handle: &crate::coredb::CoreDB,
        con: &mut T,
        _act: Vec<String>,
    ) -> std::io::Result<()>
    where
        T: ProtocolConnectionExt<Strm>,
        Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
    {
        con.write_response(&**responses::groups::HEYA).await
    }
}

#[macro_export]
macro_rules! err_if_len_is {
    ($buf:ident, $con:ident, == $len:literal) => {
        if $buf.len() - 1 == $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
    ($buf:ident, $con:ident, != $len:literal) => {
        if $buf.len() - 1 != $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
    ($buf:ident, $con:ident, > $len:literal) => {
        if $buf.len() - 1 > $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
    ($buf:ident, $con:ident, < $len:literal) => {
        if $buf.len() - 1 < $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
    ($buf:ident, $con:ident, >= $len:literal) => {
        if $buf.len() - 1 >= $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
    ($buf:ident, $con:ident, <= $len:literal) => {
        if $buf.len() - 1 <= $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
    ($buf:ident, $con:ident, & $len:literal) => {
        if $buf.len() - 1 & $len {
            return $con
                .write_response(&**crate::protocol::responses::groups::ACTION_ERR)
                .await;
        }
    };
}
