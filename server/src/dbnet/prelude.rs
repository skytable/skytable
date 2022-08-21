/*
 * Created on Sun Aug 21 2022
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

//! A 'prelude' for imports to interface with the database and the client
//!
//! This module is hollow itself, it only re-exports from `dbnet::con` and `tokio::io`

pub use {
    super::{connection::Connection, AuthProviderHandle},
    crate::{
        actions::{ensure_boolean_or_aerr, ensure_length, translate_ddl_error},
        corestore::{
            table::{KVEBlob, KVEList},
            Corestore,
        },
        get_tbl, handle_entity, is_lowbit_set,
        protocol::interface::ProtocolSpec,
        queryengine::ActionIter,
        registry,
        util::{self, UnwrapActionError, Unwrappable},
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};
