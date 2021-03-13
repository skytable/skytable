/*
 * Created on Tue Aug 25 2020
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

//! This module contains automated tests for queries

use crate::config::SnapshotConfig;
use crate::coredb::CoreDB;
use crate::dbnet;
use crate::BGSave;
use std::net::SocketAddr;
use tokio::net::TcpListener;
mod kvengine;

/// The function macro returns the name of a function
#[macro_export]
macro_rules! __func__ {
    () => {{
        fn f() {}
        fn typename<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let fn_name = typename(f);
        &fn_name[..fn_name.len() - 3]
    }};
}

async fn start_test_server(port: u16, db: Option<CoreDB>) -> SocketAddr {
    let mut socket = String::from("127.0.0.1:");
    socket.push_str(&port.to_string());
    let db = db.unwrap_or(CoreDB::new(BGSave::Disabled, SnapshotConfig::default(), None).unwrap());
    let listener = TcpListener::bind(socket)
        .await
        .expect(&format!("Failed to bind to port {}", port));
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { dbnet::test_run(listener, db, tokio::signal::ctrl_c()).await });
    addr
}
