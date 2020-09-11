/*
 * Created on Tue Aug 25 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

use crate::coredb::CoreDB;
use crate::dbnet;
use crate::protocol::responses::fresp;
use libtdb::terrapipe;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
mod kvengine_tests;

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

static ADDR: &'static str = "127.0.0.1:2003";

/// Start the server as a background asynchronous task
async fn start_server(db: Option<CoreDB>) -> (Option<SocketAddr>, CoreDB) {
    // HACK(@ohsayan): Since we want to start the server if it is not already
    // running, or use it if it is already running, we just return none if we failed
    // to bind to the port, since this will _almost_ never happen on our CI
    let listener = TcpListener::bind(ADDR).await.unwrap();
    let db = if let Some(db) = db {
        db
    } else {
        CoreDB::new().unwrap()
    };
    let asyncdb = db.clone();
    let addr = if let Ok(addr) = listener.local_addr() {
        Some(addr)
    } else {
        None
    };
    tokio::spawn(async move { dbnet::test_run(listener, asyncdb, tokio::signal::ctrl_c()).await });
    (addr, db)
}
