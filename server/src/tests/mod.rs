/*
 * Created on Tue Aug 25 2020
 *
 * This file is a part of the source code for the Terrabase database
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

use crate::dbnet;
use crate::ADDR;
use std::net::{Shutdown, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

/// Start the server as a background asynchronous task
async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { dbnet::run(listener, tokio::signal::ctrl_c()).await });

    addr
}

#[tokio::test]
async fn test_heya() {
    let server = start_server().await;
    let mut stream = TcpStream::connect(ADDR).await.unwrap();
    stream
        .write_all(b"#2\n*1\n#2\n&1\n#4\nHEYA\n")
        .await
        .unwrap();
    let mut response = [0; 20];
    let res_should_be = "#2\n*1\n#2\n&1\n+4\nHEY!\n".as_bytes().to_owned();
    stream.read_exact(&mut response).await.unwrap();
    stream.shutdown(Shutdown::Write).unwrap();
    assert_eq!(response.to_vec(), res_should_be);
}
