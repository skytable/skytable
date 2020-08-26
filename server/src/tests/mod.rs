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
use std::io::ErrorKind;
use std::net::{Shutdown, SocketAddr};
use std::thread;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

/// Start the server as a background asynchronous task
async fn start_server() -> Option<SocketAddr> {
    // HACK(@ohsayan): Since we want to start the server if it is not already
    // running, or use it if it is already running, we just return none if we failed
    // to bind to the port, since this will _almost_ never happen on our CI
    let listener = match TcpListener::bind(ADDR).await {
        Ok(l) => l,
        Err(e) => match e.kind() {
            ErrorKind::AddrInUse => return None,
            x @ _ => panic!("Failed to start bg async server: '{:?}'", x),
        },
    };
    let addr = if let Ok(addr) = listener.local_addr() {
        Some(addr)
    } else {
        None
    };
    tokio::spawn(async move { dbnet::run(listener, tokio::signal::ctrl_c()).await });
    addr
}

async fn try_get_stream() -> TcpStream {
    const SLEEP_DURATION: u64 = 4;
    let mut server = start_server().await;
    thread::sleep(Duration::from_secs(SLEEP_DURATION)); // Sleep for four seconds
    if let Ok(stream) = TcpStream::connect(ADDR).await {
        return stream;
    }
    loop {
        // try starting the server again
        server = start_server().await;
        thread::sleep(Duration::from_secs(SLEEP_DURATION)); // Sleep for four seconds
        if let Ok(stream) = TcpStream::connect(ADDR).await {
            return stream;
        } else {
            continue;
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_heya() {
    let server = start_server().await;
    let mut stream = try_get_stream().await;
    stream
        .write_all(b"#2\n*1\n#2\n&1\n#4\nHEYA\n")
        .await
        .unwrap();
    let res_should_be = "#2\n*1\n#2\n&1\n+4\nHEY!\n".as_bytes().to_owned();
    let mut response = vec![0; res_should_be.len()];
    stream.read_exact(&mut response).await.unwrap();
    stream.shutdown(Shutdown::Write).unwrap();
    assert_eq!(response.to_vec(), res_should_be);
}

#[tokio::test]
#[ignore]
async fn test_set_single_nil() {
    let server = start_server().await;
    let mut stream = try_get_stream().await;
    stream
        .write_all(b"#2\n*1\n#2\n&2\n#3\nGET\n#1\nx\n")
        .await
        .unwrap();
    let res_should_be = "#2\n*1\n#2\n&1\n!1\n1\n".as_bytes().to_owned();
    let mut response = vec![0; res_should_be.len()];
    stream.read_exact(&mut response).await.unwrap();
    stream.shutdown(Shutdown::Write).unwrap();
    assert_eq!(response, res_should_be);
}

#[tokio::test]
#[ignore]
async fn test_set_multiple_nil() {
    let server = start_server().await;
    let mut stream = try_get_stream().await;
    stream
        .write_all(b"#2\n*1\n#2\n&3\n#3\nGET\n#1\nx\n#2\nex\n")
        .await
        .unwrap();
    let res_should_be = b"#2\n*1\n#2\n&2\n!1\n1\n!1\n1";
    let mut response = vec![0; res_should_be.len()];
    stream.read_exact(&mut response).await.unwrap();
    stream.shutdown(Shutdown::Write).unwrap();
    assert_eq!(response, res_should_be.to_vec());
}
