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
use std::future::Future;
use std::net::{Shutdown, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

static ADDR: &'static str = "127.0.0.1:2003";

/// Start the server as a background asynchronous task
async fn start_server() -> (Option<SocketAddr>, CoreDB) {
    // HACK(@ohsayan): Since we want to start the server if it is not already
    // running, or use it if it is already running, we just return none if we failed
    // to bind to the port, since this will _almost_ never happen on our CI
    let listener = TcpListener::bind(ADDR).await.unwrap();
    let db = CoreDB::new().unwrap();
    let asyncdb = db.clone();
    let addr = if let Ok(addr) = listener.local_addr() {
        Some(addr)
    } else {
        None
    };
    tokio::spawn(async move { dbnet::test_run(listener, asyncdb, tokio::signal::ctrl_c()).await });
    (addr, db)
}

struct QueryVec<'a> {
    streams: Vec<TcpStream>,
    db: &'a CoreDB,
}
impl<'a> QueryVec<'a> {
    pub fn new<'b>(db: &'b CoreDB) -> Self
    where
        'b: 'a,
    {
        QueryVec {
            streams: Vec::new(),
            db,
        }
    }
    pub async fn add<F, Fut>(&mut self, function: F)
    where
        F: FnOnce(TcpStream) -> Fut,
        Fut: Future<Output = TcpStream>,
    {
        self.db.finish_db();
        let stream = TcpStream::connect(ADDR).await.unwrap();
        self.streams.push(function(stream).await);
    }
    pub fn run_queries_and_close_sockets(self) {
        for socket in self.streams.into_iter() {
            socket.shutdown(Shutdown::Both).unwrap();
        }
        self.db.finish_db();
    }
}

#[tokio::test]
#[cfg(test)]
async fn test_queries() {
    // Start the server
    let (server, db) = start_server().await;
    let mut queries = QueryVec::new(&db);
    queries.add(test_heya).await;
    queries.add(test_get_single_nil).await;
    queries.add(test_get_single_okay).await;
    queries.add(test_set_single_okay).await;
    queries.add(test_set_single_overwrite_error).await;
    queries.add(test_update_single_okay).await;
    queries.add(test_update_single_nil).await;
    queries.run_queries_and_close_sockets();

    // Clean up everything else
    drop(server);
    drop(db);
}

#[cfg(test)]
/// Test a HEYA query: The server should return HEY!
async fn test_heya(mut stream: TcpStream) -> TcpStream {
    let heya = terrapipe::proc_query("HEYA");
    stream.write_all(&heya).await.unwrap();
    let res_should_be = "#2\n*1\n#2\n&1\n+4\nHEY!\n".as_bytes().to_owned();
    let mut response = vec![0; res_should_be.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response.to_vec(), res_should_be, "HEYA failed");
    stream
}

#[cfg(test)]
/// Test a GET query: for a non-existing key
async fn test_get_single_nil(mut stream: TcpStream) -> TcpStream {
    let get_single_nil = terrapipe::proc_query("GET x");
    stream.write_all(&get_single_nil).await.unwrap();
    let mut response = vec![0; fresp::R_NIL.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(
        response.to_vec(),
        fresp::R_NIL.to_owned(),
        "GET SINGLE NIL failed"
    );
    stream
}

#[cfg(test)]
/// Test a GET query: for an existing key
async fn test_get_single_okay(stream: TcpStream) -> TcpStream {
    let mut stream = test_set_single_okay(stream).await;
    let get_single_nil = terrapipe::proc_query("GET x");
    stream.write_all(&get_single_nil).await.unwrap();
    let res_should_be = "#2\n*1\n#2\n&1\n+3\n100\n".as_bytes().to_owned();
    let mut response = vec![0; res_should_be.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response.to_vec(), res_should_be, "GET SINGLE NIL failed");
    stream
}

#[cfg(test)]
/// Test a SET query: SET a non-existing key, which should return code: 0
async fn test_set_single_okay(mut stream: TcpStream) -> TcpStream {
    let set_single_okay = terrapipe::proc_query("SET x 100");
    stream.write_all(&set_single_okay).await.unwrap();
    let mut response = vec![0; fresp::R_OKAY.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(
        response.to_vec(),
        fresp::R_OKAY.to_owned(),
        "SET SINGLE OKAY failed"
    );
    stream
}

#[cfg(test)]
/// Test a SET query: SET an existing key, which should return code: 2
async fn test_set_single_overwrite_error(stream: TcpStream) -> TcpStream {
    let mut stream = test_set_single_okay(stream).await;
    let set_single_code_2 = terrapipe::proc_query("SET x 200");
    stream.write_all(&set_single_code_2).await.unwrap();
    let mut response = vec![0; fresp::R_OVERWRITE_ERR.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response.to_vec(), fresp::R_OVERWRITE_ERR.to_owned());
    stream
}

#[cfg(test)]
/// Test an UPDATE query: which should return code: 0
async fn test_update_single_okay(stream: TcpStream) -> TcpStream {
    let mut stream = test_set_single_okay(stream).await;
    let update_single_okay = terrapipe::proc_query("UPDATE x 200");
    stream.write_all(&update_single_okay).await.unwrap();
    let mut response = vec![0; fresp::R_OKAY.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response.to_vec(), fresp::R_OKAY.to_owned());
    stream
}

#[cfg(test)]
/// Test an UPDATE query: which should return code: 1
async fn test_update_single_nil(mut stream: TcpStream) -> TcpStream {
    let update_single_okay = terrapipe::proc_query("UPDATE x 200");
    stream.write_all(&update_single_okay).await.unwrap();
    let mut response = vec![0; fresp::R_NIL.len()];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, fresp::R_NIL.to_owned());
    stream
}
