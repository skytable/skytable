/*
 * Created on Thu Jul 02 2020
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

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod coredb;
mod protocol;
use protocol::read_query;
static ADDR: &'static str = "127.0.0.1:2003";

#[tokio::main]
async fn main() {
    let mut listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Server running on terrapipe://127.0.0.1:2003");
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let q = read_query(&mut socket).await;
            let df = match q {
                Ok(q) => q,
                Err(e) => return close_conn_with_error(socket, e).await,
            };
            println!("{:#?}", df);
        });
    }
}

async fn close_conn_with_error(mut stream: TcpStream, bytes: Vec<u8>) {
    stream.write_all(&bytes).await.unwrap()
}
