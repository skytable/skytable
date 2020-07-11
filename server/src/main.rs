/*
 * Created on Thu Jul 02 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020 Sayan Nandan
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

// Internal modules
use libcore::terrapipe::{
    Dataframe, QueryMetaframe, QueryMethod, ResponseBytes, ResponseCodes, Version,
    DEF_Q_META_BUFSIZE,
};

const SELF_VERSION: Version = Version(0, 1, 0);

static ADDR: &'static str = "127.0.0.1:2003";

#[tokio::main]
async fn main() {
    let mut listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Server running on terrapipe://127.0.0.1:2003");

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut meta_buffer = String::with_capacity(DEF_Q_META_BUFSIZE);
            let mut reader = BufReader::with_capacity(DEF_Q_META_BUFSIZE, &mut socket);
            reader.read_line(&mut meta_buffer).await.unwrap();
            let mf = match QueryMetaframe::from_buffer(&SELF_VERSION, &meta_buffer) {
                Ok(m) => m,
                Err(e) => {
                    return close_conn_with_error(socket, e.response_bytes(&SELF_VERSION)).await
                }
            };
            let mut data_buffer = vec![0; mf.get_content_size()];
            reader.read(&mut data_buffer).await.unwrap();
            let df = match Dataframe::from_buffer(mf.get_content_size(), data_buffer) {
                Ok(d) => d,
                Err(e) => {
                    return close_conn_with_error(socket, e.response_bytes(&SELF_VERSION)).await
                }
            };
            return execute_query(socket, mf, df).await;
        });
    }
}

async fn close_conn_with_error(mut stream: TcpStream, bytes: Vec<u8>) {
    stream.write_all(&bytes).await.unwrap()
}

async fn execute_query(mut stream: TcpStream, mf: QueryMetaframe, df: Dataframe) {
    let vars = df.deflatten();
    use QueryMethod::*;
    match mf.get_method() {
        GET => {
            if vars.len() == 1 {
                println!("GET {}", vars[0]);
            } else if vars.len() > 1 {
                eprintln!("ERROR: Cannot do multiple GETs just yet");
            } else {
                stream
                    .write(&ResponseCodes::CorruptPacket.response_bytes(&SELF_VERSION))
                    .await
                    .unwrap();
            }
        }
        SET => {
            if vars.len() == 2 {
                println!("SET {} {}", vars[0], vars[1]);
            } else if vars.len() < 2 {
                stream
                    .write(&ResponseCodes::CorruptPacket.response_bytes(&SELF_VERSION))
                    .await
                    .unwrap();
            } else {
                eprintln!("ERROR: Cannot do multiple SETs just yet");
            }
        }
        UPDATE => {
            if vars.len() == 2 {
                println!("UPDATE {} {}", vars[0], vars[1]);
            } else if vars.len() < 2 {
                stream
                    .write(&ResponseCodes::CorruptPacket.response_bytes(&SELF_VERSION))
                    .await
                    .unwrap();
            } else {
                eprintln!("ERROR: Cannot do multiple UPDATEs just yet");
            }
        }
        DEL => {
            if vars.len() == 1 {
                println!("DEL {}", vars[0]);
            } else if vars.len() > 1 {
                eprintln!("ERROR: Cannot do multiple DELs just yet")
            } else {
                stream
                    .write(&ResponseCodes::CorruptPacket.response_bytes(&SELF_VERSION))
                    .await
                    .unwrap();
            }
        }
    }
}
