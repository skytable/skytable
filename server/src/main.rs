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
use libcore::terrapipe::{
    Dataframe, QueryMetaframe, QueryMethod, ResponseBytes, ResponseCodes, Version,
    DEF_Q_META_BUFSIZE,
};
use std::io::prelude::*;
use std::io::{BufReader, ErrorKind};
use std::net::{TcpListener, TcpStream};
use std::process;

const SELF_VERSION: Version = Version(0, 1, 0);

const EXIT_ERROR: fn(&'static str) -> ! = |err| {
    eprintln!("error: {}", err);
    process::exit(0x100);
};

fn main() {
    let listener = match TcpListener::bind("127.0.0.1:2003") {
        Ok(binding) => binding,
        Err(e) => match e.kind() {
            ErrorKind::AddrInUse => {
                EXIT_ERROR("Cannot bind to port 2003 as it is already in use");
            }
            // TODO: Need proper error handling
            _ => {
                EXIT_ERROR("Some other error occurred!");
            }
        },
    };
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_stream(stream);
    }
}

fn handle_stream(mut stream: TcpStream) {
    // The largest command that we have right now is UPDATE
    // Which along with the size will mount to 26 + 20 = 460usize
    let mut meta_buffer = String::with_capacity(DEF_Q_META_BUFSIZE);
    let mut bufreader = BufReader::with_capacity(DEF_Q_META_BUFSIZE, &stream);
    bufreader.read_line(&mut meta_buffer).unwrap();
    let mf = match QueryMetaframe::from_buffer(&SELF_VERSION, &meta_buffer) {
        Ok(m) => m,
        Err(e) => return close_conn_with_error(stream, e.response_bytes(&SELF_VERSION)),
    };
    let mut data_buffer = vec![0; mf.get_content_size()];
    bufreader.read(&mut data_buffer).unwrap();
    let df = match Dataframe::from_buffer(mf.get_content_size(), data_buffer) {
        Ok(d) => d,
        Err(e) => return close_conn_with_error(stream, e.response_bytes(&SELF_VERSION)),
    };
    execute_query(stream, mf, df);
}

fn close_conn_with_error(mut stream: TcpStream, error: Vec<u8>) {
    stream.write(&error).unwrap();
}

// TODO: This is a dummy implementation
pub fn execute_query(mut stream: TcpStream, mf: QueryMetaframe, df: Dataframe) {
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
                    .unwrap();
            }
        }
        SET => {
            if vars.len() == 2 {
                println!("SET {} {}", vars[0], vars[1]);
            } else if vars.len() < 2 {
                stream
                    .write(&ResponseCodes::CorruptPacket.response_bytes(&SELF_VERSION))
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
                    .unwrap();
            }
        }
    }
}
