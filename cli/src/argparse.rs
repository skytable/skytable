/*
 * Created on Wed Jul 01 2020
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

use libcore::terrapipe::DEF_R_META_BUFSIZE;
use libcore::terrapipe::{Dataframe, ResponseCodes::*, ResultMetaframe, Version, QUERY_PACKET};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process;

const ARG_GET: &'static str = "GET";
const ARG_SET: &'static str = "SET";
const ARG_UPDATE: &'static str = "UPDATE";
const ARG_DEL: &'static str = "DEL";
const ARG_EXIT: &'static str = "EXIT";
const ERR_MULTIPLE_GET: &'static str = "Multiple GETs aren't supported yet";
const ERR_MULTIPLE_SET: &'static str = "Multiple SETs aren't supported yet";
const ERR_MULTIPLE_UPDATE: &'static str = "Multiple UPDATEs aren't supported yet";
const ERR_MULTIPLE_DEL: &'static str = "Multiple DELs aren't supported yet";
const SELF_VERSION: Version = Version(0, 1, 0);
const ADDR: &'static str = "127.0.0.1:2003";
pub const EXIT_ERROR: fn(error: &str) -> ! = |error| {
    eprintln!("error: {}", error);
    process::exit(0x100);
};

const NORMAL_ERROR: fn(error: &str) = |error| {
    eprintln!("error: {}", error);
};

pub fn run(args: String) {
    let args: Vec<&str> = args.split_whitespace().collect();
    match args[0].to_uppercase().as_str() {
        ARG_GET => {
            if let Some(key) = args.get(1) {
                if args.get(2).is_none() {
                    send_query(QUERY_PACKET(
                        SELF_VERSION,
                        ARG_GET.to_owned(),
                        key.to_string(),
                    ));
                } else {
                    NORMAL_ERROR(ERR_MULTIPLE_GET);
                }
            } else {
                NORMAL_ERROR("Expected one more argument");
            }
        }
        ARG_SET => {
            if let Some(key) = args.get(1) {
                if let Some(value) = args.get(2) {
                    if args.get(3).is_none() {
                        send_query(QUERY_PACKET(
                            SELF_VERSION,
                            ARG_SET.to_owned(),
                            format!("{} {}", key, value),
                        ))
                    } else {
                        NORMAL_ERROR(ERR_MULTIPLE_SET);
                    }
                } else {
                    NORMAL_ERROR("Expected one more argument");
                }
            } else {
                NORMAL_ERROR("Expected more arguments");
            }
        }
        ARG_UPDATE => {
            if let Some(key) = args.get(1) {
                if let Some(value) = args.get(2) {
                    if args.get(3).is_none() {
                        send_query(QUERY_PACKET(
                            SELF_VERSION,
                            ARG_UPDATE.to_owned(),
                            format!("{} {}", key, value),
                        ))
                    } else {
                        NORMAL_ERROR(ERR_MULTIPLE_UPDATE);
                    }
                } else {
                    NORMAL_ERROR("Expected one more argument");
                }
            } else {
                NORMAL_ERROR("Expected more arguments");
            }
        }
        ARG_DEL => {
            if let Some(key) = args.get(1) {
                if args.get(2).is_none() {
                    send_query(QUERY_PACKET(
                        SELF_VERSION,
                        ARG_DEL.to_owned(),
                        key.to_string(),
                    ));
                } else {
                    NORMAL_ERROR(ERR_MULTIPLE_DEL);
                }
            } else {
                NORMAL_ERROR("Expected one more argument");
            }
        }
        ARG_EXIT => {
            println!("Goodbye!");
            process::exit(0x100)
        }
        _ => NORMAL_ERROR("Unknown command"),
    }
}

fn send_query(query: Vec<u8>) {
    let mut binding = match TcpStream::connect(ADDR) {
        Ok(b) => b,
        Err(_) => EXIT_ERROR("Couldn't connect to Terrabase"),
    };
    match binding.write(&query) {
        Ok(_) => (),
        Err(_) => EXIT_ERROR("Couldn't read data from socket"),
    }
    let mut bufreader = BufReader::new(binding);
    let mut buf = String::with_capacity(DEF_R_META_BUFSIZE);
    match bufreader.read_line(&mut buf) {
        Ok(_) => (),
        Err(_) => EXIT_ERROR("Failed to read line from socket"),
    }
    let rmf = match ResultMetaframe::from_buffer(buf) {
        Ok(mf) => mf,
        Err(e) => {
            NORMAL_ERROR(&e.to_string());
            return;
        }
    };
    match &rmf.response {
        Okay(_) => (),
        x @ _ => {
            NORMAL_ERROR(&x.to_string());
            return;
        }
    }
    let mut data_buffer = vec![0; rmf.get_content_size()];
    match bufreader.read(&mut data_buffer) {
        Ok(_) => (),
        Err(_) => EXIT_ERROR("Failed to read line from socket"),
    }
    let df = match Dataframe::from_buffer(rmf.get_content_size(), data_buffer) {
        Ok(d) => d,
        Err(e) => {
            NORMAL_ERROR(&e.to_string());
            return;
        }
    };
    let res = df.deflatten();
    if res.len() == 0 {
        return;
    } else {
        if res.len() == 1 {
            println!("{}", res[0]);
        } else {
            println!("{:?}", res);
        }
    }
}
