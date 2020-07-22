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

use corelib::terrapipe::{self, DEF_QMETALINE_BUFSIZE};
use std::io::{self, prelude::*, BufReader};
use std::net::TcpStream;
use std::process;
const ADDR: &'static str = "127.0.0.1:2003";
pub fn execute_query() {
    let mut connection = match TcpStream::connect(ADDR) {
        Ok(c) => c,
        Err(_) => {
            eprintln!("ERROR: Couldn't connect to the TDB server");
            process::exit(0x100);
        }
    };
    loop {
        print!("tsh>");
        io::stdout()
            .flush()
            .expect("Couldn't flush buffer, this is a serious error!");
        let mut rl = String::new();
        io::stdin()
            .read_line(&mut rl)
            .expect("Couldn't read line, this is a serious error!");
        let mut cmd = terrapipe::QueryBuilder::new_simple();
        cmd.from_cmd(rl);
        let (size, resp) = cmd.prepare_response();
        match connection.write(&resp) {
            Ok(n) => {
                if n < size {
                    eprintln!("ERROR: Couldn't write all bytes to server");
                    process::exit(0x100);
                }
            },
            Err(_) => {
                eprintln!("ERROR: Couldn't send data to the TDB server");
                process::exit(0x100);
            }
        }
        println!("{}", parse_response(&connection));
    }
}

pub fn parse_response(stream: &TcpStream) -> String {
    let mut metaline = String::with_capacity(DEF_QMETALINE_BUFSIZE);
    let mut bufreader = BufReader::new(stream);
    match bufreader.read_line(&mut metaline) {
        Ok(_) => (),
        Err(_) => {
            eprintln!("Couldn't read metaline from tdb server");
            process::exit(0x100);
        }
    }
    let metaline = metaline.trim_matches(char::from(0));
    let fields: Vec<&str> = metaline.split('!').collect();
    if let (Some(resptype), Some(respcode), Some(clength), Some(ml_length)) =
        (fields.get(0), fields.get(1), fields.get(2), fields.get(3))
    {
        if *resptype == "$" {
            todo!("Pipelined response deconding is yet to be implemented")
        }
        let mut is_err_response = false;
        match respcode.to_owned() {
            "0" => (),
            "1" => return format!("ERROR: Couldn't find the requested key"),
            "2" => return format!("ERROR: Can't overwrite existing value"),
            "3" => return format!("ERROR: tsh sent an invalid metaframe"),
            "4" => return format!("ERROR: tsh sent an incomplete query packet"),
            "5" => return format!("ERROR: tdb had an internal server error"),
            "6" => is_err_response = true,
            _ => (),
        }
        if let (Ok(clength), Ok(ml_length)) = (clength.parse::<usize>(), ml_length.parse::<usize>())
        {
            let mut metalinebuf = String::with_capacity(ml_length);
            let mut databuf = vec![0; clength];
            bufreader.read_line(&mut metalinebuf).unwrap();
            let sizes: Vec<usize> = metalinebuf
                .split("#")
                .map(|size| size.parse::<usize>().unwrap())
                .collect();
            bufreader.read(&mut databuf).unwrap();
            eprintln!("{:?}", String::from_utf8_lossy(&databuf));
            let res = extract_idents(databuf, sizes);
            let resp: String = res.iter().flat_map(|s| s.chars()).collect();
            if !is_err_response {
                return resp;
            } else {
                return format!("ERROR: {}", resp);
            }
        }
    }
    format!("ERROR: The server sent an invalid response")
}

fn extract_idents(buf: Vec<u8>, skip_sequence: Vec<usize>) -> Vec<String> {
    skip_sequence
        .into_iter()
        .scan(buf.into_iter(), |databuf, size| {
            let tok: Vec<u8> = databuf.take(size).collect();
            let _ = databuf.next();
            // FIXME(@ohsayan): This is quite slow, we'll have to use SIMD in the future
            Some(String::from_utf8_lossy(&tok).to_string())
        })
        .collect()
}
