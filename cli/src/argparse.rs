/*
 * Created on Wed Jul 01 2020
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

use crate::protocol;
use libtdb::terrapipe::ADDR;
use std::env;
use std::io::{self, prelude::*};
use std::process;

/// This creates a REPL on the command line and also parses command-line arguments
/// Anything that is entered following a return, is parsed into a query and is
/// written to the socket (which is either `localhost:2003` or it is determined by
/// command line parameters)
pub async fn execute_query() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() > 2 {
        eprintln!("Incorrect number of arguments\n\tUSAGE tsh [host] [port]");
    }
    let mut host = match args.get(0) {
        Some(h) => h.clone(),
        None => ADDR.to_owned(),
    };
    host.push(':');
    match args.get(1) {
        Some(p) => match p.parse::<u16>() {
            Ok(p) => host.push_str(&p.to_string()),
            Err(_) => {
                eprintln!("ERROR: Invalid port");
                process::exit(0x100);
            }
        },
        None => host.push_str("2003"),
    }
    let mut con = match protocol::Connection::new(&host).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: {}", e);
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
        if rl.trim().to_uppercase() == "EXIT" {
            println!("Goodbye!");
            process::exit(0x100);
        }
        con.run_query(rl).await;
    }
}
