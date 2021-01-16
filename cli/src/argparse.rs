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
use clap::load_yaml;
use clap::App;
use libtdb::terrapipe::ADDR;
use protocol::{Con, Connection, SslConnection};
use std::io::{self, prelude::*};
use std::process;
const MSG_WELCOME: &'static str = "TerrabaseDB v0.5.1";

/// This creates a REPL on the command line and also parses command-line arguments
///
/// Anything that is entered following a return, is parsed into a query and is
/// written to the socket (which is either `localhost:2003` or it is determined by
/// command line parameters)
pub async fn start_repl() {
    let cfg_layout = load_yaml!("./cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let mut host = match matches.value_of("host") {
        Some(h) => h.to_owned(),
        None => ADDR.to_owned(),
    };
    host.push(':');
    match matches.value_of("port") {
        Some(p) => match p.parse::<u16>() {
            Ok(p) => host.push_str(&p.to_string()),
            Err(_) => {
                eprintln!("ERROR: Invalid port");
                process::exit(0x100);
            }
        },
        None => host.push_str("2003"),
    }
    let ssl = matches.value_of("cert");
    let mut con = if let Some(sslcert) = ssl {
        let con = match SslConnection::new(&host, sslcert).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("ERROR: {}", e);
                process::exit(0x100);
            }
        };
        Con::Secure(con)
    } else {
        let con = match Connection::new(&host).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("ERROR: {}", e);
                process::exit(0x100);
            }
        };
        Con::Insecure(con)
    };
    if let Some(eval_expr) = matches.value_of("eval") {
        if eval_expr.len() == 0 {
            return;
        }
        con.execute_query(eval_expr.to_string()).await;
        return;
    }
    println!("{}", MSG_WELCOME);
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
        if rl.len() == 0 {
            // The query was empty, so let it be
            continue;
        }
        con.execute_query(rl).await;
    }
}
