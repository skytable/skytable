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

use crate::client::Client;
use std::io::{self, prelude::*};
use tokio::signal;
const ADDR: &'static str = "127.0.0.1:2003";
pub async fn execute_query() {
    let mut client = match Client::new(ADDR).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
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
        client.run(rl, signal::ctrl_c()).await;
    }
}
