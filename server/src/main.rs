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

use tokio::net::TcpListener;
mod coredb;
mod dbnet;
mod diskstore;
mod kvengine;
mod protocol;
mod queryengine;
mod resp;
use coredb::CoreDB;
use dbnet::run;
use tokio::signal;
#[cfg(test)]
mod tests;
static ADDR: &'static str = "127.0.0.1:2003";
static MSG: &'static str = "TerrabaseDB v0.3.2 | https://github.com/terrabasedb/terrabase\nServer running on terrapipe://127.0.0.1:2003";
static TEXT: &'static str = " 
      _______                       _                        _____   ____  
     |__   __|                     | |                      |  __ \\ |  _ \\ 
        | |  ___  _ __  _ __  __ _ | |__    __ _  ___   ___ | |  | || |_) |
        | | / _ \\| '__|| '__|/ _` || '_ \\  / _` |/ __| / _ \\| |  | ||  _ < 
        | ||  __/| |   | |  | (_| || |_) || (_| |\\__ \\|  __/| |__| || |_) |
        |_| \\___||_|   |_|   \\__,_||_.__/  \\__,_||___/ \\___||_____/ |____/
        
        +-++-++-+ +-++-++-++-+ +-++-++-++-++-+ +-++-++-++-++-++-++-++-+
        |T||h||e| |n||e||x||t| |N||o||S||Q||L| |d||a||t||a||b||a||s||e|
        +-++-++-+ +-++-++-++-+ +-++-++-++-++-+ +-++-++-++-++-++-++-++-+    
";
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("{}\n{}", TEXT, MSG);
    // Start the server which asynchronously waits for a CTRL+C signal
    // which will safely shut down the server
    run(listener, signal::ctrl_c()).await;
}
