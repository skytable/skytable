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
mod protocol;
mod queryengine;
use coredb::CoreDB;
use dbnet::run;
use tokio::signal;
static ADDR: &'static str = "127.0.0.1:2003";

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind(ADDR).await.unwrap();
    println!("Server running on terrapipe://127.0.0.1:2003");
    // Start the server which asynchronously waits for a CTRL+C signal
    // which will safely shut down the server
    run(listener, signal::ctrl_c()).await;
}
