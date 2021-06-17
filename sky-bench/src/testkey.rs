/*
 * Created on Thu Jun 17 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use crate::hoststr;
use crate::sanity_test;
use crate::util::ran_string;
use libstress::Workpool;
use rand::thread_rng;
use std::io::{Read, Write};
use std::net::{self, TcpStream};

pub fn create_testkeys(host: &str, port: u16, num: usize, connections: usize, size: usize) {
    sanity_test!(host, port);
    let host = hoststr!(host, port);
    let mut rand = thread_rng();
    let np = Workpool::new(
        connections,
        move || TcpStream::connect(host.clone()).unwrap(),
        |sock, packet: Vec<u8>| {
            sock.write_all(&packet).unwrap();
            let _ = sock.read(&mut vec![0; 1024]).unwrap();
        },
        |socket| {
            socket.shutdown(net::Shutdown::Both).unwrap();
        },
        true,
    );
    println!("Generating keys ...");
    let keys: Vec<String> = (0..num)
        .into_iter()
        .map(|_| ran_string(size, &mut rand))
        .collect();
    let values: Vec<String> = (0..num)
        .into_iter()
        .map(|_| ran_string(size, &mut rand))
        .collect();
    let set_packs: Vec<Vec<u8>> = (0..num)
        .map(|idx| libsky::into_raw_query(&format!("SET {} {}", keys[idx], values[idx])))
        .collect();
    set_packs.into_iter().for_each(|packet| {
        np.execute(packet);
    });
    drop(np);
    println!("Created mock keys successfully");
}
