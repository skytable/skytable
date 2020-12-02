/*
 * Created on Wed Dec 02 2020
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

use bincode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct PartMap {
    partitions: Vec<Partition>,
}
impl PartMap {
    pub fn new(partitions: Vec<Partition>) -> Self {
        PartMap { partitions }
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Partition {
    name: String,
    len: usize,
}
impl Partition {
    pub fn new(begin: usize, end: usize, name: String) -> Self {
        Partition {
            name,
            len: (end - begin),
        }
    }
    pub fn len(&self) -> usize {
        self.len
    }
}

impl IntoIterator for PartMap {
    type Item = Partition;
    type IntoIter = std::vec::IntoIter<Partition>;

    fn into_iter(self) -> <Self as std::iter::IntoIterator>::IntoIter {
        self.partitions.into_iter()
    }
}
pub fn multi_ns_flush(ns: HashMap<&str, &HashMap<String, Vec<u8>>>) -> PartMap {
    let mut file = fs::File::create("snapstore.bin").unwrap();
    let mut partitions = Vec::new();
    let mut ns = ns.into_iter();
    let mut cur_offset = 0;
    while let Some((ns, ns_data)) = ns.next() {
        let start = cur_offset;
        let serialized = bincode::serialize(&ns_data).unwrap();
        cur_offset += serialized.len();
        file.write_all(&serialized).unwrap();
        partitions.push(Partition::new(start, cur_offset, ns.to_owned()));
        continue;
    }
    drop(file);
    let mut file = fs::File::create("snapstore.partmap").unwrap();
    let map = PartMap::new(partitions);
    file.write_all(&bincode::serialize(&map).unwrap()).unwrap();
    map
}

pub fn multi_ns_unflush() -> HashMap<String, HashMap<String, Vec<u8>>> {
    let pmap: PartMap = bincode::deserialize(&fs::read("snapstore.partmap").unwrap()).unwrap();
    let mut file = fs::File::open("snapstore.bin").unwrap();
    let mut map = pmap.into_iter();
    let mut hmaps: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
    while let Some(partition) = map.next() {
        let mut exact_op = vec![0; partition.len()];
        file.read_exact(&mut exact_op).unwrap();
        let tmp_map = bincode::deserialize(&exact_op).unwrap();
        hmaps.insert(partition.name, tmp_map);
    }
    hmaps
}

#[test]
fn test_multi_ns_flush() {
    let mut nsa = HashMap::new();
    nsa.insert("my".to_owned(), "ohmy".to_owned().into_bytes());
    nsa.insert("fly".to_owned(), "moondust".to_owned().into_bytes());
    let mut nsb = HashMap::new();
    nsb.insert("make".to_owned(), "melody".to_owned().into_bytes());
    nsb.insert("aurora".to_owned(), "shower".to_owned().into_bytes());
    let mut hm = HashMap::new();
    hm.insert("nsa", &nsa);
    hm.insert("nsb", &nsb);
    let _ = multi_ns_flush(hm);
    let mut hm_eq = HashMap::new();
    hm_eq.insert("nsa".to_owned(), nsa);
    hm_eq.insert("nsb".to_owned(), nsb);
    let unflushed = multi_ns_unflush();
    assert_eq!(unflushed, hm_eq);
}
