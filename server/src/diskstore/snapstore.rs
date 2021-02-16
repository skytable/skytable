/*
 * Created on Wed Dec 02 2020
 *
 * This file is a part of Skybase
 * Skybase (formerly known as TerrabaseDB) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! # Snapstore
//!
//! Snapstore is an extremely fundamental but powerful disk storage format which comprises of two parts:
//! 1. The data file (`.bin`)
//! 2. The partition map (`.partmap`)
//!
//! ## The data file
//!
//! Well, the data file, contains data! Jokes aside, the data file contains the serialized equivalent of
//! multiple namespaces.
//!
//! ## The partition map
//!
//! The partition map file is the serialized equivalent of the `Partition` data structure. When
//! deserialized, this file gives us partition _markers_ or byte positions. Using these positions, we can
//! read in multiple namespaces (virtually an infinite number of them, provided that they can reside in memory)
//! and give their "real" equivalents. Another advent of this method is that we can use separate threads
//! for reading in data, in the event that there is a lot of data to be read, and this data is spread
//! over multiple namespaces.
//!
//! > In other words, the `snapstore.bin` file is completely useless without the `snapstore.partmap` file; so,
//! if you happen to lose it — have a good day!

use bincode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::prelude::*;

pub trait IntoBinaryData: Serialize {
    fn into_bin(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

impl<'a> IntoBinaryData for &'a HashMap<String, Vec<u8>> {}

#[derive(Serialize, Deserialize, Debug)]
/// The `PartMap` is a partition map which contains metadata about multiple partitions stored in a
/// snapstore file. The `PartMap` holds all of this data in a simple `Vec`tor to make life easier.
pub struct PartMap {
    /// The vector of partitions
    partitions: Vec<Partition>,
}
impl PartMap {
    /// Create a new partition map using existing `Partition` data
    pub const fn new(partitions: Vec<Partition>) -> Self {
        PartMap { partitions }
    }
}

#[derive(Serialize, Deserialize, Debug)]
/// A `Partition` contains a partition marker or `end` and `begin` values which demarcate the location
/// of this partition in the data file
pub struct Partition {
    /// The name of the partition
    name: String,
    /// The ending byte of this partition
    end: usize,
    /// The starting byte of this partition
    begin: usize,
}

impl Partition {
    /// Create a new `Partition` using existing partitioning data
    pub const fn new(begin: usize, end: usize, name: String) -> Self {
        Partition { name, end, begin }
    }
    /// Get the size of the partition
    pub const fn len(&self) -> usize {
        self.end - self.begin
    }
}

// We implement `IntoIterator` for `PartMap` so that we can use it for sequentially deserializing
// partitions
impl IntoIterator for PartMap {
    type Item = Partition;
    type IntoIter = std::vec::IntoIter<Partition>;

    fn into_iter(self) -> <Self as std::iter::IntoIterator>::IntoIter {
        self.partitions.into_iter()
    }
}

/// Flush the data of multiple namespaces
///
/// This function creates two files: `snapstore.bin` and `snapstore.partmap`; the former is the data file
/// and the latter one is the partition map, or simply put, the partition metadata file. This function
/// accepts a `HashMap` of `HashMap`s with the key being the name of the partition.
pub fn flush_multi_ns<T>(ns: HashMap<&str, T>) -> Result<(), Box<dyn Error>>
where
    T: IntoBinaryData,
{
    // Create the data file first
    let mut file = fs::File::create("snapstore.bin")?;
    // This contains the partitions for the `PartMap` object
    let mut partitions = Vec::new();
    // Create an iterator over the namespaces and their corresponding data
    let mut ns = ns.into_iter();
    // The offset from the starting byte we are at currently
    let mut cur_offset = 0;
    while let Some((ns, ns_data)) = ns.next() {
        // We keep `start` to be the cur_offset, even if it is zero, since we're going to read it sequentially
        // TODO: Enable non-sequential or "jumpy" reading
        let start = cur_offset;
        // Serialize the data
        let serialized = ns_data.into_bin();
        // We will write these many bytes to the file, so move the offset ahead
        cur_offset += serialized.len();
        // Now write the data
        file.write_all(&serialized)?;
        // Add this partition data to our vector of partitions
        partitions.push(Partition::new(start, cur_offset, ns.to_owned()));
        continue;
    }
    drop(file);
    // Now create the partition map file
    let mut file = fs::File::create("snapstore.partmap")?;
    let map = PartMap::new(partitions);
    // Serialize the partition map and write it to disk
    file.write_all(&bincode::serialize(&map)?)?;
    // We're done here
    Ok(())
}

/// This function restores the 'named' namespaces from disk
///
/// This function expects two things:
/// 1. You should have a data file called 'snapstore.bin'
/// 2. You should have a partition map or partition metadata file called 'snapstore.partmap'
///
/// Once these requirements are met, the file will return a `HashMap` of named partitions which
/// can be used as required
pub fn unflush_multi_ns() -> Result<HashMap<String, HashMap<String, Vec<u8>>>, Box<dyn Error>> {
    // Try to read the partition map
    let pmap: PartMap = bincode::deserialize(&fs::read("snapstore.partmap")?)?;
    // Now read the data file
    let mut file = fs::File::open("snapstore.bin")?;
    // Get an iterator over the namespace data from the partition map
    let mut map = pmap.into_iter();
    let mut hmaps: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
    while let Some(partition) = map.next() {
        // Create an empty buffer which will read precisely `len()` bytes from the file
        let mut exact_op = vec![0; partition.len()];
        // Now read this data
        file.read_exact(&mut exact_op)?;
        // Deserialize this chunk
        let tmp_map = bincode::deserialize(&exact_op)?;
        // Insert the deserialized equivalent into our `HashMap` of `HashMap`s
        hmaps.insert(partition.name, tmp_map);
    }
    Ok(hmaps)
}

#[test]
fn test_flush_multi_ns() {
    let mut nsa = HashMap::new();
    nsa.insert("my".to_owned(), "ohmy".to_owned().into_bytes());
    nsa.insert("fly".to_owned(), "moondust".to_owned().into_bytes());
    let mut nsb = HashMap::new();
    nsb.insert("make".to_owned(), "melody".to_owned().into_bytes());
    nsb.insert("aurora".to_owned(), "shower".to_owned().into_bytes());
    let mut hm = HashMap::new();
    hm.insert("nsa", &nsa);
    hm.insert("nsb", &nsb);
    let _ = flush_multi_ns(hm).unwrap();
    let mut hm_eq = HashMap::new();
    hm_eq.insert("nsa".to_owned(), nsa);
    hm_eq.insert("nsb".to_owned(), nsb);
    let unflushed = unflush_multi_ns().unwrap();
    assert_eq!(unflushed, hm_eq);
}
