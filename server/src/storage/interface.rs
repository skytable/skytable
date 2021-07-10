/*
 * Created on Sat Jul 10 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! Interfaces with the file system

use super::PartitionID;
use crate::coredb::htable::Coremap;
use crate::coredb::htable::Data;
use std::fs;
use std::io::Result as IoResult;
use std::io::{BufWriter, Write};
use std::thread::{self, JoinHandle};

/// Uses a buffered writer under the hood to improve write performance as the provided
/// writable interface might be very slow. The buffer does flush once done, however, it
/// is important that you fsync yourself!
pub fn serialize_map_into_slow_buffer<T: Write>(
    buffer: &mut T,
    map: &Coremap<Data, Data>,
) -> std::io::Result<()> {
    let mut buffer = BufWriter::new(buffer);
    super::raw_serialize_map(map, &mut buffer)?;
    buffer.flush()?;
    Ok(())
}

/// Get the file for COW. If the parition ID is 0000
fn cow_file(id: PartitionID) -> String {
    let mut id = unsafe { super::raw_byte_repr(&id) }.to_owned();
    id.push(b'_');
    unsafe { String::from_utf8_unchecked(id) }
}

/// Returns a handle to a thread that was spawned to handle this specific flush routine
pub fn threaded_se(
    tblref: Coremap<Data, Data>,
    partition_id: PartitionID,
) -> JoinHandle<IoResult<()>> {
    thread::spawn(move || {
        let mut f = fs::File::create(cow_file(partition_id))?;
        self::serialize_map_into_slow_buffer(&mut f, &tblref)?;
        f.sync_all()?;
        Ok(())
    })
}
