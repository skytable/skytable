/*
 * Created on Sat Jul 17 2021
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

use crate::coredb::memstore::Memstore;
use crate::coredb::memstore::ObjectID;
use core::ptr;
use std::collections::HashSet;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::io::Write;

// our version and endian are based on nibbles

#[cfg(target_endian = "little")]
const META_SEGMENT: u8 = 0b1000_0000;

#[cfg(target_endian = "big")]
const META_SEGMENT: u8 = 0b1000_0001;

const VERSION: u8 = 1;

/// Generate the `PRELOAD` disk file for this instance
/// ```text
/// [1B: Endian Mark/Version Mark (padded)] => Meta segment
/// [8B: Extent header] => Predata Segment
/// ([8B: Partion ID len][8B: Parition ID (not padded)])* => Data segment
/// ```
///
pub fn raw_generate_preload<W: Write>(w: &mut W, store: &Memstore) -> IoResult<()> {
    // generate the meta segment
    #[allow(clippy::identity_op)]
    w.write_all(&[META_SEGMENT])?;
    super::raw_serialize_set(&store.keyspaces, w)?;
    Ok(())
}

pub fn read_preload(preload: Vec<u8>) -> IoResult<HashSet<ObjectID>> {
    if preload.len() < 16 {
        // nah, this is a bad disk file
        return Err(IoError::from(ErrorKind::UnexpectedEof));
    }
    // first read in the meta segment
    unsafe {
        let meta_segment: u8 = ptr::read(preload.as_ptr());
        if meta_segment != META_SEGMENT {
            return Err(IoError::from(ErrorKind::Unsupported));
        }
    }
    // all checks complete; time to decode
    let ret = super::deserialize_set_ctype(&preload[1..]);
    match ret {
        Some(ret) => Ok(ret),
        _ => Err(IoError::from(ErrorKind::UnexpectedEof)),
    }
}
