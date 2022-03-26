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

//! # Preload binary files
//!
//! Preloads are very critical binary files which contain metadata for this instance of
//! the database. Preloads are of two kinds:
//! 1. the `PRELOAD` that is placed at the root directory
//! 2. the `PARTMAP` preload that is placed in the ks directory
//!

use crate::corestore::memstore::Memstore;
use crate::corestore::memstore::ObjectID;
use crate::storage::v1::error::{StorageEngineError, StorageEngineResult};
use crate::IoResult;
use core::ptr;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;

pub type LoadedPartfile = HashMap<ObjectID, (u8, u8)>;

// our version and endian are based on nibbles

const META_SEGMENT_LE: u8 = 0b1000_0000;
const META_SEGMENT_BE: u8 = 0b1000_0001;

#[cfg(target_endian = "little")]
const META_SEGMENT: u8 = META_SEGMENT_LE;

#[cfg(target_endian = "big")]
const META_SEGMENT: u8 = META_SEGMENT_BE;

/// Generate the `PRELOAD` disk file for this instance
/// ```text
/// [1B: Endian Mark/Version Mark (padded)] => Meta segment
/// [8B: Extent header] => Predata Segment
/// ([8B: Partion ID len][8B: Parition ID (not padded)])* => Data segment
/// ```
///
pub(super) fn raw_generate_preload<W: Write>(w: &mut W, store: &Memstore) -> IoResult<()> {
    // generate the meta segment
    w.write_all(&[META_SEGMENT])?;
    super::se::raw_serialize_set(&store.keyspaces, w)?;
    Ok(())
}

/// Reads the preload file and returns a set
pub(super) fn read_preload_raw(preload: Vec<u8>) -> StorageEngineResult<HashSet<ObjectID>> {
    if preload.len() < 16 {
        // nah, this is a bad disk file
        return Err(StorageEngineError::corrupted_preload());
    }
    // first read in the meta segment
    unsafe {
        let meta_segment: u8 = ptr::read(preload.as_ptr());
        match meta_segment {
            META_SEGMENT_BE => {
                super::iter::endian_set_big();
            }
            META_SEGMENT_LE => {
                super::iter::endian_set_little();
            }
            _ => return Err(StorageEngineError::BadMetadata("preload".into())),
        }
    }
    // all checks complete; time to decode
    super::de::deserialize_set_ctype(&preload[1..])
        .ok_or_else(StorageEngineError::corrupted_preload)
}
