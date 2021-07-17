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

use super::raw_byte_repr;
use crate::coredb::memstore::Memstore;
use std::io::Result as IoResult;
use std::io::Write;

const VERSION_MARK: u64 = 1u64.swap_bytes();

/// Add padding bytes to align to 8B boundaries
fn pad_nul_align8<W: Write>(l: usize, w: &mut W) -> IoResult<()> {
    // ignore handled amount
    let _ = w.write(&[b'0'].repeat(64 - l))?;
    Ok(())
}

/// Generate the `PRELOAD` disk file for this instance
/// ```text
/// [8B: Endian Mark/Version Mark (padded)] => Meta segment
/// [8B: Extent header] => Predata Segment
/// ([8B: Parition ID (nul padded)])* => Data segment
/// ```
///
/// The meta segment need not be 8B, but it is done for easier alignment
pub fn raw_generate_preload<W: Write>(w: &mut W, store: Memstore) -> IoResult<()> {
    unsafe {
        // generate the meta segment
        #[allow(clippy::identity_op)] // clippy doesn't understand endian
        let meta_segment = endian_mark!() | VERSION_MARK;
        w.write_all(&raw_byte_repr(&meta_segment))?;

        // generate and write the extent header (predata)
        w.write_all(&raw_byte_repr(&to_64bit_little_endian!(store
            .keyspaces
            .len())))?;
    }
    // start writing the parition IDs
    for partition in store.keyspaces.iter() {
        let partition_id = partition.key();
        w.write_all(&partition_id)?;
        // pad
        pad_nul_align8(partition_id.len(), w)?;
    }
    Ok(())
}
