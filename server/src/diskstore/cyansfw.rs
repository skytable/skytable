/*
 * Created on Sat Sep 19 2020
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

//! # `CyanSWF`
//! `CyanSWF` short for Cyan "Streaming File Writer" is a minimal implementation
//! of a file to which writes are _streamed_.
//!
//! What this essentially means — is that,
//! instead of taking a whole object and then writing it to the disk, like what persistent
//! storage did till v0.4.3-alpha.1, `CyanSWF` takes a part, encodes into its _kinda binary_
//! equivalent, and then writes that to disk. It then takes the next part, encodes it, and then writes it to disk.
//! The advantage of using this method is that the entire object does not need to be encoded at once
//! which has an additional memory and CPU time overhead.
//!
//! TODO: At this moment, this is specific to the core `HashMap`. However, in the future
//! a more generic implementation is to be made.

#![allow(dead_code)]

use crate::diskstore::TResult;
use std::fs::File;

/// The magic number that separates every piece of data from the other
const CYANSWF_MAGIC: u8 = 0xCA;

/// # Streaming file writer for `CyanSS`
///
/// `CyanSS` or Cyan Snapstore is a file format that will be used by TDB for persistent
/// storage, backups, etc.
///
/// This is what the file looks like:
///
/// ```text
///  CYANSWF$DDMMYYYY$NANOTIME
///  __kvstore_begin
///  ---- DATA -----
/// ___kvstore_end
/// ```
///
/// Here,
/// - `DDMMMYYYY` - Is the date in DDMMYYYY format, which reflects when this
/// file was created
/// - `NANOTIME` - Is the time in nanoseconds when the file was created
/// - `DATA` is, well, the data
///
/// The `__kvstore_begin` and `__kvstore_end` are _partition flags_, which separate different
/// data types. As of now, we support k/v pairs, so it is `kvstore`. If we generalize this,
/// it would look like: `__<datatype>_begin` or `__<datatype>_end`.
///
pub struct CyanSFW {
    /// The file to which data would be streamed into
    file: File,
}

impl CyanSFW {
    /// Create a new `CyanSWF` instance
    pub fn new(filename_and_path: &str) -> TResult<Self> {
        let file = File::create(filename_and_path)?;
        Ok(CyanSFW { file })
    }
}
