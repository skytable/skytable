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
//! `CyanSWF` short for Cyan "StreamingWriteFile" is a minimal implementation
//! of a file to which writes are _streamed_.
//!
//! What this essentially means — is that,
//! instead of taking a whole object and then writing it to the disk, like what persistent
//! storage did till v0.4.2, `CyanSWF` takes a part, encodes into its _kinda binary_
//! equivalent, and then writes that to disk. It then takes the next part, encodes it, and then writes it to disk.
//! The advantage of using this method is that the entire object does not need to be encoded at once
//! which has an additional memory and CPU time overhead.
//!
//! TODO: At this moment, this is specific to the core `HashMap`. However, in the future
//! a more generic implementation is to be made.

use crate::diskstore::TResult;
use std::fs::File;

/// The magic number that separates every piece of data from the other
const CYANSWF_MAGIC: u8 = 0xCA;

/// The streaming file writer
pub struct CyanSWF {
    /// The file to which data would be streamed into
    file: File,
}

impl CyanSWF {
    /// Create a new `CyanSWF` instance
    pub fn new(filename_and_path: &str) -> TResult<Self> {
        let file = File::create(filename_and_path)?;
        Ok(CyanSWF { file })
    }
}
