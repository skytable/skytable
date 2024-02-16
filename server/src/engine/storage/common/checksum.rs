/*
 * Created on Sun Sep 03 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

//! # Checksum utils
//!
//! This module contains utils for handling checksums
//!

use {
    crc::{Crc, Digest, CRC_64_XZ},
    std::fmt,
};

/*
    NOTE(@ohsayan): we're currently using crc's impl. but the reason I decided to make a wrapper is because I have a
    different impl in mind
*/

const CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_XZ);

#[derive(Clone)]
pub struct SCrc64 {
    digest: Digest<'static, u64>,
}

impl Default for SCrc64 {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SCrc64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SCrc64")
            .field("digest", &self.digest.clone().finalize())
            .finish()
    }
}

impl SCrc64 {
    pub const fn new() -> Self {
        Self {
            digest: CRC64.digest(),
        }
    }
    pub fn update(&mut self, b: &[u8]) {
        self.digest.update(b)
    }
    pub fn finish(self) -> u64 {
        self.digest.finalize()
    }
}
