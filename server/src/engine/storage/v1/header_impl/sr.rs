/*
 * Created on Thu May 25 2023
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

use crate::engine::storage::{
    header::{StaticRecordUV, StaticRecordUVRaw},
    v1::{SDSSError, SDSSResult},
    versions,
};

#[derive(Debug, PartialEq, Clone)]
pub struct StaticRecord {
    sr: StaticRecordUV,
}

impl StaticRecord {
    /// Verified:
    /// - header version
    ///
    /// Need to verify: N/A
    pub fn verify(&self) -> SDSSResult<()> {
        if self.sr().header_version() == versions::v1::V1_HEADER_VERSION {
            Ok(())
        } else {
            return Err(SDSSError::HeaderDecodeHeaderVersionMismatch);
        }
    }
}

impl StaticRecord {
    pub const fn new(sr: StaticRecordUV) -> Self {
        Self { sr }
    }
    pub const fn encoded(&self) -> StaticRecordRaw {
        StaticRecordRaw {
            base: self.sr.encoded(),
        }
    }
    pub const fn sr(&self) -> &StaticRecordUV {
        &self.sr
    }
}

/// Static record
#[derive(Clone)]
pub struct StaticRecordRaw {
    pub(super) base: StaticRecordUVRaw,
}

impl StaticRecordRaw {
    pub const fn new_auto() -> Self {
        Self::new(StaticRecordUVRaw::create(versions::v1::V1_HEADER_VERSION))
    }
    pub const fn new(base: StaticRecordUVRaw) -> Self {
        Self { base }
    }
    pub const fn empty_buffer() -> [u8; sizeof!(Self)] {
        [0u8; sizeof!(Self)]
    }
    pub fn decode_noverify(buf: [u8; sizeof!(Self)]) -> Option<StaticRecord> {
        StaticRecordUVRaw::decode_from_bytes(buf).map(StaticRecord::new)
    }
}