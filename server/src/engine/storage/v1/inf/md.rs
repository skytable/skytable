/*
 * Created on Mon Aug 21 2023
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

use super::super::{rw::BufferedScanner, SDSSError, SDSSResult};

/// metadata spec for a persist map entry
pub trait PersistObjectMD: Sized {
    /// set to true if decode is infallible once the MD payload has been verified
    const MD_DEC_INFALLIBLE: bool;
    /// returns true if the current buffered source can be used to decode the metadata (self)
    fn pretest_src_for_metadata_dec(scanner: &BufferedScanner) -> bool;
    /// returns true if per the metadata and the current buffered source, the target object in question can be decoded
    fn pretest_src_for_object_dec(&self, scanner: &BufferedScanner) -> bool;
    /// decode the metadata
    unsafe fn dec_md_payload(scanner: &mut BufferedScanner) -> Option<Self>;
}

/// Metadata for a simple size requirement
pub struct SimpleSizeMD<const N: usize>;

impl<const N: usize> PersistObjectMD for SimpleSizeMD<N> {
    const MD_DEC_INFALLIBLE: bool = true;
    fn pretest_src_for_metadata_dec(scanner: &BufferedScanner) -> bool {
        scanner.has_left(N)
    }
    fn pretest_src_for_object_dec(&self, _: &BufferedScanner) -> bool {
        true
    }
    unsafe fn dec_md_payload(_: &mut BufferedScanner) -> Option<Self> {
        Some(Self)
    }
}

/// For wrappers and other complicated metadata handling, set this to the metadata type
pub struct VoidMetadata;

impl PersistObjectMD for VoidMetadata {
    const MD_DEC_INFALLIBLE: bool = true;
    fn pretest_src_for_metadata_dec(_: &BufferedScanner) -> bool {
        true
    }
    fn pretest_src_for_object_dec(&self, _: &BufferedScanner) -> bool {
        true
    }
    unsafe fn dec_md_payload(_: &mut BufferedScanner) -> Option<Self> {
        Some(Self)
    }
}

/// Decode metadata
///
/// ## Safety
/// unsafe because you need to set whether you've already verified the metadata or not
pub(super) unsafe fn dec_md<Md: PersistObjectMD, const ASSUME_PRETEST_PASS: bool>(
    scanner: &mut BufferedScanner,
) -> SDSSResult<Md> {
    if ASSUME_PRETEST_PASS || Md::pretest_src_for_metadata_dec(scanner) {
        match Md::dec_md_payload(scanner) {
            Some(md) => Ok(md),
            None => {
                if Md::MD_DEC_INFALLIBLE {
                    impossible!()
                } else {
                    Err(SDSSError::InternalDecodeStructureCorrupted)
                }
            }
        }
    } else {
        Err(SDSSError::InternalDecodeStructureCorrupted)
    }
}
