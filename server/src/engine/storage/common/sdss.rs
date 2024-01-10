/*
 * Created on Wed Jan 10 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

/*!
    # SDSS spec

    This module provides traits and types to deal with the SDSS spec, especially headers.

    The static SDSS header block has a special segment that defines the header version which is static and will
    never change across any versions. While the same isn't warranted for the rest of the header, it's exceedingly
    unlikely that we'll ever change the static block ever.

    The only header that we currently use is [`CompactHeaderV1`].
*/

use {
    super::{
        static_meta::{HostArch, HostEndian, HostOS, HostPointerWidth, SDSS_MAGIC_8B},
        versions::{self, DriverVersion, FileSpecifierVersion, HeaderVersion, ServerVersion},
    },
    crate::{
        engine::{error::StorageError, mem::memcpy},
        util::os,
    },
    std::{
        mem::{transmute, ManuallyDrop},
        ops::Range,
    },
};

pub trait HeaderV1Enumeration {
    /// the maximum value of this enumeration
    const MAX: u8;
    /// Create a new enumeration, given that the maximum is validated
    unsafe fn new(x: u8) -> Self;
    /// Return the 1B repr of the enumeration
    fn repr_u8(&self) -> u8;
}

/// A trait that enables customizing the SDSS header for a specific file type
pub trait HeaderV1Spec {
    // types
    /// The file class type
    type FileClass: HeaderV1Enumeration + Copy;
    /// The file specifier type
    type FileSpecifier: HeaderV1Enumeration + Copy;
    // constants
    /// The server version to use during encode
    ///
    /// NB: This is NOT the compatible version but rather the current version
    const CURRENT_SERVER_VERSION: ServerVersion;
    /// The driver version to use during encode
    ///
    /// NB: This is NOT the compatible version but rather the current version
    const CURRENT_DRIVER_VERSION: DriverVersion;
    /// The file class to use and verify at encode/decode time
    /// check server version compatibility is valid at decode time
    fn check_if_server_version_compatible(v: ServerVersion) -> bool {
        v == Self::CURRENT_SERVER_VERSION
    }
    /// check driver version compatibility is valid at decode time
    fn check_if_driver_version_compatible(v: DriverVersion) -> bool {
        v == Self::CURRENT_DRIVER_VERSION
    }
}

/*
    Compact SDSS Header v1
    ---
    - 1: Magic block (16B): magic + header version
    - 2: Static block (40B):
        - 2.1: Genesis static record (24B)
            - 2.1.1: Software information (16B)
                - Server version (8B)
                - Driver version (8B)
            - 2.1.2: Host information (4B):
                - OS (1B)
                - Arch (1B)
                - Pointer width (1B)
                - Endian (1B)
            - 2.1.3: File information (4B):
                - File class (1B)
                - File specifier (1B)
                - File specifier version (2B)
        - 2.2: Genesis runtime record (16B)
            - Host epoch (16B)
    - 3: Padding block (8B)
*/

#[repr(align(8))]
#[derive(Debug, PartialEq)]
pub struct HeaderV1<H: HeaderV1Spec> {
    // 1 magic block
    magic_header_version: HeaderVersion,
    // 2.1.1
    genesis_static_sw_server_version: ServerVersion,
    genesis_static_sw_driver_version: DriverVersion,
    // 2.1.2
    genesis_static_host_os: HostOS,
    genesis_static_host_arch: HostArch,
    genesis_static_host_ptr_width: HostPointerWidth,
    genesis_static_host_endian: HostEndian,
    // 2.1.3
    genesis_static_file_class: H::FileClass,
    genesis_static_file_specifier: H::FileSpecifier,
    genesis_static_file_specifier_version: FileSpecifierVersion,
    // 2.2
    genesis_runtime_epoch_time: u128,
    // 3
    genesis_padding_block: [u8; 8],
}

#[allow(unused)]
impl<H: HeaderV1Spec> HeaderV1<H> {
    const SEG1_MAGIC: Range<usize> = 0..8;
    const SEG1_HEADER_VERSION: Range<usize> = 8..16;
    const SEG2_REC1_SERVER_VERSION: Range<usize> = 16..24;
    const SEG2_REC1_DRIVER_VERSION: Range<usize> = 24..32;
    const SEG2_REC1_HOST_OS: usize = 32;
    const SEG2_REC1_HOST_ARCH: usize = 33;
    const SEG2_REC1_HOST_PTR_WIDTH: usize = 34;
    const SEG2_REC1_HOST_ENDIAN: usize = 35;
    const SEG2_REC1_FILE_CLASS: usize = 36;
    const SEG2_REC1_FILE_SPECIFIER: usize = 37;
    const SEG2_REC1_FILE_SPECIFIER_VERSION: Range<usize> = 38..40;
    const SEG2_REC2_RUNTIME_EPOCH_TIME: Range<usize> = 40..56;
    const SEG3_PADDING_BLK: Range<usize> = 56..64;
    pub const SIZE: usize = 64;
    fn _encode(
        file_class: H::FileClass,
        file_specifier: H::FileSpecifier,
        file_specifier_version: FileSpecifierVersion,
        epoch_time: u128,
        padding_block: [u8; 8],
    ) -> [u8; 64] {
        let mut ret = [0; 64];
        // 1. mgblk
        ret[Self::SEG1_MAGIC].copy_from_slice(&SDSS_MAGIC_8B.to_le_bytes());
        ret[Self::SEG1_HEADER_VERSION]
            .copy_from_slice(&versions::v1::V1_HEADER_VERSION.little_endian_u64());
        // 2.1.1
        ret[Self::SEG2_REC1_SERVER_VERSION]
            .copy_from_slice(&versions::v1::V1_SERVER_VERSION.little_endian());
        ret[Self::SEG2_REC1_DRIVER_VERSION]
            .copy_from_slice(&versions::v1::V1_DRIVER_VERSION.little_endian());
        // 2.1.2
        ret[Self::SEG2_REC1_HOST_OS] = HostOS::new().value_u8();
        ret[Self::SEG2_REC1_HOST_ARCH] = HostArch::new().value_u8();
        ret[Self::SEG2_REC1_HOST_PTR_WIDTH] = HostPointerWidth::new().value_u8();
        ret[Self::SEG2_REC1_HOST_ENDIAN] = HostEndian::new().value_u8();
        // 2.1.3
        ret[Self::SEG2_REC1_FILE_CLASS] = file_class.repr_u8();
        ret[Self::SEG2_REC1_FILE_SPECIFIER] = file_specifier.repr_u8();
        ret[Self::SEG2_REC1_FILE_SPECIFIER_VERSION]
            .copy_from_slice(&file_specifier_version.little_endian());
        // 2.2
        ret[Self::SEG2_REC2_RUNTIME_EPOCH_TIME].copy_from_slice(&epoch_time.to_le_bytes());
        // 3
        ret[Self::SEG3_PADDING_BLK].copy_from_slice(&padding_block);
        ret
    }
    pub fn _encode_auto(
        file_class: H::FileClass,
        file_specifier: H::FileSpecifier,
        file_specifier_version: FileSpecifierVersion,
    ) -> [u8; 64] {
        let epoch_time = os::get_epoch_time();
        Self::_encode(
            file_class,
            file_specifier,
            file_specifier_version,
            epoch_time,
            [0; 8],
        )
    }
    /// Decode and validate the full header block (validate ONLY; you must verify yourself)
    ///
    /// Notes:
    /// - Time might be inconsistent; verify
    /// - Compatibility requires additional intervention
    /// - If padding block was not zeroed, handle
    /// - No file metadata and is verified. Check!
    ///
    pub fn decode(block: [u8; 64]) -> Result<Self, StorageError> {
        var!(let raw_magic, raw_header_version, raw_server_version, raw_driver_version, raw_host_os, raw_host_arch,
            raw_host_ptr_width, raw_host_endian, raw_file_class, raw_file_specifier, raw_file_specifier_version,
            raw_runtime_epoch_time, raw_paddding_block,
        );
        macro_rules! u64 {
            ($pos:expr) => {
                u64::from_le_bytes(memcpy(&block[$pos]))
            };
        }
        unsafe {
            // UNSAFE(@ohsayan): all segments are correctly accessed (aligned to u8)
            raw_magic = u64!(Self::SEG1_MAGIC);
            raw_header_version = HeaderVersion::__new(u64!(Self::SEG1_HEADER_VERSION));
            raw_server_version = ServerVersion::__new(u64!(Self::SEG2_REC1_SERVER_VERSION));
            raw_driver_version = DriverVersion::__new(u64!(Self::SEG2_REC1_DRIVER_VERSION));
            raw_host_os = block[Self::SEG2_REC1_HOST_OS];
            raw_host_arch = block[Self::SEG2_REC1_HOST_ARCH];
            raw_host_ptr_width = block[Self::SEG2_REC1_HOST_PTR_WIDTH];
            raw_host_endian = block[Self::SEG2_REC1_HOST_ENDIAN];
            raw_file_class = block[Self::SEG2_REC1_FILE_CLASS];
            raw_file_specifier = block[Self::SEG2_REC1_FILE_SPECIFIER];
            raw_file_specifier_version = FileSpecifierVersion::__new(u16::from_le_bytes(memcpy(
                &block[Self::SEG2_REC1_FILE_SPECIFIER_VERSION],
            )));
            raw_runtime_epoch_time =
                u128::from_le_bytes(memcpy(&block[Self::SEG2_REC2_RUNTIME_EPOCH_TIME]));
            raw_paddding_block = memcpy::<8>(&block[Self::SEG3_PADDING_BLK]);
        }
        macro_rules! okay {
            ($($expr:expr),* $(,)?) => {
                $(($expr) &)*true
            }
        }
        let okay_header_version = raw_header_version == versions::HEADER_V1;
        let okay_server_version = H::check_if_server_version_compatible(raw_server_version);
        let okay_driver_version = H::check_if_driver_version_compatible(raw_driver_version);
        let okay = okay!(
            // 1.1 mgblk
            raw_magic == SDSS_MAGIC_8B,
            okay_header_version,
            // 2.1.1
            okay_server_version,
            okay_driver_version,
            // 2.1.2
            raw_host_os <= HostOS::MAX,
            raw_host_arch <= HostArch::MAX,
            raw_host_ptr_width <= HostPointerWidth::MAX,
            raw_host_endian <= HostEndian::MAX,
            // 2.1.3
            raw_file_class <= H::FileClass::MAX,
            raw_file_specifier <= H::FileSpecifier::MAX,
        );
        if okay {
            Ok(unsafe {
                // UNSAFE(@ohsayan): the block ranges are very well defined
                Self {
                    // 1.1
                    magic_header_version: raw_header_version,
                    // 2.1.1
                    genesis_static_sw_server_version: raw_server_version,
                    genesis_static_sw_driver_version: raw_driver_version,
                    // 2.1.2
                    genesis_static_host_os: transmute(raw_host_os),
                    genesis_static_host_arch: transmute(raw_host_arch),
                    genesis_static_host_ptr_width: transmute(raw_host_ptr_width),
                    genesis_static_host_endian: transmute(raw_host_endian),
                    // 2.1.3
                    genesis_static_file_class: H::FileClass::new(raw_file_class),
                    genesis_static_file_specifier: H::FileSpecifier::new(raw_file_specifier),
                    genesis_static_file_specifier_version: raw_file_specifier_version,
                    // 2.2
                    genesis_runtime_epoch_time: raw_runtime_epoch_time,
                    // 3
                    genesis_padding_block: raw_paddding_block,
                }
            })
        } else {
            let version_okay = okay_header_version & okay_server_version & okay_driver_version;
            let md = ManuallyDrop::new([
                StorageError::HeaderDecodeCorruptedHeader,
                StorageError::HeaderDecodeVersionMismatch,
            ]);
            Err(unsafe {
                // UNSAFE(@ohsayan): while not needed, md for drop safety + correct index
                md.as_ptr().add(!version_okay as usize).read().into()
            })
        }
    }
}

#[allow(unused)]
impl<H: HeaderV1Spec> HeaderV1<H> {
    pub fn header_version(&self) -> HeaderVersion {
        self.magic_header_version
    }
    pub fn server_version(&self) -> ServerVersion {
        self.genesis_static_sw_server_version
    }
    pub fn driver_version(&self) -> DriverVersion {
        self.genesis_static_sw_driver_version
    }
    pub fn host_os(&self) -> HostOS {
        self.genesis_static_host_os
    }
    pub fn host_arch(&self) -> HostArch {
        self.genesis_static_host_arch
    }
    pub fn host_ptr_width(&self) -> HostPointerWidth {
        self.genesis_static_host_ptr_width
    }
    pub fn host_endian(&self) -> HostEndian {
        self.genesis_static_host_endian
    }
    pub fn file_class(&self) -> H::FileClass {
        self.genesis_static_file_class
    }
    pub fn file_specifier(&self) -> H::FileSpecifier {
        self.genesis_static_file_specifier
    }
    pub fn file_specifier_version(&self) -> FileSpecifierVersion {
        self.genesis_static_file_specifier_version
    }
    pub fn epoch_time(&self) -> u128 {
        self.genesis_runtime_epoch_time
    }
    pub fn padding_block(&self) -> [u8; 8] {
        self.genesis_padding_block
    }
}
