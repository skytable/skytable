/*
 * Created on Mon Sep 25 2023
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

/*
    Header specification
    ---
    We utilize two different kinds of headers:
    - Static header - Mostly to avoid data corruption
    - Variable header - For preserving dynamic information
*/

use {
    super::{
        rw::{RawFSInterface, SDSSFileIO},
        SDSSResult,
    },
    crate::{
        engine::storage::{
            header::{HostArch, HostEndian, HostOS, HostPointerWidth},
            v1::SDSSErrorKind,
            versions::{self, DriverVersion, HeaderVersion, ServerVersion},
        },
        util::os,
    },
    std::{
        mem::{transmute, ManuallyDrop},
        ops::Range,
    },
};

/*
    meta
*/

/// The file scope
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum FileScope {
    Journal = 0,
    DataBatch = 1,
    FlatmapData = 2,
}

impl FileScope {
    pub const fn try_new(id: u64) -> Option<Self> {
        Some(match id {
            0 => Self::Journal,
            1 => Self::DataBatch,
            2 => Self::FlatmapData,
            _ => return None,
        })
    }
    pub const fn new(id: u64) -> Self {
        match Self::try_new(id) {
            Some(v) => v,
            None => panic!("unknown filescope"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum FileSpecifier {
    GNSTxnLog = 0,
    TableDataBatch = 1,
    SysDB = 2,
    #[cfg(test)]
    TestTransactionLog = 0xFF,
}

impl FileSpecifier {
    pub const fn try_new(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::GNSTxnLog,
            1 => Self::TableDataBatch,
            2 => Self::SysDB,
            #[cfg(test)]
            0xFF => Self::TestTransactionLog,
            _ => return None,
        })
    }
    pub const fn new(v: u32) -> Self {
        match Self::try_new(v) {
            Some(v) => v,
            _ => panic!("unknown filespecifier"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileSpecifierVersion(u16);
impl FileSpecifierVersion {
    pub const fn __new(v: u16) -> Self {
        Self(v)
    }
}

const SDSS_MAGIC: u64 = 0x4F48534159414E21;

/// Specification for a SDSS file
pub trait FileSpec {
    /// The header spec for the file
    type Header: Header;
    /// Encode data
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs;
    /// Decode data
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs;
    /// Verify data
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs;
}

/*
    file spec impls
*/

#[cfg(test)]
pub struct TestFile;
#[cfg(test)]
impl FileSpec for TestFile {
    type Header = SDSSStaticHeaderV1Compact;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::FlatmapData,
        FileSpecifier::TestTransactionLog,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/// The file specification for the GNS transaction log (impl v1)
pub struct GNSTransactionLogV1;
impl FileSpec for GNSTransactionLogV1 {
    type Header = SDSSStaticHeaderV1Compact;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::Journal,
        FileSpecifier::GNSTxnLog,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/// The file specification for a journal batch
pub struct DataBatchJournalV1;
impl FileSpec for DataBatchJournalV1 {
    type Header = SDSSStaticHeaderV1Compact;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::DataBatch,
        FileSpecifier::TableDataBatch,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/// The file specification for the system db
pub struct SysDBV1;
impl FileSpec for SysDBV1 {
    type Header = SDSSStaticHeaderV1Compact;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::FlatmapData,
        FileSpecifier::SysDB,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/*
    header spec
*/

/// SDSS Header specification
pub trait Header: Sized {
    /// Encode arguments
    type EncodeArgs;
    /// Decode arguments
    type DecodeArgs;
    /// Decode verify arguments
    type DecodeVerifyArgs;
    /// Encode the header
    fn encode<Fs: RawFSInterface>(f: &mut SDSSFileIO<Fs>, args: Self::EncodeArgs)
        -> SDSSResult<()>;
    /// Decode the header
    fn decode<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        args: Self::DecodeArgs,
    ) -> SDSSResult<Self>;
    /// Verify the header
    fn verify(&self, args: Self::DecodeVerifyArgs) -> SDSSResult<()>;
    /// Decode and verify the header
    fn decode_verify<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        d_args: Self::DecodeArgs,
        v_args: Self::DecodeVerifyArgs,
    ) -> SDSSResult<Self> {
        let h = Self::decode(f, d_args)?;
        h.verify(v_args)?;
        Ok(h)
    }
}

/*
    header impls
*/

unsafe fn memcpy<const N: usize>(src: &[u8]) -> [u8; N] {
    let mut dst = [0u8; N];
    src.as_ptr().copy_to_nonoverlapping(dst.as_mut_ptr(), N);
    dst
}

macro_rules! var {
    (let $($name:ident),* $(,)?) => {
        $(let $name;)*
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
pub struct SDSSStaticHeaderV1Compact {
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
    genesis_static_file_class: FileScope,
    genesis_static_file_specifier: FileSpecifier,
    genesis_static_file_specifier_version: FileSpecifierVersion,
    // 2.2
    genesis_runtime_epoch_time: u128,
    // 3
    genesis_padding_block: [u8; 8],
}

impl SDSSStaticHeaderV1Compact {
    pub const SIZE: usize = 64;
    /// Decode and validate the full header block (validate ONLY; you must verify yourself)
    ///
    /// Notes:
    /// - Time might be inconsistent; verify
    /// - Compatibility requires additional intervention
    /// - If padding block was not zeroed, handle
    /// - No file metadata and is verified. Check!
    ///
    fn _decode(block: [u8; 64]) -> SDSSResult<Self> {
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
        let okay_header_version = raw_header_version == versions::CURRENT_HEADER_VERSION;
        let okay_server_version = raw_server_version == versions::CURRENT_SERVER_VERSION;
        let okay_driver_version = raw_driver_version == versions::CURRENT_DRIVER_VERSION;
        let okay = okay!(
            // 1.1 mgblk
            raw_magic == SDSS_MAGIC,
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
            raw_file_class <= FileScope::MAX,
            raw_file_specifier <= FileSpecifier::MAX,
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
                    genesis_static_file_class: transmute(raw_file_class),
                    genesis_static_file_specifier: transmute(raw_file_specifier),
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
                SDSSErrorKind::HeaderDecodeCorruptedHeader,
                SDSSErrorKind::HeaderDecodeVersionMismatch,
            ]);
            Err(unsafe {
                // UNSAFE(@ohsayan): while not needed, md for drop safety + correct index
                md.as_ptr().add(!version_okay as usize).read().into()
            })
        }
    }
}

impl SDSSStaticHeaderV1Compact {
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
    fn _encode(
        file_class: FileScope,
        file_specifier: FileSpecifier,
        file_specifier_version: FileSpecifierVersion,
        epoch_time: u128,
        padding_block: [u8; 8],
    ) -> [u8; 64] {
        let mut ret = [0; 64];
        // 1. mgblk
        ret[Self::SEG1_MAGIC].copy_from_slice(&SDSS_MAGIC.to_le_bytes());
        ret[Self::SEG1_HEADER_VERSION]
            .copy_from_slice(&versions::CURRENT_HEADER_VERSION.little_endian_u64());
        // 2.1.1
        ret[Self::SEG2_REC1_SERVER_VERSION]
            .copy_from_slice(&versions::CURRENT_SERVER_VERSION.little_endian());
        ret[Self::SEG2_REC1_DRIVER_VERSION]
            .copy_from_slice(&versions::CURRENT_DRIVER_VERSION.little_endian());
        // 2.1.2
        ret[Self::SEG2_REC1_HOST_OS] = HostOS::new().value_u8();
        ret[Self::SEG2_REC1_HOST_ARCH] = HostArch::new().value_u8();
        ret[Self::SEG2_REC1_HOST_PTR_WIDTH] = HostPointerWidth::new().value_u8();
        ret[Self::SEG2_REC1_HOST_ENDIAN] = HostEndian::new().value_u8();
        // 2.1.3
        ret[Self::SEG2_REC1_FILE_CLASS] = file_class.value_u8();
        ret[Self::SEG2_REC1_FILE_SPECIFIER] = file_specifier.value_u8();
        ret[Self::SEG2_REC1_FILE_SPECIFIER_VERSION]
            .copy_from_slice(&file_specifier_version.0.to_le_bytes());
        // 2.2
        ret[Self::SEG2_REC2_RUNTIME_EPOCH_TIME].copy_from_slice(&epoch_time.to_le_bytes());
        // 3
        ret[Self::SEG3_PADDING_BLK].copy_from_slice(&padding_block);
        ret
    }
    pub fn _encode_auto(
        file_class: FileScope,
        file_specifier: FileSpecifier,
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
}

impl SDSSStaticHeaderV1Compact {
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
    pub fn file_class(&self) -> FileScope {
        self.genesis_static_file_class
    }
    pub fn file_specifier(&self) -> FileSpecifier {
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

impl Header for SDSSStaticHeaderV1Compact {
    type EncodeArgs = (FileScope, FileSpecifier, FileSpecifierVersion);
    type DecodeArgs = ();
    type DecodeVerifyArgs = Self::EncodeArgs;
    fn encode<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        (scope, spec, spec_v): Self::EncodeArgs,
    ) -> SDSSResult<()> {
        let b = Self::_encode_auto(scope, spec, spec_v);
        f.fsynced_write(&b)
    }
    fn decode<Fs: RawFSInterface>(f: &mut SDSSFileIO<Fs>, _: Self::DecodeArgs) -> SDSSResult<Self> {
        let mut buf = [0u8; 64];
        f.read_to_buffer(&mut buf)?;
        Self::_decode(buf)
    }
    fn verify(&self, (scope, spec, spec_v): Self::DecodeVerifyArgs) -> SDSSResult<()> {
        if (self.file_class() == scope)
            & (self.file_specifier() == spec)
            & (self.file_specifier_version() == spec_v)
        {
            Ok(())
        } else {
            Err(SDSSErrorKind::HeaderDecodeDataMismatch.into())
        }
    }
}
