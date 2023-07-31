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

use crate::{
    engine::{
        mem::ByteStack,
        storage::{
            header::{HostArch, HostEndian, HostOS, HostPointerWidth},
            v1::{header_impl::FileSpecifierVersion, SDSSError, SDSSResult},
            versions::{self, DriverVersion, ServerVersion},
        },
    },
    util,
};

/*
    Dynamic record (1/2): Host signature
    ---
    - 8B: Server version
    - 8B: Driver version
    - 4B: File specifier ID
    - 1B: Endian
    - 1B: Pointer width
    - 1B: Arch
    - 1B: OS
*/

#[derive(Debug, PartialEq, Clone)]
pub struct DRHostSignature {
    server_version: ServerVersion,
    driver_version: DriverVersion,
    file_specifier_version: FileSpecifierVersion,
    endian: HostEndian,
    ptr_width: HostPointerWidth,
    arch: HostArch,
    os: HostOS,
}

impl DRHostSignature {
    pub fn verify(&self, expected_file_specifier_version: FileSpecifierVersion) -> SDSSResult<()> {
        if self.server_version() != versions::v1::V1_SERVER_VERSION {
            return Err(SDSSError::ServerVersionMismatch);
        }
        if self.driver_version() != versions::v1::V1_DRIVER_VERSION {
            return Err(SDSSError::DriverVersionMismatch);
        }
        if self.file_specifier_version() != expected_file_specifier_version {
            return Err(SDSSError::HeaderDataMismatch);
        }
        Ok(())
    }
}

impl DRHostSignature {
    /// Decode the [`DRHostSignature`] from the given bytes
    ///
    /// **☢ WARNING ☢: This only decodes; it doesn't validate expected values!**
    pub fn decode_noverify(bytes: [u8; sizeof!(DRHostSignatureRaw)]) -> Option<Self> {
        let ns = ByteStack::new(bytes);
        let server_version = ServerVersion::__new(u64::from_le(
            ns.read_qword(DRHostSignatureRaw::DRHS_OFFSET_P0),
        ));
        let driver_version = DriverVersion::__new(u64::from_le(
            ns.read_qword(DRHostSignatureRaw::DRHS_OFFSET_P1),
        ));
        let file_specifier_id = FileSpecifierVersion::__new(u32::from_le(
            ns.read_dword(DRHostSignatureRaw::DRHS_OFFSET_P2),
        ));
        let endian =
            HostEndian::try_new_with_val(ns.read_byte(DRHostSignatureRaw::DRHS_OFFSET_P3))?;
        let ptr_width =
            HostPointerWidth::try_new_with_val(ns.read_byte(DRHostSignatureRaw::DRHS_OFFSET_P4))?;
        let arch = HostArch::try_new_with_val(ns.read_byte(DRHostSignatureRaw::DRHS_OFFSET_P5))?;
        let os = HostOS::try_new_with_val(ns.read_byte(DRHostSignatureRaw::DRHS_OFFSET_P6))?;
        Some(Self::new(
            server_version,
            driver_version,
            file_specifier_id,
            endian,
            ptr_width,
            arch,
            os,
        ))
    }
}

impl DRHostSignature {
    pub const fn new(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        file_specifier_version: FileSpecifierVersion,
        endian: HostEndian,
        ptr_width: HostPointerWidth,
        arch: HostArch,
        os: HostOS,
    ) -> Self {
        Self {
            server_version,
            driver_version,
            file_specifier_version,
            endian,
            ptr_width,
            arch,
            os,
        }
    }
    pub const fn server_version(&self) -> ServerVersion {
        self.server_version
    }
    pub const fn driver_version(&self) -> DriverVersion {
        self.driver_version
    }
    pub const fn file_specifier_version(&self) -> FileSpecifierVersion {
        self.file_specifier_version
    }
    pub const fn endian(&self) -> HostEndian {
        self.endian
    }
    pub const fn ptr_width(&self) -> HostPointerWidth {
        self.ptr_width
    }
    pub const fn arch(&self) -> HostArch {
        self.arch
    }
    pub const fn os(&self) -> HostOS {
        self.os
    }
    pub const fn encoded(&self) -> DRHostSignatureRaw {
        DRHostSignatureRaw::new_full(
            self.server_version(),
            self.driver_version(),
            self.file_specifier_version(),
            self.endian(),
            self.ptr_width(),
            self.arch(),
            self.os(),
        )
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DRHostSignatureRaw {
    pub(super) data: ByteStack<24>,
}

impl DRHostSignatureRaw {
    const DRHS_OFFSET_P0: usize = 0;
    const DRHS_OFFSET_P1: usize = sizeof!(u64);
    const DRHS_OFFSET_P2: usize = Self::DRHS_OFFSET_P1 + sizeof!(u64);
    const DRHS_OFFSET_P3: usize = Self::DRHS_OFFSET_P2 + sizeof!(u32);
    const DRHS_OFFSET_P4: usize = Self::DRHS_OFFSET_P3 + 1;
    const DRHS_OFFSET_P5: usize = Self::DRHS_OFFSET_P4 + 1;
    const DRHS_OFFSET_P6: usize = Self::DRHS_OFFSET_P5 + 1;
    const _ENSURE: () = assert!(Self::DRHS_OFFSET_P6 == sizeof!(Self) - 1);
    pub const fn new_auto(file_specifier_version: FileSpecifierVersion) -> Self {
        Self::new(
            versions::v1::V1_SERVER_VERSION,
            versions::v1::V1_DRIVER_VERSION,
            file_specifier_version,
        )
    }
    pub const fn new(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        file_specifier_id: FileSpecifierVersion,
    ) -> Self {
        Self::new_full(
            server_version,
            driver_version,
            file_specifier_id,
            HostEndian::new(),
            HostPointerWidth::new(),
            HostArch::new(),
            HostOS::new(),
        )
    }
    pub const fn new_full(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        file_specifier_id: FileSpecifierVersion,
        endian: HostEndian,
        ptr_width: HostPointerWidth,
        arch: HostArch,
        os: HostOS,
    ) -> Self {
        let _ = Self::_ENSURE;
        let bytes: [u8; 24] = unsafe {
            let [qw_a, qw_b]: [u64; 2] = core::mem::transmute([
                server_version.little_endian(),
                driver_version.little_endian(),
            ]);
            let dw: u32 = core::mem::transmute([
                endian.value_u8(),
                ptr_width.value_u8(),
                arch.value_u8(),
                os.value_u8(),
            ]);
            let qw_c: u64 = core::mem::transmute([(file_specifier_id.0.to_le(), dw.to_le())]);
            core::mem::transmute([qw_a, qw_b, qw_c])
        };
        Self {
            data: ByteStack::new(bytes),
        }
    }
}

impl DRHostSignatureRaw {
    pub const fn read_p0_server_version(&self) -> ServerVersion {
        ServerVersion::__new(self.data.read_qword(Self::DRHS_OFFSET_P0))
    }
    pub const fn read_p1_driver_version(&self) -> DriverVersion {
        DriverVersion::__new(self.data.read_qword(Self::DRHS_OFFSET_P1))
    }
    pub const fn read_p2_file_specifier_id(&self) -> FileSpecifierVersion {
        FileSpecifierVersion::__new(self.data.read_dword(Self::DRHS_OFFSET_P2))
    }
    pub const fn read_p3_endian(&self) -> HostEndian {
        HostEndian::new_with_val(self.data.read_byte(Self::DRHS_OFFSET_P3))
    }
    pub const fn read_p4_pointer_width(&self) -> HostPointerWidth {
        HostPointerWidth::new_with_val(self.data.read_byte(Self::DRHS_OFFSET_P4))
    }
    pub const fn read_p5_arch(&self) -> HostArch {
        HostArch::new_with_val(self.data.read_byte(Self::DRHS_OFFSET_P5))
    }
    pub const fn read_p6_os(&self) -> HostOS {
        HostOS::new_with_val(self.data.read_byte(Self::DRHS_OFFSET_P6))
    }
    pub const fn decoded(&self) -> DRHostSignature {
        DRHostSignature::new(
            self.read_p0_server_version(),
            self.read_p1_driver_version(),
            self.read_p2_file_specifier_id(),
            self.read_p3_endian(),
            self.read_p4_pointer_width(),
            self.read_p5_arch(),
            self.read_p6_os(),
        )
    }
}

/*
    Dynamic record (2/2): Runtime signature
    ---
    - 8B: Dynamic record modify count
    - 16B: Host epoch time
    - 16B: Host uptime
    - 1B: Host name length
    - 255B: Host name (nulled)
    = 296B
*/

#[derive(Debug, PartialEq, Clone)]
pub struct DRRuntimeSignature {
    modify_count: u64,
    epoch_time: u128,
    host_uptime: u128,
    host_name_length: u8,
    host_name_raw: [u8; 255],
}

impl DRRuntimeSignature {
    pub fn verify(&self) -> SDSSResult<()> {
        let et = util::os::get_epoch_time();
        if self.epoch_time() > et || self.host_uptime() > et {
            // a file from the future?
            return Err(SDSSError::TimeConflict);
        }
        Ok(())
    }
    pub fn decode_noverify(bytes: [u8; sizeof!(DRRuntimeSignatureRaw)]) -> Option<Self> {
        let bytes = ByteStack::new(bytes);
        // check
        let modify_count = u64::from_le(bytes.read_qword(DRRuntimeSignatureRaw::DRRS_OFFSET_P0));
        let epoch_time = u128::from_le(bytes.read_xmmword(DRRuntimeSignatureRaw::DRRS_OFFSET_P1));
        let host_uptime = u128::from_le(bytes.read_xmmword(DRRuntimeSignatureRaw::DRRS_OFFSET_P2));
        let host_name_length = bytes.read_byte(DRRuntimeSignatureRaw::DRRS_OFFSET_P3);
        let host_name_raw =
            util::copy_slice_to_array(&bytes.slice()[DRRuntimeSignatureRaw::DRRS_OFFSET_P4..]);
        if cfg!(debug_assertions) {
            assert_eq!(
                255 - host_name_raw.iter().filter(|b| **b == 0u8).count(),
                host_name_length as _
            );
        }
        Some(Self {
            modify_count,
            epoch_time,
            host_uptime,
            host_name_length,
            host_name_raw,
        })
    }
}

impl DRRuntimeSignature {
    pub const fn new(
        modify_count: u64,
        epoch_time: u128,
        host_uptime: u128,
        host_name_length: u8,
        host_name_raw: [u8; 255],
    ) -> Self {
        Self {
            modify_count,
            epoch_time,
            host_uptime,
            host_name_length,
            host_name_raw,
        }
    }
    pub const fn modify_count(&self) -> u64 {
        self.modify_count
    }
    pub const fn epoch_time(&self) -> u128 {
        self.epoch_time
    }
    pub const fn host_uptime(&self) -> u128 {
        self.host_uptime
    }
    pub const fn host_name_length(&self) -> u8 {
        self.host_name_length
    }
    pub const fn host_name_raw(&self) -> [u8; 255] {
        self.host_name_raw
    }
    pub fn host_name(&self) -> &[u8] {
        &self.host_name_raw[..self.host_name_length() as usize]
    }
    pub fn encoded(&self) -> DRRuntimeSignatureRaw {
        DRRuntimeSignatureRaw::new(
            self.modify_count(),
            self.epoch_time(),
            self.host_uptime(),
            self.host_name_length(),
            self.host_name_raw(),
        )
    }
    pub fn set_modify_count(&mut self, new: u64) {
        self.modify_count = new;
    }
    pub fn bump_modify_count(&mut self) {
        self.modify_count += 1;
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DRRuntimeSignatureRaw {
    pub(super) data: ByteStack<296>,
}

impl DRRuntimeSignatureRaw {
    const DRRS_OFFSET_P0: usize = 0;
    const DRRS_OFFSET_P1: usize = sizeof!(u64);
    const DRRS_OFFSET_P2: usize = Self::DRRS_OFFSET_P1 + sizeof!(u128);
    const DRRS_OFFSET_P3: usize = Self::DRRS_OFFSET_P2 + sizeof!(u128);
    const DRRS_OFFSET_P4: usize = Self::DRRS_OFFSET_P3 + 1;
    const _ENSURE: () = assert!(Self::DRRS_OFFSET_P4 == sizeof!(Self) - 255);
    pub fn new_auto(modify_count: u64) -> Self {
        let hostname = crate::util::os::get_hostname();
        Self::new(
            modify_count,
            crate::util::os::get_epoch_time(),
            crate::util::os::get_uptime(),
            hostname.len(),
            hostname.raw(),
        )
    }
    pub fn new(
        modify_count: u64,
        host_epoch_time: u128,
        host_uptime: u128,
        host_name_length: u8,
        host_name: [u8; 255],
    ) -> Self {
        let _ = Self::_ENSURE;
        let mut data = [0u8; 296];
        data[Self::DRRS_OFFSET_P0..Self::DRRS_OFFSET_P1]
            .copy_from_slice(&modify_count.to_le_bytes());
        data[Self::DRRS_OFFSET_P1..Self::DRRS_OFFSET_P2]
            .copy_from_slice(&host_epoch_time.to_le_bytes());
        data[Self::DRRS_OFFSET_P2..Self::DRRS_OFFSET_P3]
            .copy_from_slice(&host_uptime.to_le_bytes());
        data[Self::DRRS_OFFSET_P3] = host_name_length;
        data[Self::DRRS_OFFSET_P4..].copy_from_slice(&host_name);
        Self {
            data: ByteStack::new(data),
        }
    }
    pub fn decoded(&self) -> DRRuntimeSignature {
        DRRuntimeSignature::new(
            self.read_p0_modify_count(),
            self.read_p1_epoch_time(),
            self.read_p2_uptime(),
            self.read_p3_host_name_length() as _,
            util::copy_slice_to_array(self.read_p4_host_name_raw_null()),
        )
    }
    pub const fn read_p0_modify_count(&self) -> u64 {
        self.data.read_qword(Self::DRRS_OFFSET_P0)
    }
    pub const fn read_p1_epoch_time(&self) -> u128 {
        self.data.read_xmmword(Self::DRRS_OFFSET_P1)
    }
    pub const fn read_p2_uptime(&self) -> u128 {
        self.data.read_xmmword(Self::DRRS_OFFSET_P2)
    }
    pub const fn read_p3_host_name_length(&self) -> usize {
        self.data.read_byte(Self::DRRS_OFFSET_P3) as _
    }
    pub fn read_p4_host_name_raw_null(&self) -> &[u8] {
        &self.data.slice()[Self::DRRS_OFFSET_P4..]
    }
    pub fn read_host_name(&self) -> &[u8] {
        &self.data.slice()
            [Self::DRRS_OFFSET_P4..Self::DRRS_OFFSET_P4 + self.read_p3_host_name_length()]
    }
}

#[test]
fn test_dr_host_signature_encode_decode() {
    const TARGET: DRHostSignature = DRHostSignature::new(
        crate::engine::storage::versions::v1::V1_SERVER_VERSION,
        crate::engine::storage::versions::v1::V1_DRIVER_VERSION,
        FileSpecifierVersion::__new(u32::MAX - 3),
        HostEndian::new(),
        HostPointerWidth::new(),
        HostArch::new(),
        HostOS::new(),
    );
    let encoded = TARGET.encoded();
    let decoded = encoded.decoded();
    assert_eq!(decoded, TARGET);
}

#[test]
fn test_dr_runtime_signature_encoded_decode() {
    const TARGET: DRRuntimeSignature = DRRuntimeSignature::new(
        u64::MAX - 3,
        u128::MAX - u32::MAX as u128,
        u128::MAX - u32::MAX as u128,
        "skycloud".len() as _,
        util::copy_str_to_array("skycloud"),
    );
    let encoded = TARGET.encoded();
    let decoded = encoded.decoded();
    assert_eq!(decoded, TARGET);
    assert_eq!(decoded.host_name(), b"skycloud");
}
