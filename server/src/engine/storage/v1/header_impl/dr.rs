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

use crate::engine::{
    mem::ByteStack,
    storage::{
        header::{HostArch, HostEndian, HostOS, HostPointerWidth},
        v1::header_impl::FileSpecifierVersion,
        versions::{DriverVersion, ServerVersion},
    },
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct DRHostSignatureRaw {
    data: ByteStack<24>,
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
    pub const fn encoded(&self) -> DRHostSignature {
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
    - 8B: Host name length
    - ?B: Host name
*/

#[derive(Debug, PartialEq, Clone)]
pub struct DRRuntimeSignature {
    rt_signature_fixed: DRRuntimeSignatureFixed,
    host_name: Box<[u8]>,
}

impl DRRuntimeSignature {
    pub fn new(fixed: DRRuntimeSignatureFixed, host_name: Box<[u8]>) -> Self {
        Self {
            rt_signature_fixed: fixed,
            host_name,
        }
    }
    pub const fn rt_signature_fixed(&self) -> &DRRuntimeSignatureFixed {
        &self.rt_signature_fixed
    }
    pub fn host_name(&self) -> &[u8] {
        self.host_name.as_ref()
    }
    pub fn into_encoded(self) -> DRRuntimeSignatureRaw {
        let len = self.host_name.len();
        DRRuntimeSignatureRaw::new_with_sections(
            self.host_name,
            self.rt_signature_fixed.encoded(len),
        )
    }
    pub fn encoded(&self) -> DRRuntimeSignatureRaw {
        self.clone().into_encoded()
    }
}

pub struct DRRuntimeSignatureRaw {
    rt_signature: DRRuntimeSignatureFixedRaw,
    pub(super) host_name: Box<[u8]>,
}

impl DRRuntimeSignatureRaw {
    pub fn new(host_name: Box<[u8]>, modify_count: u64) -> Self {
        Self {
            rt_signature: DRRuntimeSignatureFixedRaw::new(modify_count, host_name.len()),
            host_name,
        }
    }
    pub fn new_with_sections(host_name: Box<[u8]>, fixed: DRRuntimeSignatureFixedRaw) -> Self {
        Self {
            rt_signature: fixed,
            host_name,
        }
    }
    pub fn decode(
        data: [u8; sizeof!(DRRuntimeSignatureFixedRaw)],
    ) -> Option<(usize, DRRuntimeSignatureFixed)> {
        let s = ByteStack::new(data);
        let modify_count = u64::from_le(s.read_qword(DRRuntimeSignatureFixedRaw::DRRS_OFFSET_P0));
        let epoch_time = u128::from_le(s.read_xmmword(DRRuntimeSignatureFixedRaw::DRRS_OFFSET_P1));
        let uptime = u128::from_le(s.read_xmmword(DRRuntimeSignatureFixedRaw::DRRS_OFFSET_P2));
        let host_name_length =
            u64::from_le(s.read_qword(DRRuntimeSignatureFixedRaw::DRRS_OFFSET_P3));
        if epoch_time > crate::util::os::get_epoch_time() || host_name_length > usize::MAX as u64 {
            // damn, this file is from the future; I WISH EVERYONE HAD NTP SYNC GRRRR
            // or, we have a bad host name. like, what?
            return None;
        }
        Some((
            host_name_length as _,
            DRRuntimeSignatureFixed::new(modify_count, epoch_time, uptime),
        ))
    }
    pub const fn runtime_signature(&self) -> &DRRuntimeSignatureFixedRaw {
        &self.rt_signature
    }
    pub fn name(&self) -> &[u8] {
        &self.host_name
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DRRuntimeSignatureFixed {
    modify_count: u64,
    epoch_time: u128,
    uptime: u128,
}

impl DRRuntimeSignatureFixed {
    pub const fn new(modify_count: u64, epoch_time: u128, uptime: u128) -> Self {
        Self {
            modify_count,
            epoch_time,
            uptime,
        }
    }
    pub const fn modify_count(&self) -> u64 {
        self.modify_count
    }
    pub const fn epoch_time(&self) -> u128 {
        self.epoch_time
    }
    pub const fn uptime(&self) -> u128 {
        self.uptime
    }
    pub fn encoded(&self, host_name_length: usize) -> DRRuntimeSignatureFixedRaw {
        DRRuntimeSignatureFixedRaw::new_full(
            self.modify_count(),
            self.epoch_time(),
            self.uptime(),
            host_name_length,
        )
    }
}

pub struct DRRuntimeSignatureFixedRaw {
    data: ByteStack<48>,
}

impl DRRuntimeSignatureFixedRaw {
    const DRRS_OFFSET_P0: usize = 0;
    const DRRS_OFFSET_P1: usize = sizeof!(u64);
    const DRRS_OFFSET_P2: usize = Self::DRRS_OFFSET_P1 + sizeof!(u128);
    const DRRS_OFFSET_P3: usize = Self::DRRS_OFFSET_P2 + sizeof!(u128);
    const _ENSURE: () = assert!(Self::DRRS_OFFSET_P3 == sizeof!(Self) - 8);
    pub fn new_full(
        modify_count: u64,
        epoch_time: u128,
        uptime: u128,
        host_name_length: usize,
    ) -> Self {
        let _ = Self::_ENSURE;
        let mut data = [0u8; sizeof!(Self)];
        data[0..8].copy_from_slice(&modify_count.to_le_bytes());
        data[8..24].copy_from_slice(&epoch_time.to_le_bytes());
        data[24..40].copy_from_slice(&uptime.to_le_bytes());
        data[40..48].copy_from_slice(&(host_name_length as u64).to_le_bytes());
        Self {
            data: ByteStack::new(data),
        }
    }
    pub fn new(modify_count: u64, host_name_length: usize) -> Self {
        Self::new_full(
            modify_count,
            crate::util::os::get_epoch_time(),
            crate::util::os::get_uptime(),
            host_name_length,
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
    pub const fn read_p3_host_name_length(&self) -> u64 {
        self.data.read_qword(Self::DRRS_OFFSET_P3)
    }
}
