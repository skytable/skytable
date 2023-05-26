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
            v1::header_impl::{FileScope, FileSpecifier, FileSpecifierVersion, HostRunMode},
            versions::{self, DriverVersion, ServerVersion},
        },
    },
    util,
};

/*
    Genesis record (1/2)
    ---
    Metadata record (8B x 3 + (4B x 2)):
    +----------+----------+----------+---------+
    |  Server  |  Driver  |   File   |File|Spec|
    |  version |  Version |   Scope  |Spec|ID  |
    +----------+----------+----------+---------+
    0, 63
*/

#[derive(Debug, PartialEq)]
pub struct GRMetadataRecord {
    server_version: ServerVersion,
    driver_version: DriverVersion,
    file_scope: FileScope,
    file_spec: FileSpecifier,
    file_spec_id: FileSpecifierVersion,
}

impl GRMetadataRecord {
    pub const fn new(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        file_scope: FileScope,
        file_spec: FileSpecifier,
        file_spec_id: FileSpecifierVersion,
    ) -> Self {
        Self {
            server_version,
            driver_version,
            file_scope,
            file_spec,
            file_spec_id,
        }
    }
    pub const fn server_version(&self) -> ServerVersion {
        self.server_version
    }
    pub const fn driver_version(&self) -> DriverVersion {
        self.driver_version
    }
    pub const fn file_scope(&self) -> FileScope {
        self.file_scope
    }
    pub const fn file_spec(&self) -> FileSpecifier {
        self.file_spec
    }
    pub const fn file_spec_id(&self) -> FileSpecifierVersion {
        self.file_spec_id
    }
    pub const fn encoded(&self) -> GRMetadataRecordRaw {
        GRMetadataRecordRaw::new_full(
            self.server_version(),
            self.driver_version(),
            self.file_scope(),
            self.file_spec(),
            self.file_spec_id(),
        )
    }
}

pub struct GRMetadataRecordRaw {
    pub(super) data: ByteStack<32>,
}

impl GRMetadataRecordRaw {
    /// Decodes a given metadata record, validating all data for correctness.
    ///
    /// **☢ WARNING ☢: This only decodes; it doesn't validate expected values!**
    pub fn decode(data: [u8; 32]) -> Option<GRMetadataRecord> {
        let data = ByteStack::new(data);
        let server_version =
            ServerVersion::__new(u64::from_le(data.read_qword(Self::MDR_OFFSET_P0)));
        let driver_version =
            DriverVersion::__new(u64::from_le(data.read_qword(Self::MDR_OFFSET_P1)));
        let file_scope = FileScope::try_new(u64::from_le(data.read_qword(Self::MDR_OFFSET_P2)))?;
        let file_spec = FileSpecifier::try_new(u32::from_le(data.read_dword(Self::MDR_OFFSET_P3)))?;
        let file_spec_id =
            FileSpecifierVersion::__new(u32::from_le(data.read_dword(Self::MDR_OFFSET_P4)));
        Some(GRMetadataRecord::new(
            server_version,
            driver_version,
            file_scope,
            file_spec,
            file_spec_id,
        ))
    }
}

impl GRMetadataRecordRaw {
    const MDR_OFFSET_P0: usize = 0;
    const MDR_OFFSET_P1: usize = sizeof!(u64);
    const MDR_OFFSET_P2: usize = Self::MDR_OFFSET_P1 + sizeof!(u64);
    const MDR_OFFSET_P3: usize = Self::MDR_OFFSET_P2 + sizeof!(u64);
    const MDR_OFFSET_P4: usize = Self::MDR_OFFSET_P3 + sizeof!(u32);
    const _ENSURE: () = assert!(Self::MDR_OFFSET_P4 == (sizeof!(Self) - sizeof!(u32)));
    pub const fn empty_buffer() -> [u8; sizeof!(Self)] {
        [0u8; sizeof!(Self)]
    }
    pub const fn new_full(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        scope: FileScope,
        specifier: FileSpecifier,
        specifier_id: FileSpecifierVersion,
    ) -> Self {
        let _ = Self::_ENSURE;
        let mut ret = [0u8; 32];
        let mut i = 0;
        // read buf
        let server_version = server_version.little_endian();
        let driver_version = driver_version.little_endian();
        let file_scope = scope.value_qword().to_le_bytes();
        // specifier + specifier ID
        let file_specifier_and_id: u64 = unsafe {
            core::mem::transmute([
                (specifier.value_u8() as u32).to_le(),
                specifier_id.0.to_le(),
            ])
        };
        let file_specifier_and_id = file_specifier_and_id.to_le_bytes();
        while i < sizeof!(u64) {
            ret[i] = server_version[i];
            ret[i + sizeof!(u64, 1)] = driver_version[i];
            ret[i + sizeof!(u64, 2)] = file_scope[i];
            ret[i + sizeof!(u64, 3)] = file_specifier_and_id[i];
            i += 1;
        }
        Self {
            data: ByteStack::new(ret),
        }
    }
    pub const fn new(
        scope: FileScope,
        specifier: FileSpecifier,
        specifier_id: FileSpecifierVersion,
    ) -> Self {
        Self::new_full(
            versions::v1::V1_SERVER_VERSION,
            versions::v1::V1_DRIVER_VERSION,
            scope,
            specifier,
            specifier_id,
        )
    }
}

impl GRMetadataRecordRaw {
    pub const fn read_p0_server_version(&self) -> ServerVersion {
        ServerVersion::__new(self.data.read_qword(Self::MDR_OFFSET_P0))
    }
    pub const fn read_p1_driver_version(&self) -> DriverVersion {
        DriverVersion::__new(self.data.read_qword(Self::MDR_OFFSET_P1))
    }
    pub const fn read_p2_file_scope(&self) -> FileScope {
        FileScope::new(self.data.read_qword(Self::MDR_OFFSET_P2))
    }
    pub const fn read_p3_file_spec(&self) -> FileSpecifier {
        FileSpecifier::new(self.data.read_dword(Self::MDR_OFFSET_P3))
    }
    pub const fn read_p4_file_spec_version(&self) -> FileSpecifierVersion {
        FileSpecifierVersion(self.data.read_dword(Self::MDR_OFFSET_P4))
    }
}

/*
    Genesis Record (2/2)
    ---
    Host record (?B; > 56B):
    - 16B: Host epoch time in nanoseconds
    - 16B: Host uptime in nanoseconds
    - 08B:
     - 04B: Host setting version ID
     - 04B: Host run mode
    - 08B: Host startup counter
    - 01B: Host name length
    - 255B: Host name
    = 304B
*/

#[derive(Debug, PartialEq)]
pub struct GRHostRecord {
    epoch_time: u128,
    uptime: u128,
    setting_version: u32,
    run_mode: HostRunMode,
    startup_counter: u64,
    hostname_len: u8,
    hostname_raw: [u8; 255],
}

impl GRHostRecord {
    pub fn decode(bytes: [u8; sizeof!(GRHostRecordRaw)]) -> Option<Self> {
        let ns = ByteStack::new(bytes);
        let epoch_time = u128::from_le(ns.read_xmmword(GRHostRecordRaw::GRHR_OFFSET_P0));
        let uptime = u128::from_le(ns.read_xmmword(GRHostRecordRaw::GRHR_OFFSET_P1));
        let setting_version = u32::from_le(ns.read_dword(GRHostRecordRaw::GRHR_OFFSET_P2));
        let run_mode = HostRunMode::try_new_with_val(u32::from_le(
            ns.read_dword(GRHostRecordRaw::GRHR_OFFSET_P3),
        ))?;
        let startup_counter = u64::from_le(ns.read_qword(GRHostRecordRaw::GRHR_OFFSET_P4));
        let host_name_len = ns.read_byte(GRHostRecordRaw::GRHR_OFFSET_P5);
        let host_name_raw =
            util::copy_slice_to_array(&ns.slice()[GRHostRecordRaw::GRHR_OFFSET_P6..]);
        Some(Self::new(
            epoch_time,
            uptime,
            setting_version,
            run_mode,
            startup_counter,
            host_name_len,
            host_name_raw,
        ))
    }
}

impl GRHostRecord {
    pub const fn new(
        epoch_time: u128,
        uptime: u128,
        setting_version: u32,
        run_mode: HostRunMode,
        startup_counter: u64,
        hostname_len: u8,
        hostname: [u8; 255],
    ) -> Self {
        Self {
            epoch_time,
            uptime,
            setting_version,
            run_mode,
            startup_counter,
            hostname_len,
            hostname_raw: hostname,
        }
    }
    pub fn epoch_time(&self) -> u128 {
        self.epoch_time
    }
    pub fn uptime(&self) -> u128 {
        self.uptime
    }
    pub fn setting_version(&self) -> u32 {
        self.setting_version
    }
    pub fn run_mode(&self) -> HostRunMode {
        self.run_mode
    }
    pub fn startup_counter(&self) -> u64 {
        self.startup_counter
    }
    pub fn hostname_len(&self) -> u8 {
        self.hostname_len
    }
    pub fn hostname_raw(&self) -> [u8; 255] {
        self.hostname_raw
    }
    pub fn encoded(&self) -> GRHostRecordRaw {
        GRHostRecordRaw::new(
            self.epoch_time(),
            self.uptime(),
            self.setting_version(),
            self.run_mode(),
            self.startup_counter(),
            self.hostname_len(),
            self.hostname_raw(),
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct GRHostRecordRaw {
    pub(super) data: ByteStack<304>,
}

impl GRHostRecordRaw {
    const GRHR_OFFSET_P0: usize = 0;
    const GRHR_OFFSET_P1: usize = sizeof!(u128);
    const GRHR_OFFSET_P2: usize = Self::GRHR_OFFSET_P1 + sizeof!(u128);
    const GRHR_OFFSET_P3: usize = Self::GRHR_OFFSET_P2 + sizeof!(u32);
    const GRHR_OFFSET_P4: usize = Self::GRHR_OFFSET_P3 + sizeof!(u32);
    const GRHR_OFFSET_P5: usize = Self::GRHR_OFFSET_P4 + sizeof!(u64);
    const GRHR_OFFSET_P6: usize = Self::GRHR_OFFSET_P5 + 1;
    const _ENSURE: () = assert!(Self::GRHR_OFFSET_P6 == sizeof!(Self) - 255);
    pub fn new(
        p0_epoch_time: u128,
        p1_uptime: u128,
        p2_setting_version: u32,
        p3_run_mode: HostRunMode,
        p4_host_startup_counter: u64,
        p5_host_name_length: u8,
        p6_host_name_raw: [u8; 255],
    ) -> Self {
        let _ = Self::_ENSURE;
        let mut data = [0u8; sizeof!(Self)];
        data[Self::GRHR_OFFSET_P0..Self::GRHR_OFFSET_P1]
            .copy_from_slice(&p0_epoch_time.to_le_bytes());
        data[Self::GRHR_OFFSET_P1..Self::GRHR_OFFSET_P2].copy_from_slice(&p1_uptime.to_le_bytes());
        data[Self::GRHR_OFFSET_P2..Self::GRHR_OFFSET_P3]
            .copy_from_slice(&p2_setting_version.to_le_bytes());
        data[Self::GRHR_OFFSET_P3..Self::GRHR_OFFSET_P4]
            .copy_from_slice(&(p3_run_mode.value_u8() as u32).to_le_bytes());
        data[Self::GRHR_OFFSET_P4..Self::GRHR_OFFSET_P5]
            .copy_from_slice(&p4_host_startup_counter.to_le_bytes());
        data[Self::GRHR_OFFSET_P5] = p5_host_name_length;
        data[Self::GRHR_OFFSET_P6..].copy_from_slice(&p6_host_name_raw);
        Self {
            data: ByteStack::new(data),
        }
    }
    pub const fn read_p0_epoch_time(&self) -> u128 {
        self.data.read_xmmword(Self::GRHR_OFFSET_P0)
    }
    pub const fn read_p1_uptime(&self) -> u128 {
        self.data.read_xmmword(Self::GRHR_OFFSET_P1)
    }
    pub const fn read_p2_setting_version_id(&self) -> u32 {
        self.data.read_dword(Self::GRHR_OFFSET_P2)
    }
    pub const fn read_p3_run_mode(&self) -> HostRunMode {
        HostRunMode::new_with_val(self.data.read_dword(Self::GRHR_OFFSET_P3))
    }
    pub const fn read_p4_startup_counter(&self) -> u64 {
        self.data.read_qword(Self::GRHR_OFFSET_P4)
    }
    pub const fn read_p5_host_name_length(&self) -> usize {
        self.data.read_byte(Self::GRHR_OFFSET_P5) as _
    }
    pub fn read_p6_host_name_raw(&self) -> &[u8] {
        &self.data.slice()[Self::GRHR_OFFSET_P6..]
    }
    pub fn read_host_name(&self) -> &[u8] {
        &self.data.slice()
            [Self::GRHR_OFFSET_P6..Self::GRHR_OFFSET_P6 + self.read_p5_host_name_length()]
    }
    pub fn decoded(&self) -> GRHostRecord {
        GRHostRecord::new(
            self.read_p0_epoch_time(),
            self.read_p1_uptime(),
            self.read_p2_setting_version_id(),
            self.read_p3_run_mode(),
            self.read_p4_startup_counter(),
            self.read_p5_host_name_length() as _,
            util::copy_slice_to_array(self.read_p6_host_name_raw()),
        )
    }
}

#[test]
fn test_metadata_record_encode_decode() {
    let md = GRMetadataRecordRaw::new(
        FileScope::TransactionLog,
        FileSpecifier::GNSTxnLog,
        FileSpecifierVersion(1),
    );
    assert_eq!(md.read_p0_server_version(), versions::v1::V1_SERVER_VERSION);
    assert_eq!(md.read_p1_driver_version(), versions::v1::V1_DRIVER_VERSION);
    assert_eq!(md.read_p2_file_scope(), FileScope::TransactionLog);
    assert_eq!(md.read_p3_file_spec(), FileSpecifier::GNSTxnLog);
    assert_eq!(md.read_p4_file_spec_version(), FileSpecifierVersion(1));
}

#[test]
fn test_host_record_encode_decode() {
    const HOST_UPTIME: u128 = u128::MAX - 434324903;
    const HOST_SETTING_VERSION_ID: u32 = 245;
    const HOST_RUN_MODE: HostRunMode = HostRunMode::Prod;
    const HOST_STARTUP_COUNTER: u64 = u32::MAX as _;
    const HOST_NAME: &str = "skycloud";
    use std::time::*;
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let hr = GRHostRecordRaw::new(
        time,
        HOST_UPTIME,
        HOST_SETTING_VERSION_ID,
        HOST_RUN_MODE,
        HOST_STARTUP_COUNTER,
        HOST_NAME.len() as _,
        crate::util::copy_str_to_array(HOST_NAME),
    );
    assert_eq!(hr.read_p0_epoch_time(), time);
    assert_eq!(hr.read_p1_uptime(), HOST_UPTIME);
    assert_eq!(hr.read_p2_setting_version_id(), HOST_SETTING_VERSION_ID);
    assert_eq!(hr.read_p3_run_mode(), HOST_RUN_MODE);
    assert_eq!(hr.read_p4_startup_counter(), HOST_STARTUP_COUNTER);
    assert_eq!(hr.read_p5_host_name_length(), HOST_NAME.len());
    assert_eq!(hr.read_host_name(), HOST_NAME.as_bytes());
}
